import asyncio
import logging
import time
from collections import defaultdict
from typing import Optional, Callable

try:
    import orjson
    def json_loads(data): return orjson.loads(data)
    def json_dumps(obj): return orjson.dumps(obj).decode()
except ImportError:
    import json
    json_loads = json.loads
    json_dumps = json.dumps

import websockets

from config import (
    CHAINS, ALCHEMY_API_KEY,
    SWAP_EVENT_V2, SYNC_EVENT_V2, SWAP_EVENT_V3,
    DUMP_THRESHOLD_PCT,
)

# All three event signatures we subscribe to
ALL_SWAP_TOPICS = [SYNC_EVENT_V2, SWAP_EVENT_V2, SWAP_EVENT_V3]

logger = logging.getLogger(__name__)


def decode_v2_sync(data_hex: str) -> tuple[int, int]:
    """Decode Sync(uint112 reserve0, uint112 reserve1) event data."""
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    reserve0 = int(data[0:64], 16)
    reserve1 = int(data[64:128], 16)
    return reserve0, reserve1


def decode_v3_swap(data_hex: str) -> tuple[int, int, int, int, int]:
    """
    Decode Swap(address,address,int256,int256,uint160,uint128,int24) data.
    Topics: [event_sig, sender, recipient]
    Data: amount0, amount1, sqrtPriceX96, liquidity, tick
    """
    data = data_hex[2:] if data_hex.startswith("0x") else data_hex
    amount0 = int.from_bytes(bytes.fromhex(data[0:64]), "big", signed=True)
    amount1 = int.from_bytes(bytes.fromhex(data[64:128]), "big", signed=True)
    sqrt_price_x96 = int(data[128:192], 16)
    liquidity = int(data[192:256], 16)
    tick = int.from_bytes(bytes.fromhex(data[256:320]), "big", signed=True)
    return amount0, amount1, sqrt_price_x96, liquidity, tick


def price_from_sqrt_price_x96(sqrt_price_x96: int) -> float:
    """Convert sqrtPriceX96 to price (token1/token0)."""
    if sqrt_price_x96 == 0:
        return 0.0
    return (sqrt_price_x96 / (2**96)) ** 2


def price_from_reserves(reserve0: int, reserve1: int) -> float:
    """Price = reserve1/reserve0 (token1 per token0)."""
    if reserve0 == 0:
        return 0.0
    return reserve1 / reserve0


class SwapEvent:
    __slots__ = ("pool_address", "block_number", "timestamp", "tx_hash",
                 "log_index", "is_v3", "price_after")

    def __init__(self, pool_address: str, block_number: int, timestamp: int,
                 tx_hash: str, log_index: int, is_v3: bool, price_after: float):
        self.pool_address = pool_address
        self.block_number = block_number
        self.timestamp = timestamp
        self.tx_hash = tx_hash
        self.log_index = log_index
        self.is_v3 = is_v3
        self.price_after = price_after


class PriceTracker:
    """
    Groups swaps by (pool, second) and detects dumps.
    Price "before" a second = price after the last event of the previous second.
    """

    def __init__(self, dump_threshold_pct: float = DUMP_THRESHOLD_PCT):
        self._threshold = dump_threshold_pct / 100.0
        self._settled_price: dict[str, float] = {}
        self._second_open_price: dict[str, float] = {}
        self._current_second: dict[str, int] = {}
        self._current_events: dict[str, list[SwapEvent]] = defaultdict(list)
        self._fired: set[tuple[str, int]] = set()

    def process_swap(self, event: SwapEvent) -> Optional[dict]:
        pool = event.pool_address
        sec = event.timestamp

        prev_sec = self._current_second.get(pool)
        if prev_sec is not None and prev_sec != sec:
            if self._current_events[pool]:
                self._settled_price[pool] = self._current_events[pool][-1].price_after
            self._current_events[pool] = []
            self._second_open_price.pop(pool, None)

        self._current_second[pool] = sec

        if pool not in self._second_open_price:
            open_price = self._settled_price.get(pool)
            if open_price and open_price > 0:
                self._second_open_price[pool] = open_price

        self._current_events[pool].append(event)

        open_price = self._second_open_price.get(pool)
        if not open_price or open_price == 0:
            self._settled_price[pool] = event.price_after
            return None

        change_pct = (event.price_after - open_price) / open_price

        if change_pct <= -self._threshold and (pool, sec) not in self._fired:
            self._fired.add((pool, sec))
            old_fired = [(p, s) for p, s in self._fired if s < sec - 30]
            for item in old_fired:
                self._fired.discard(item)

            return {
                "pool_address": pool,
                "timestamp": sec,
                "price_before": open_price,
                "price_after": event.price_after,
                "change_pct": change_pct * 100,
                "num_swaps": len(self._current_events[pool]),
                "tx_hash": self._current_events[pool][-1].tx_hash,
            }

        return None

    def set_initial_price(self, pool: str, price: float):
        if pool not in self._settled_price:
            self._settled_price[pool] = price


BSC_BACKUP_WS = [
    "wss://bsc-rpc.publicnode.com",
    "wss://bsc.drpc.org",
    "wss://bsc-mainnet.public.blastapi.io",
]

# Stale thresholds per chain (seconds without any event = likely dead connection)
STALE_THRESHOLDS = {
    "ethereum": 30,
    "bsc": 15,
    "base": 15,
    "arbitrum": 10,
}


class ChainMonitor:
    """Monitors swap events on a single chain via WebSocket with fallback + stale detection."""

    def __init__(
        self,
        chain_name: str,
        pool_addresses_getter: Callable[[], list[str]],
        pool_info_getter: Callable[[str], Optional[dict]],
        on_dump: Callable[[str, dict], asyncio.coroutine],
    ):
        self._chain_name = chain_name
        self._chain_config = CHAINS[chain_name]
        self._get_pool_addresses = pool_addresses_getter
        self._get_pool_info = pool_info_getter
        self._on_dump = on_dump
        self._price_tracker = PriceTracker()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._block_timestamps: dict[int, int] = {}
        self._consecutive_failures = 0
        self._current_url_index = 0
        self._last_event_time: float = 0
        self._events_count: int = 0
        # V2: buffer last Sync price per pool until Swap confirms it's a real trade
        self._v2_pending_price: dict[str, tuple[float, str, int]] = {}

    def _get_ws_urls(self) -> list[str]:
        urls = [self._chain_config["ws_url"]]
        backup = self._chain_config.get("ws_url_backup")
        if backup:
            urls.append(backup)
        if self._chain_name == "bsc":
            for url in BSC_BACKUP_WS:
                if url not in urls:
                    urls.append(url)
        return urls

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
                self._consecutive_failures = 0
            except Exception as e:
                self._consecutive_failures += 1
                wait = min(5 * self._consecutive_failures, 60)
                logger.error(
                    f"[{self._chain_name}] WS error #{self._consecutive_failures}: {e}. Retry in {wait}s"
                )
                if self._consecutive_failures >= 3:
                    self._current_url_index = (self._current_url_index + 1) % len(self._get_ws_urls())
                    logger.info(f"[{self._chain_name}] Switching to backup URL")
                    self._consecutive_failures = 0
                await asyncio.sleep(wait)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect_and_listen(self):
        urls = self._get_ws_urls()
        ws_url = urls[self._current_url_index % len(urls)]
        pool_addresses = self._get_pool_addresses()

        if not pool_addresses:
            logger.warning(f"[{self._chain_name}] No pools to monitor, waiting...")
            await asyncio.sleep(60)
            return

        logger.info(f"[{self._chain_name}] Connecting to {ws_url}, {len(pool_addresses)} pools")

        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=60, max_size=2**22) as ws:
            self._ws = ws
            self._last_event_time = time.time()
            self._events_count = 0

            # Robust handshake: match acks by id, buffer notifications that arrive early.
            buffered = await self._subscribe_all(ws, pool_addresses)

            # Process any notifications that arrived during the handshake.
            for note in buffered:
                await self._handle_message(note)

            stale_threshold = STALE_THRESHOLDS.get(self._chain_name, 30)

            while self._running:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=stale_threshold)
                    await self._handle_message(json_loads(msg))
                except asyncio.TimeoutError:
                    # Stale detection: no message for too long
                    elapsed = time.time() - self._last_event_time
                    if elapsed > stale_threshold * 2:
                        logger.warning(
                            f"[{self._chain_name}] Connection stale ({elapsed:.0f}s no events), reconnecting..."
                        )
                        return
                    # Try a ping
                    try:
                        await ws.send(json_dumps({"jsonrpc": "2.0", "id": 0, "method": "net_version", "params": []}))
                        await asyncio.wait_for(ws.recv(), timeout=5)
                    except Exception:
                        logger.warning(f"[{self._chain_name}] Ping failed, reconnecting...")
                        return

    async def _subscribe_all(self, ws, pool_addresses: list[str]) -> list[dict]:
        """
        Send newHeads + log subscriptions, then read responses matching acks by `id`.
        Any eth_subscription notification that arrives mid-handshake is buffered and
        returned (so it isn't mistaken for an ack and isn't dropped). This is essential
        on fast chains (Arbitrum/Base) where notifications interleave with acks.
        """
        expected_ids: set[int] = set()

        heads_id = 9999
        await ws.send(json_dumps({
            "jsonrpc": "2.0", "id": heads_id,
            "method": "eth_subscribe", "params": ["newHeads"],
        }))
        expected_ids.add(heads_id)

        batch_size = 500
        sub_id = 1
        for i in range(0, len(pool_addresses), batch_size):
            batch = pool_addresses[i:i + batch_size]
            await ws.send(json_dumps({
                "jsonrpc": "2.0", "id": sub_id,
                "method": "eth_subscribe",
                "params": ["logs", {
                    "address": batch,
                    "topics": [ALL_SWAP_TOPICS],
                }],
            }))
            expected_ids.add(sub_id)
            sub_id += 1

        total_batches = sub_id - 1
        buffered: list[dict] = []
        failed = 0
        timeout = 30  # max seconds to wait for all acks

        start = time.time()
        while expected_ids:
            if time.time() - start > timeout:
                logger.warning(
                    f"[{self._chain_name}] Handshake timeout, {len(expected_ids)} acks missing"
                )
                break

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
            except asyncio.TimeoutError:
                continue

            msg = json_loads(raw)

            # A proper JSON-RPC response has "id" but NOT "method".
            # Messages with "method" are notifications (even if they carry an id field).
            if msg.get("method") == "eth_subscription":
                buffered.append(msg)
                continue

            mid = msg.get("id")
            if mid in expected_ids:
                if "result" in msg:
                    expected_ids.discard(mid)
                elif "error" in msg:
                    failed += 1
                    logger.error(f"[{self._chain_name}] Subscribe id={mid} error: {msg['error']}")
                    expected_ids.discard(mid)
                # else: unexpected format, ignore

        if failed == total_batches + 1:
            raise Exception("All subscriptions failed")

        logger.info(
            f"[{self._chain_name}] Ready ({total_batches} batches, "
            f"{failed} failed, {len(buffered)} early notifications buffered)"
        )
        return buffered

    async def _handle_message(self, msg: dict):
        if msg.get("method") != "eth_subscription":
            return

        params = msg.get("params", {})
        result = params.get("result", {})

        # Handle newHeads (block with timestamp)
        if "timestamp" in result and "number" in result:
            block_num = int(result["number"], 16)
            block_ts = int(result["timestamp"], 16)
            self._block_timestamps[block_num] = block_ts
            # Cleanup old
            if len(self._block_timestamps) > 200:
                min_block = block_num - 150
                self._block_timestamps = {
                    k: v for k, v in self._block_timestamps.items() if k > min_block
                }
            return

        topics = result.get("topics", [])
        if not topics:
            return

        self._last_event_time = time.time()
        self._events_count += 1

        event_sig = topics[0]
        pool_address = result.get("address", "").lower()
        data = result.get("data", "0x")
        block_hex = result.get("blockNumber", "0x0")
        block_number = int(block_hex, 16)
        tx_hash = result.get("transactionHash", "")
        log_index = int(result.get("logIndex", "0x0"), 16)

        pool_info = self._get_pool_info(pool_address)
        if not pool_info:
            return

        # V2 Sync event: ONLY update price state.
        # Does NOT trigger dump detection (fires on liquidity events too).
        if event_sig == SYNC_EVENT_V2:
            try:
                reserve0, reserve1 = decode_v2_sync(data)
            except (ValueError, IndexError):
                return
            pool_price = price_from_reserves(reserve0, reserve1)
            if pool_price <= 0:
                return
            monitored_price = self._to_monitored_price(pool_price, pool_info)
            # Store latest price + tx_hash for when the Swap event arrives
            self._v2_pending_price[pool_address] = (monitored_price, tx_hash, block_number)
            self._price_tracker.set_initial_price(pool_address, monitored_price)
            return

        # V2 Swap event: confirms a REAL trade happened.
        # Use price from the Sync event that preceded it in the same tx.
        if event_sig == SWAP_EVENT_V2:
            pending = self._v2_pending_price.get(pool_address)
            if not pending:
                return
            monitored_price, sync_tx, sync_block = pending
            # Sync and Swap must be in same tx (Sync always precedes Swap in V2)
            if sync_tx != tx_hash:
                return
            timestamp = self._get_block_timestamp(block_number)
            swap_event = SwapEvent(
                pool_address=pool_address,
                block_number=block_number,
                timestamp=timestamp,
                tx_hash=tx_hash,
                log_index=log_index,
                is_v3=False,
                price_after=monitored_price,
            )
            dump = self._price_tracker.process_swap(swap_event)
            if dump:
                asyncio.create_task(self._on_dump(self._chain_name, dump))
            return

        # V3 Swap event: contains price directly, always a real trade.
        if event_sig == SWAP_EVENT_V3:
            try:
                _, _, sqrt_price_x96, _, _ = decode_v3_swap(data)
            except (ValueError, IndexError):
                return
            pool_price = price_from_sqrt_price_x96(sqrt_price_x96)
            if pool_price <= 0:
                return
            monitored_price = self._to_monitored_price(pool_price, pool_info)
            timestamp = self._get_block_timestamp(block_number)
            swap_event = SwapEvent(
                pool_address=pool_address,
                block_number=block_number,
                timestamp=timestamp,
                tx_hash=tx_hash,
                log_index=log_index,
                is_v3=True,
                price_after=monitored_price,
            )
            dump = self._price_tracker.process_swap(swap_event)
            if dump:
                asyncio.create_task(self._on_dump(self._chain_name, dump))

    def _to_monitored_price(self, pool_price: float, pool_info: dict) -> float:
        """Convert pool ratio to monitored token's price."""
        if self._monitored_is_token0(pool_info):
            return pool_price
        return 1.0 / pool_price

    @staticmethod
    def _monitored_is_token0(pool_info: dict) -> bool:
        """
        Determine if the monitored token is token0 in the pool.
        Uniswap/Pancake/Aerodrome/Camelot all sort token0 < token1 by address,
        so this needs no RPC call. Result is cached on pool_info.
        """
        cached = pool_info.get("monitored_is_token0")
        if cached is not None:
            return cached

        token = (pool_info.get("token_address") or "").lower()
        base = (pool_info.get("base_token_address") or "").lower()
        quote = (pool_info.get("quote_token_address") or "").lower()

        other = quote if token == base else base
        if not token or not other:
            result = True  # fallback: assume token0 (no inversion)
        else:
            result = token < other

        pool_info["monitored_is_token0"] = result
        return result

    def _get_block_timestamp(self, block_number: int) -> int:
        """Get block timestamp from newHeads cache, fallback to estimation."""
        if block_number in self._block_timestamps:
            return self._block_timestamps[block_number]

        # Estimate from nearest known block
        if self._block_timestamps:
            nearest_block = max(self._block_timestamps.keys())
            nearest_ts = self._block_timestamps[nearest_block]
            block_time = self._chain_config["block_time"]
            estimated = nearest_ts + int((block_number - nearest_block) * block_time)
            self._block_timestamps[block_number] = estimated
            return estimated

        now = int(time.time())
        self._block_timestamps[block_number] = now
        return now


class EthMempoolMonitor:
    """
    Monitors ETH mempool for large single swaps via Alchemy's
    alchemy_pendingTransactions subscription.
    Only works if ALCHEMY_API_KEY is set.
    """

    def __init__(
        self,
        pool_addresses_getter: Callable[[], list[str]],
        pool_info_getter: Callable[[str], Optional[dict]],
        on_dump: Callable[[str, dict], asyncio.coroutine],
    ):
        self._get_pool_addresses = pool_addresses_getter
        self._get_pool_info = pool_info_getter
        self._on_dump = on_dump
        self._price_tracker = PriceTracker()
        self._running = False

    async def start(self):
        if not ALCHEMY_API_KEY:
            logger.info("ETH mempool monitor disabled (no Alchemy key)")
            return

        self._running = True
        ws_url = f"wss://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

        while self._running:
            try:
                await self._connect(ws_url)
            except Exception as e:
                logger.error(f"[ETH mempool] Error: {e}")
                await asyncio.sleep(10)

    async def stop(self):
        self._running = False

    async def _connect(self, ws_url: str):
        pool_addresses = self._get_pool_addresses()
        if not pool_addresses:
            await asyncio.sleep(60)
            return

        uniswap_v2_router = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
        uniswap_v3_router = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
        universal_router = "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"

        target_addresses = [uniswap_v2_router, uniswap_v3_router, universal_router]

        async with websockets.connect(ws_url, ping_interval=20, ping_timeout=60) as ws:
            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "alchemy_pendingTransactions",
                    {"toAddress": target_addresses, "hashesOnly": False}
                ]
            }
            await ws.send(json_dumps(subscribe_msg))
            resp = await ws.recv()
            resp_data = json_loads(resp)
            if "result" not in resp_data:
                logger.error(f"[ETH mempool] Subscribe failed: {resp_data}")
                return

            logger.info("[ETH mempool] Subscribed to Alchemy pending transactions")

            while self._running:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=60)
                    data = json_loads(msg)
                    if data.get("method") == "eth_subscription":
                        await self._handle_pending_tx(data["params"]["result"])
                except asyncio.TimeoutError:
                    continue

    async def _handle_pending_tx(self, tx: dict):
        """
        Analyze pending tx for price impact using cached pool reserves.
        Detects large swap amounts from transaction input data.
        """
        pass
