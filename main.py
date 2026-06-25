import asyncio
import logging
import signal
import sys
import time

from config import CHAINS, ALCHEMY_API_KEY
from pool_manager import PoolManager
from swap_monitor import ChainMonitor, EthMempoolMonitor
from alert import TelegramAlert
from wallet_checker import WalletChecker
from cex_checker import CexChecker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Alert cooldown: don't send more than 1 alert per token per N seconds
ALERT_COOLDOWN_SEC = 60


class CryptoDumpBot:
    def __init__(self):
        self._pool_manager = PoolManager()
        self._alert = TelegramAlert()
        self._wallet_checker = WalletChecker()
        self._cex_checker = CexChecker()
        self._chain_monitors: dict[str, ChainMonitor] = {}
        self._mempool_monitor: EthMempoolMonitor | None = None
        self._tasks: list[asyncio.Task] = []
        # Cooldown: (chain, token_address) -> last alert timestamp
        self._alert_cooldowns: dict[tuple[str, str], float] = {}
        # In-memory token info cache to avoid DB lookups
        self._token_cache: dict[tuple[str, str], dict] = {}
        # Cache tokens confirmed NOT on CEX (skip future dumps)
        self._not_on_cex: set[str] = set()

    async def start(self):
        logger.info("=" * 50)
        logger.info("  crypto_dump_bot starting...")
        logger.info("=" * 50)

        await self._pool_manager.initialize()

        if await self._pool_manager.needs_update():
            logger.info("Pool list needs update, fetching...")
            await self._pool_manager.update_pools()
        else:
            logger.info("Pool list is up to date")

        await self._pool_manager.start_scheduled_updates()

        for chain_name in CHAINS:
            monitor = ChainMonitor(
                chain_name=chain_name,
                pool_addresses_getter=lambda cn=chain_name: self._pool_manager.get_all_pool_addresses(cn),
                pool_info_getter=lambda addr, cn=chain_name: self._pool_manager.get_pools(cn).get(addr),
                on_dump=self._handle_dump,
            )
            self._chain_monitors[chain_name] = monitor
            task = asyncio.create_task(monitor.start())
            self._tasks.append(task)
            logger.info(f"Started monitor for {chain_name}")

        if ALCHEMY_API_KEY:
            self._mempool_monitor = EthMempoolMonitor(
                pool_addresses_getter=lambda: self._pool_manager.get_all_pool_addresses("ethereum"),
                pool_info_getter=lambda addr: self._pool_manager.get_pools("ethereum").get(addr),
                on_dump=self._handle_dump,
            )
            task = asyncio.create_task(self._mempool_monitor.start())
            self._tasks.append(task)
            logger.info("Started ETH mempool monitor (Alchemy)")

        logger.info("Bot is running. Press Ctrl+C to stop.")
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _handle_dump(self, chain: str, dump_info: dict):
        """Called when a dump is detected. Non-blocking alert dispatch."""
        pool_address = dump_info["pool_address"]
        pool_info = self._pool_manager.get_pools(chain).get(pool_address)

        if not pool_info:
            return

        token_address = pool_info.get("token_address", "")

        # Cooldown check — skip if same token alerted recently
        cooldown_key = (chain, token_address)
        now = time.time()
        last_alert = self._alert_cooldowns.get(cooldown_key, 0)
        if now - last_alert < ALERT_COOLDOWN_SEC:
            logger.debug(f"Cooldown active for {token_address} on {chain}, skipping")
            return
        self._alert_cooldowns[cooldown_key] = now

        # Cleanup old cooldowns
        if len(self._alert_cooldowns) > 1000:
            cutoff = now - ALERT_COOLDOWN_SEC * 2
            self._alert_cooldowns = {
                k: v for k, v in self._alert_cooldowns.items() if v > cutoff
            }

        # Get token info from cache or DB
        cache_key = (chain, token_address)
        token_info = self._token_cache.get(cache_key)
        if not token_info:
            token_info = await self._pool_manager.get_token_info(chain, token_address)
            if token_info:
                self._token_cache[cache_key] = token_info

        if not token_info:
            token_info = {
                "symbol": pool_info.get("base_token_symbol", "???"),
                "name": "",
                "platforms": {},
            }

        # CEX verification — skip tokens not on any major exchange
        coingecko_id = token_info.get("coingecko_id", "")
        if coingecko_id in self._not_on_cex:
            logger.debug(f"Skipping {token_info['symbol']} — cached as not on CEX")
            return

        is_on_cex, exchanges = await self._cex_checker.is_on_cex(coingecko_id)
        if not is_on_cex:
            self._not_on_cex.add(coingecko_id)
            return

        logger.info(
            f"DUMP [{chain}] {token_info['symbol']} "
            f"{dump_info['change_pct']:.2f}% "
            f"({dump_info['num_swaps']} swaps in 1s) "
            f"[CEX: {', '.join(exchanges[:3])}]"
        )

        # Message 1: instant dump alert (fire immediately)
        asyncio.create_task(self._send_dump_alert_safe(chain, token_info, dump_info, pool_info))
        # Message 2: seller info (fires after wallet check, ~0.3-0.5s later)
        asyncio.create_task(self._send_seller_alert_safe(chain, token_info, dump_info, pool_info))

    async def _send_dump_alert_safe(self, chain, token_info, dump_info, pool_info):
        """Message 1: instant dump alert. Zero delay."""
        try:
            await self._alert.send_dump_alert(
                chain=chain,
                token_info=token_info,
                dump_info=dump_info,
                pool_info=pool_info,
            )
        except Exception as e:
            logger.error(f"Failed to send dump alert: {e}")

    async def _send_seller_alert_safe(self, chain, token_info, dump_info, pool_info):
        """Message 2: seller wallet + sold %. Arrives ~0.3-0.5s after dump alert."""
        try:
            token_address = pool_info.get("token_address", "")
            tx_hash = dump_info.get("tx_hash", "")

            if not tx_hash or not token_address:
                return

            seller_info = await self._wallet_checker.get_seller_info(
                chain=chain,
                tx_hash=tx_hash,
                token_address=token_address,
            )

            if seller_info:
                await self._alert.send_seller_alert(
                    chain=chain,
                    token_info=token_info,
                    seller_info=seller_info,
                )
        except Exception as e:
            logger.error(f"Failed to send seller alert: {e}")

    async def stop(self):
        logger.info("Shutting down...")

        for monitor in self._chain_monitors.values():
            await monitor.stop()
        if self._mempool_monitor:
            await self._mempool_monitor.stop()

        for task in self._tasks:
            task.cancel()

        await self._pool_manager.close()
        await self._alert.close()
        await self._wallet_checker.close()
        await self._cex_checker.close()
        logger.info("Shutdown complete.")


async def main():
    bot = CryptoDumpBot()

    loop = asyncio.get_event_loop()
    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(bot.stop()))
        loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.create_task(bot.stop()))

    try:
        await bot.start()
    except KeyboardInterrupt:
        await bot.stop()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        await bot.stop()


if __name__ == "__main__":
    asyncio.run(main())
