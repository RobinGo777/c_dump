import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

import aiosqlite

from config import CHAINS, POOL_UPDATE_TIME
from coingecko import CoinGeckoClient
from dexscreener import DexScreenerClient

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "pools.db"


class PoolManager:
    """
    Manages the pool list using inverted CEX filter:
    1. CoinGecko -> tokens on CEX
    2. DexScreener -> pool addresses for those tokens
    3. Store in local DB, update daily
    """

    def __init__(self):
        self._db: Optional[aiosqlite.Connection] = None
        self._coingecko = CoinGeckoClient()
        self._dexscreener = DexScreenerClient()
        self._pools: dict[str, dict[str, dict]] = {chain: {} for chain in CHAINS}
        self._update_task: Optional[asyncio.Task] = None

    async def initialize(self):
        self._db = await aiosqlite.connect(str(DB_PATH))
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS tokens (
                chain TEXT,
                address TEXT,
                symbol TEXT,
                name TEXT,
                coingecko_id TEXT,
                platforms_json TEXT,
                updated_at REAL,
                PRIMARY KEY (chain, address)
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS pools (
                chain TEXT,
                pair_address TEXT,
                token_address TEXT,
                dex_id TEXT,
                base_token_address TEXT,
                base_token_symbol TEXT,
                quote_token_address TEXT,
                quote_token_symbol TEXT,
                liquidity_usd REAL,
                is_v3 INTEGER DEFAULT 0,
                updated_at REAL,
                PRIMARY KEY (chain, pair_address)
            )
        """)
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await self._db.commit()
        await self._load_pools_from_db()

    async def _load_pools_from_db(self):
        """Load pools from local DB into memory."""
        async with self._db.execute("SELECT * FROM pools") as cursor:
            rows = await cursor.fetchall()

        for row in rows:
            chain = row[0]
            pair_address = row[1]
            token_address = row[2]
            dex_id = row[3]

            is_v3 = "v3" in dex_id or "aerodrome" in dex_id or "camelot" in dex_id

            self._pools[chain][pair_address] = {
                "token_address": token_address,
                "dex_id": dex_id,
                "base_token_address": row[4],
                "base_token_symbol": row[5],
                "quote_token_address": row[6],
                "quote_token_symbol": row[7],
                "liquidity_usd": row[8],
                "is_v3": is_v3,
            }

        total = sum(len(v) for v in self._pools.values())
        logger.info(f"Loaded {total} pools from DB")

    async def get_token_info(self, chain: str, token_address: str) -> Optional[dict]:
        """Get token info (symbol, name, platforms) from DB."""
        async with self._db.execute(
            "SELECT symbol, name, coingecko_id, platforms_json FROM tokens WHERE chain = ? AND address = ?",
            (chain, token_address)
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            return {
                "symbol": row[0],
                "name": row[1],
                "coingecko_id": row[2],
                "platforms": json.loads(row[3]) if row[3] else {},
            }
        return None

    def get_pools(self, chain: str) -> dict[str, dict]:
        """Get all monitored pools for a chain. {pair_address: pool_info}"""
        return self._pools.get(chain, {})

    def get_all_pool_addresses(self, chain: str) -> list[str]:
        """Get list of all pool addresses for a chain."""
        return list(self._pools.get(chain, {}).keys())

    async def update_pools(self):
        """Full update cycle: CoinGecko -> DexScreener -> DB."""
        logger.info("Starting pool list update...")
        start = time.time()

        try:
            cex_tokens = await self._coingecko.get_cex_tokens()
        except Exception as e:
            logger.error(f"Failed to fetch CEX tokens: {e}")
            return

        for chain_name in CHAINS:
            tokens = cex_tokens.get(chain_name, {})
            if not tokens:
                continue

            token_addresses = list(tokens.keys())
            logger.info(f"[{chain_name}] Fetching pools for {len(token_addresses)} tokens...")

            try:
                chain_slug = CHAINS[chain_name]["dexscreener_slug"]
                pools_data = await self._dexscreener.get_pools_for_tokens(
                    chain_slug, token_addresses
                )
            except Exception as e:
                logger.error(f"[{chain_name}] Failed to fetch pools: {e}")
                continue

            now = time.time()

            for token_addr, token_info in tokens.items():
                await self._db.execute(
                    """INSERT OR REPLACE INTO tokens
                       (chain, address, symbol, name, coingecko_id, platforms_json, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        chain_name, token_addr,
                        token_info["symbol"], token_info["name"],
                        token_info["coingecko_id"],
                        json.dumps(token_info.get("platforms", {})),
                        now,
                    )
                )

            for token_addr, pool_list in pools_data.items():
                for pool in pool_list:
                    pair_addr = pool["pair_address"]
                    dex_id = pool["dex_id"]
                    is_v3 = "v3" in dex_id or "aerodrome" in dex_id or "camelot" in dex_id

                    await self._db.execute(
                        """INSERT OR REPLACE INTO pools
                           (chain, pair_address, token_address, dex_id,
                            base_token_address, base_token_symbol,
                            quote_token_address, quote_token_symbol,
                            liquidity_usd, is_v3, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            chain_name, pair_addr, token_addr, dex_id,
                            pool["base_token"]["address"],
                            pool["base_token"]["symbol"],
                            pool["quote_token"]["address"],
                            pool["quote_token"]["symbol"],
                            pool["liquidity_usd"],
                            int(is_v3),
                            now,
                        )
                    )

                    self._pools[chain_name][pair_addr] = {
                        "token_address": token_addr,
                        "dex_id": dex_id,
                        "base_token_address": pool["base_token"]["address"],
                        "base_token_symbol": pool["base_token"]["symbol"],
                        "quote_token_address": pool["quote_token"]["address"],
                        "quote_token_symbol": pool["quote_token"]["symbol"],
                        "liquidity_usd": pool["liquidity_usd"],
                        "is_v3": is_v3,
                    }

            await self._db.commit()

        await self._db.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("last_update", str(time.time()))
        )
        await self._db.commit()

        total = sum(len(v) for v in self._pools.values())
        elapsed = time.time() - start
        logger.info(f"Pool update complete: {total} pools in {elapsed:.0f}s")

    async def needs_update(self) -> bool:
        """Check if pool list needs updating (older than 20 hours)."""
        async with self._db.execute(
            "SELECT value FROM meta WHERE key = 'last_update'"
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            return True

        last_update = float(row[0])
        hours_since = (time.time() - last_update) / 3600
        return hours_since > 20

    async def start_scheduled_updates(self):
        """Start background task for daily pool updates."""
        self._update_task = asyncio.create_task(self._update_loop())

    async def _update_loop(self):
        while True:
            try:
                if await self.needs_update():
                    await self.update_pools()

                hours, minutes = map(int, POOL_UPDATE_TIME.split(":"))
                now = time.time()
                import datetime
                target = datetime.datetime.utcnow().replace(
                    hour=hours, minute=minutes, second=0, microsecond=0
                )
                if target.timestamp() <= now:
                    target += datetime.timedelta(days=1)

                wait_seconds = target.timestamp() - now
                logger.info(f"Next pool update in {wait_seconds/3600:.1f} hours")
                await asyncio.sleep(wait_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Update loop error: {e}")
                await asyncio.sleep(300)

    async def close(self):
        if self._update_task:
            self._update_task.cancel()
        await self._coingecko.close()
        await self._dexscreener.close()
        if self._db:
            await self._db.close()
