import asyncio
import logging
from typing import Optional

import aiohttp

from config import DEXSCREENER_BASE_URL, MIN_LIQUIDITY_USD

logger = logging.getLogger(__name__)


class DexScreenerClient:
    """Finds DEX pool addresses for tokens via DexScreener API."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit = asyncio.Semaphore(1)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, endpoint: str) -> Optional[dict]:
        async with self._rate_limit:
            session = await self._get_session()
            url = f"{DEXSCREENER_BASE_URL}{endpoint}"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 429:
                        logger.warning("DexScreener rate limit, waiting 65s")
                        await asyncio.sleep(65)
                        return await self._request(endpoint)
                    if resp.status != 200:
                        logger.error(f"DexScreener {endpoint} returned {resp.status}")
                        return None
                    return await resp.json()
            except Exception as e:
                logger.error(f"DexScreener request failed: {e}")
                return None

    async def get_pools_for_tokens(
        self, chain: str, token_addresses: list[str]
    ) -> dict[str, list[dict]]:
        """
        Batch fetch pools for multiple token addresses on a chain.
        DexScreener supports up to 30 addresses per request.
        Returns: {token_address: [{pair_address, dex_id, base_token, quote_token, liquidity_usd}]}
        """
        result: dict[str, list[dict]] = {}
        batches = [token_addresses[i:i+30] for i in range(0, len(token_addresses), 30)]

        for batch in batches:
            addresses_str = ",".join(batch)
            data = await self._request(f"/tokens/{addresses_str}")

            if not data or "pairs" not in data:
                await asyncio.sleep(2)
                continue

            for pair in data.get("pairs", []):
                if pair.get("chainId") != chain:
                    continue

                liquidity = pair.get("liquidity", {}).get("usd", 0) or 0
                if liquidity < MIN_LIQUIDITY_USD:
                    continue

                base_addr = pair.get("baseToken", {}).get("address", "").lower()
                quote_addr = pair.get("quoteToken", {}).get("address", "").lower()

                token_addr = None
                for addr in batch:
                    if addr == base_addr or addr == quote_addr:
                        token_addr = addr
                        break

                if not token_addr:
                    continue

                pool_info = {
                    "pair_address": pair.get("pairAddress", "").lower(),
                    "dex_id": pair.get("dexId", ""),
                    "base_token": {
                        "address": base_addr,
                        "symbol": pair.get("baseToken", {}).get("symbol", ""),
                    },
                    "quote_token": {
                        "address": quote_addr,
                        "symbol": pair.get("quoteToken", {}).get("symbol", ""),
                    },
                    "liquidity_usd": liquidity,
                }

                if token_addr not in result:
                    result[token_addr] = []
                result[token_addr].append(pool_info)

            await asyncio.sleep(2)

        total_pools = sum(len(v) for v in result.values())
        logger.info(f"DexScreener [{chain}]: found {total_pools} pools for {len(result)} tokens")
        return result
