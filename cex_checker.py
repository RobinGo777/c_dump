import asyncio
import logging
from typing import Optional

import aiohttp

from config import COINGECKO_BASE_URL

logger = logging.getLogger(__name__)

# Known DEX identifiers to exclude (everything else = CEX)
KNOWN_DEX_IDENTIFIERS = {
    "uniswap_v2", "uniswap_v3", "sushiswap", "pancakeswap_v2", "pancakeswap_v3",
    "curve", "balancer", "aerodrome", "camelot", "trader_joe", "quickswap",
    "spookyswap", "velodrome", "raydium", "orca", "jupiter",
    "dodo", "kyberswap", "1inch", "paraswap", "maverick",
    "thena", "baseswap", "spiritswap", "biswap",
}


class CexChecker:
    """
    Verifies if a token is listed on ANY centralized exchange at alert time.
    Any exchange that isn't a known DEX counts as CEX.
    Single API call per check — only invoked on rare dump events.
    Caches results to avoid repeated calls for the same token.
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        # Cache: coingecko_id -> (is_on_cex: bool, exchange_names: list)
        self._cache: dict[str, tuple[bool, list[str]]] = {}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def is_on_cex(self, coingecko_id: str) -> tuple[bool, list[str]]:
        """
        Check if token is traded on any major CEX.
        Returns (is_on_cex, list_of_exchange_names).
        Uses CoinGecko /coins/{id}/tickers (1 call, cached).
        """
        if not coingecko_id:
            return False, []

        if coingecko_id in self._cache:
            return self._cache[coingecko_id]

        session = await self._get_session()
        url = f"{COINGECKO_BASE_URL}/coins/{coingecko_id}/tickers"

        try:
            async with session.get(url, params={"depth": "false"},
                                   timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 429:
                    logger.warning("CexChecker: rate limited, assuming on CEX (safe fallback)")
                    return True, ["unknown (rate limited)"]
                if resp.status != 200:
                    logger.warning(f"CexChecker: {resp.status} for {coingecko_id}, assuming on CEX")
                    return True, ["unknown (api error)"]
                data = await resp.json()
        except Exception as e:
            logger.warning(f"CexChecker error: {e}, assuming on CEX")
            return True, ["unknown (error)"]

        tickers = data.get("tickers", [])
        cex_names = set()

        for ticker in tickers:
            market = ticker.get("market", {})
            market_id = market.get("identifier", "")
            market_name = market.get("name", "")

            # Any exchange that is NOT a known DEX = CEX
            if market_id and market_id not in KNOWN_DEX_IDENTIFIERS:
                cex_names.add(market_name)

        is_listed = len(cex_names) > 0
        result = (is_listed, list(cex_names))
        self._cache[coingecko_id] = result

        if is_listed:
            logger.info(f"CexChecker: {coingecko_id} is on CEX: {', '.join(cex_names)}")
        else:
            logger.info(f"CexChecker: {coingecko_id} NOT on any major CEX — skipping alert")

        return result
