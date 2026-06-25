import asyncio
import logging
from typing import Optional

import aiohttp

from config import COINGECKO_BASE_URL, CHAINS

logger = logging.getLogger(__name__)

PLATFORM_MAP = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bsc",
    "base": "base",
    "arbitrum-one": "arbitrum",
}

PLATFORM_MAP_REVERSE = {v: k for k, v in PLATFORM_MAP.items()}


class CoinGeckoClient:
    """
    CoinGecko client for free tier.
    
    Strategy:
    1. /coins/list?include_platform=true → all coins with addresses (1 call)
    2. /coins/markets (top by market cap, all pages) → these are all on CEX
    
    Handles rate limits gracefully: waits and retries without deadlock.
    """

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _request(self, endpoint: str, params: dict = None, max_retries: int = 5) -> Optional[dict | list]:
        """Single request with rate-limit retry (no deadlock)."""
        session = await self._get_session()
        url = f"{COINGECKO_BASE_URL}{endpoint}"

        for attempt in range(max_retries):
            await asyncio.sleep(12.0)

            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 429:
                        wait = 65
                        logger.warning(f"CoinGecko rate limit, retry in {wait}s (attempt {attempt+1})...")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status == 404:
                        logger.warning(f"CoinGecko {endpoint} 404 (skipping)")
                        return None
                    if resp.status != 200:
                        logger.error(f"CoinGecko {endpoint} returned {resp.status}")
                        return None
                    return await resp.json()
            except asyncio.TimeoutError:
                logger.warning(f"CoinGecko {endpoint} timeout (attempt {attempt+1})")
                await asyncio.sleep(10)
                continue
            except Exception as e:
                logger.error(f"CoinGecko request error: {e}")
                return None

        logger.error(f"CoinGecko {endpoint} failed after {max_retries} attempts")
        return None

    async def get_cex_tokens(self) -> dict[str, dict]:
        """
        Returns: {chain_name: {token_address_lower: {symbol, name, coingecko_id, platforms}}}
        
        Fetches ALL top coins by market cap. Handles rate limits patiently.
        """
        # Step 1: Get all coins with platform addresses (1 call)
        logger.info("CoinGecko: fetching coin list with platforms...")
        coins_list = await self._request("/coins/list", {"include_platform": "true"})
        if not coins_list:
            logger.error("Failed to fetch coins list")
            return {chain: {} for chain in CHAINS}

        # Build lookup: coingecko_id → {symbol, name, chains: {chain: address}}
        coin_lookup: dict[str, dict] = {}
        for coin in coins_list:
            cg_id = coin.get("id", "")
            platforms = coin.get("platforms", {})
            if not platforms:
                continue

            chain_addresses = {}
            for platform_name, addr in platforms.items():
                if platform_name in PLATFORM_MAP and addr:
                    chain_name = PLATFORM_MAP[platform_name]
                    chain_addresses[chain_name] = addr.lower()

            if chain_addresses:
                coin_lookup[cg_id] = {
                    "symbol": coin.get("symbol", "").upper(),
                    "name": coin.get("name", ""),
                    "chains": chain_addresses,
                }

        logger.info(f"CoinGecko: {len(coin_lookup)} coins have addresses on our chains")

        # Step 2: Get top coins by market cap — all are on CEX
        # Fetch until we have ~2000 or no more data
        cex_coin_ids: set[str] = set()
        per_page = 250
        max_pages = 10  # up to 2500 coins

        for page in range(1, max_pages + 1):
            logger.info(f"CoinGecko: fetching market data page {page}/{max_pages} "
                        f"(have {len(cex_coin_ids)} coins so far)...")
            data = await self._request("/coins/markets", {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": str(per_page),
                "page": str(page),
                "sparkline": "false",
            })

            if not data:
                logger.warning(f"CoinGecko: page {page} failed, continuing with what we have")
                break

            for coin in data:
                cg_id = coin.get("id", "")
                if cg_id:
                    cex_coin_ids.add(cg_id)

            logger.info(f"CoinGecko: page {page} done (+{len(data)} coins)")

            if len(data) < per_page:
                break

        logger.info(f"CoinGecko: {len(cex_coin_ids)} top coins fetched total")

        # Step 3: Cross-reference — coins that are in top marketcap AND on our chains
        result: dict[str, dict] = {chain: {} for chain in CHAINS}

        for cg_id in cex_coin_ids:
            coin_data = coin_lookup.get(cg_id)
            if not coin_data:
                continue

            chains = coin_data["chains"]
            for chain_name, address in chains.items():
                other_chains = {k: v for k, v in chains.items() if k != chain_name}
                result[chain_name][address] = {
                    "symbol": coin_data["symbol"],
                    "name": coin_data["name"],
                    "coingecko_id": cg_id,
                    "platforms": other_chains,
                }

        total = sum(len(v) for v in result.values())
        logger.info(f"CoinGecko: DONE — {total} tokens across all chains")
        for chain, tokens in result.items():
            logger.info(f"  {chain}: {len(tokens)} tokens")

        return result
