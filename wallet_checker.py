import asyncio
import logging
from typing import Any, Optional

import aiohttp

from config import CHAINS

logger = logging.getLogger(__name__)

BALANCE_OF_SELECTOR = "0x70a08231"

RPC_HTTP_URLS = {
    "ethereum": "https://ethereum-rpc.publicnode.com",
    "bsc": "https://bsc-rpc.publicnode.com",
    "base": "https://base-rpc.publicnode.com",
    "arbitrum": "https://arbitrum-one-rpc.publicnode.com",
}


class WalletChecker:
    """
    Identifies seller wallet and checks how much they sold (%).
    2-3 RPC calls per dump event — negligible load.
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

    async def _rpc_call(self, chain: str, method: str, params: list) -> Optional[Any]:
        url = RPC_HTTP_URLS.get(chain)
        if not url:
            return None

        session = await self._get_session()
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }

        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data.get("result")
        except Exception as e:
            logger.error(f"RPC [{chain}] {method}: {e}")
            return None

    async def get_seller_info(self, chain: str, tx_hash: str, token_address: str) -> Optional[dict]:
        """
        Get seller wallet + sell percentage.
        Returns: {
            "seller_address": "0x...",
            "sold_pct": float (0-100),
            "sold_all": bool,
        }
        """
        # Step 1: Get transaction → seller address + value/input for amounts
        tx_data = await self._rpc_call(chain, "eth_getTransactionByHash", [tx_hash])
        if not tx_data:
            return None

        seller_address = tx_data.get("from", "").lower()
        if not seller_address:
            return None

        # Step 2: Get tx receipt to find exact token transfer amounts
        receipt = await self._rpc_call(chain, "eth_getTransactionReceipt", [tx_hash])
        amount_sold = 0
        if receipt:
            amount_sold = self._extract_token_transfer_amount(
                receipt.get("logs", []), token_address, seller_address
            )

        # Step 3: Get current balance of seller
        remaining = await self._get_token_balance(chain, token_address, seller_address)

        # Calculate percentage sold
        if amount_sold > 0:
            balance_before = remaining + amount_sold
            sold_pct = (amount_sold / balance_before) * 100 if balance_before > 0 else 100.0
        elif remaining == 0:
            sold_pct = 100.0
        else:
            sold_pct = 0.0

        sold_all = remaining == 0 or sold_pct >= 99.9

        return {
            "seller_address": seller_address,
            "sold_pct": min(sold_pct, 100.0),
            "sold_all": sold_all,
            "remaining_raw": remaining,
        }

    def _extract_token_transfer_amount(
        self, logs: list, token_address: str, from_address: str
    ) -> int:
        """
        Find Transfer(from, to, amount) events in tx logs to determine how much was sold.
        Transfer event: topic[0] = keccak256("Transfer(address,address,uint256)")
        """
        transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        total_sent = 0

        for log in logs:
            if log.get("address", "").lower() != token_address.lower():
                continue
            topics = log.get("topics", [])
            if len(topics) < 3:
                continue
            if topics[0] != transfer_topic:
                continue

            # topics[1] = from address (padded to 32 bytes)
            log_from = "0x" + topics[1][-40:]
            if log_from.lower() == from_address.lower():
                data = log.get("data", "0x")
                if data and data != "0x":
                    try:
                        amount = int(data, 16)
                        total_sent += amount
                    except ValueError:
                        pass

        return total_sent

    async def _get_token_balance(self, chain: str, token_address: str, wallet_address: str) -> int:
        """Call ERC-20 balanceOf(wallet)."""
        padded_address = wallet_address.replace("0x", "").lower().zfill(64)
        call_data = BALANCE_OF_SELECTOR + padded_address

        result = await self._rpc_call(chain, "eth_call", [
            {"to": token_address, "data": call_data},
            "latest"
        ])

        if not result or result == "0x":
            return 0

        try:
            return int(result, 16)
        except (ValueError, TypeError):
            return 0
