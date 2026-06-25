import asyncio
import logging
from typing import Optional

import aiohttp

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, CHAINS

logger = logging.getLogger(__name__)

CHAIN_EXPLORERS = {
    "ethereum": "https://etherscan.io",
    "bsc": "https://bscscan.com",
    "base": "https://basescan.org",
    "arbitrum": "https://arbiscan.io",
}


class TelegramAlert:
    """Sends formatted dump alerts to Telegram (2-message approach)."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._rate_limit = asyncio.Semaphore(5)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def send_dump_alert(
        self,
        chain: str,
        token_info: dict,
        dump_info: dict,
        pool_info: dict,
    ):
        """
        Message 1: Instant dump alert (no waiting for wallet check).
        Sent immediately when dump detected.
        """
        symbol = token_info.get("symbol", "???")
        change_pct = abs(dump_info["change_pct"])
        price_before = dump_info["price_before"]
        price_after = dump_info["price_after"]
        token_address = pool_info.get("token_address", "")
        platforms = token_info.get("platforms", {})

        price_before_str = self._format_price(price_before)
        price_after_str = self._format_price(price_after)

        lines = [
            f"🚨 {symbol} DN {change_pct:.2f}%",
            f"📉 {price_before_str} --> {price_after_str}",
            f"💱 Chain: {chain}",
            f"📋 Contract: `{token_address}`",
        ]

        if platforms:
            other_lines = []
            for other_chain, other_addr in platforms.items():
                other_lines.append(f"{other_chain}: `{other_addr}`")
            if other_lines:
                lines.append("Other chains: " + "\n".join(other_lines))

        lines.append("")

        explorer = CHAIN_EXPLORERS.get(chain, "")
        dexscreener_chain = CHAINS[chain]["dexscreener_slug"]

        token_links = [
            f"[CMC](https://dex.coinmarketcap.com/token/{dexscreener_chain}/{token_address}/)",
            f"[Defined](https://www.defined.fi/{dexscreener_chain}/{token_address}/)",
            f"[OKX](https://web3.okx.com/token/{dexscreener_chain}/{token_address}/)",
            f"[Arkham](https://intel.arkm.com/explorer/address/{token_address})",
        ]
        lines.append(" | ".join(token_links))

        if dump_info.get("num_swaps", 1) > 1:
            lines.append(f"⚡ {dump_info['num_swaps']} swaps in 1s")

        message = "\n".join(lines)
        await self._send_message(message)

    async def send_seller_alert(
        self,
        chain: str,
        token_info: dict,
        seller_info: dict,
    ):
        """
        Message 2: Seller wallet info (arrives ~0.5s after dump alert).
        """
        symbol = token_info.get("symbol", "???")
        seller_addr = seller_info["seller_address"]
        sold_pct = seller_info["sold_pct"]
        sold_all = seller_info["sold_all"]

        if sold_all:
            status = f"💀 SOLD ALL 100% ✅"
        else:
            status = f"💀 SOLD {sold_pct:.0f}%"

        explorer = CHAIN_EXPLORERS.get(chain, "")

        lines = [
            f"👛 {symbol} seller:",
            f"`{seller_addr}`",
            status,
            "",
        ]

        wallet_links = [
            f"[DeBank](https://debank.com/profile/{seller_addr})",
            f"[Arkham](https://intel.arkm.com/explorer/address/{seller_addr})",
        ]
        if explorer:
            wallet_links.append(f"[Explorer]({explorer}/address/{seller_addr})")

        lines.append(" | ".join(wallet_links))

        message = "\n".join(lines)
        await self._send_message(message)

    async def _send_message(self, text: str):
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured, alert skipped")
            logger.info(f"ALERT:\n{text}")
            return

        async with self._rate_limit:
            session = await self._get_session()
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }

            try:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.error(f"Telegram send failed ({resp.status}): {body}")
                        if "can't parse entities" in body.lower():
                            payload["parse_mode"] = None
                            payload["text"] = text.replace("`", "").replace("[", "").replace("]", "").replace("(", " ").replace(")", "")
                            async with session.post(url, json=payload) as retry_resp:
                                if retry_resp.status != 200:
                                    logger.error("Telegram retry also failed")
                    else:
                        logger.debug("Telegram alert sent")
            except Exception as e:
                logger.error(f"Telegram send error: {e}")

    @staticmethod
    def _format_price(price: float) -> str:
        """Compact, readable formatting with ~6 significant figures.

        The price is a token1/token0 ratio (unit is the paired token, not USD);
        detection is %-based so the absolute unit does not matter — this only
        keeps the displayed before/after numbers human-readable.
        """
        if price == 0:
            return "0"
        abs_p = abs(price)
        if abs_p >= 1e7 or abs_p < 1e-7:
            return f"{price:.4e}"
        if abs_p >= 1:
            return f"{price:.6g}"
        return f"{price:.6g}"
