import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY", "")

DUMP_THRESHOLD_PCT = float(os.getenv("DUMP_THRESHOLD_PCT", "20"))
MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "50000"))
POOL_UPDATE_TIME = os.getenv("POOL_UPDATE_TIME", "03:00")

CHAINS = {
    "ethereum": {
        "chain_id": 1,
        "ws_url": "wss://ethereum-rpc.publicnode.com",
        "ws_url_backup": f"wss://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}" if ALCHEMY_API_KEY else None,
        "block_time": 12,
        "dexscreener_slug": "ethereum",
        "dexes": {
            "uniswap_v2": {
                "factory": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
            },
            "uniswap_v3": {
                "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
            },
        },
    },
    "bsc": {
        "chain_id": 56,
        "ws_url": "wss://bsc-rpc.publicnode.com",
        "ws_url_backup": None,
        "block_time": 3,
        "dexscreener_slug": "bsc",
        "dexes": {
            "pancakeswap_v2": {
                "factory": "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",
            },
            "pancakeswap_v3": {
                "factory": "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865",
            },
        },
    },
    "base": {
        "chain_id": 8453,
        "ws_url": "wss://base-rpc.publicnode.com",
        "ws_url_backup": f"wss://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}" if ALCHEMY_API_KEY else None,
        "block_time": 2,
        "dexscreener_slug": "base",
        "dexes": {
            "uniswap_v3": {
                "factory": "0x33128a8fC17869897dcE68Ed026d694621f6FDfD",
            },
            "aerodrome": {
                "factory": "0x420DD381b31aEf6683db6B902084cB0FFECe40Da",
            },
        },
    },
    "arbitrum": {
        "chain_id": 42161,
        "ws_url": "wss://arbitrum-one-rpc.publicnode.com",
        "ws_url_backup": f"wss://arb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}" if ALCHEMY_API_KEY else None,
        "block_time": 0.25,
        "dexscreener_slug": "arbitrum",
        "dexes": {
            "uniswap_v3": {
                "factory": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
            },
            "camelot_v3": {
                "factory": "0x1a3c9B1d2F0529e10aE05c97F96a237b6DAf038E",
            },
        },
    },
}

SWAP_EVENT_V2 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SYNC_EVENT_V2 = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
SWAP_EVENT_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
DEXSCREENER_BASE_URL = "https://api.dexscreener.com/latest/dex"

# CoinGecko exchange IDs (not display names!)
MAJOR_CEXES = [
    "binance",
    "gdax",          # Coinbase
    "okx",
    "bybit_spot",
    "kraken",
    "kucoin",
    "upbit",
]
