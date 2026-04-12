"""GMC Dashboard Configuration -- Example
Copy this to config.py and fill in your values.
Never commit config.py to git.
"""

# Paths
POSITIONS_DB = "/path/to/gmc_data/positions.db"
COINBASE_CDP_KEY = "/path/to/crypto_backtest/cdp_api_key.json"

# IB Gateway
IB_GATEWAY_URL = "https://localhost:7462/v1/api"
IB_ACCOUNT_LIVE = "YOUR_LIVE_ACCOUNT"
IB_ACCOUNT_PAPER = "YOUR_PAPER_ACCOUNT"
EVENT_ALPHA_FALLBACK = 10000

# FMP API
FMP_API_KEY = "YOUR_FMP_API_KEY"

# Bedrock holdings
BEDROCK_ENTRY_DATE = "2026-04-07"
BEDROCK_HOLDINGS = [
    {"ticker": "ASML", "shares": 1, "cost_per_share": 0.00},
    {"ticker": "TSM",  "shares": 3, "cost_per_share": 0.00},
    {"ticker": "ANET", "shares": 9, "cost_per_share": 0.00},
    {"ticker": "VRT",  "shares": 5, "cost_per_share": 0.00},
]
BEDROCK_TOTAL_COST = 0.00

# Digital Alpha
DIGITAL_ALPHA_BASELINE = 0.00
DIGITAL_ALPHA_BASELINE_DATE = "2026-04-10"

# Dashboard
DASHBOARD_PORT = 5050
DASHBOARD_HOST = "0.0.0.0"
