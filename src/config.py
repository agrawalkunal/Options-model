"""Configuration settings for APP Options Trading Model."""

import os
from dotenv import load_dotenv

load_dotenv()

# Stock Configuration
TICKER = "APP"
ENTRY_DAYS = ["Thursday", "Friday"]  # Days to look for entries
TARGET_DTE = [0, 1, 2]  # 0-2 DTE options
TARGET_GAIN = 7.5  # 750% = 7.5x multiplier

# Strike Selection
STRIKE_TYPES = ["OTM", "DEEP_OTM"]
OTM_THRESHOLD = 0.05  # 5% out of the money
DEEP_OTM_THRESHOLD = 0.10  # 10% out of the money

# Ad Industry Related Tickers (for sector news correlation)
AD_SECTOR_TICKERS = ["META", "GOOGL", "TTD", "MGNI", "PUBM", "DV"]

# News Keywords for Ad Industry
AD_INDUSTRY_KEYWORDS = [
    "digital advertising",
    "ad spend",
    "programmatic",
    "ROAS",
    "ad revenue",
    "mobile advertising",
    "app monetization",
    "connected TV",
    "CTV advertising",
]

# API Keys (from environment)
SCHWAB_APP_KEY = os.getenv("SCHWAB_APP_KEY")
SCHWAB_APP_SECRET = os.getenv("SCHWAB_APP_SECRET")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
ALPHA_VANTAGE_KEY = os.getenv("ALPHA_VANTAGE_KEY")

# Market Hours (Eastern Time)
MARKET_OPEN = "09:30"
MARKET_CLOSE = "16:00"
PREMARKET_SCAN = "09:00"

# Polling Interval (seconds)
SIGNAL_CHECK_INTERVAL = 300  # 5 minutes
