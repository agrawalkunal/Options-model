"""Schwab API client for real-time stock and options data.

This module provides a wrapper around the Schwab API for fetching:
- Real-time stock quotes
- Options chain data with Greeks
- Historical price data

Note: Requires Schwab developer API credentials. Set up at developer.schwab.com
"""

import os
from datetime import datetime, timedelta
from typing import Optional
import logging

# Placeholder imports - uncomment when schwab-py is available
# from schwab import auth, client
# from schwab.client import Client

import yfinance as yf  # Fallback for development

logger = logging.getLogger(__name__)


class SchwabClient:
    """Wrapper for Schwab API with fallback to yfinance for development."""

    def __init__(self):
        self.app_key = os.getenv("SCHWAB_APP_KEY")
        self.app_secret = os.getenv("SCHWAB_APP_SECRET")
        self.callback_url = os.getenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1")
        self.token_path = "schwab_token.json"
        self.client = None
        self.use_fallback = True  # Use yfinance until Schwab API is ready

        if self.app_key and self.app_secret:
            self._initialize_client()
        else:
            logger.warning("Schwab API credentials not found. Using yfinance fallback.")

    def _initialize_client(self):
        """Initialize the Schwab API client with OAuth."""
        try:
            # TODO: Uncomment when Schwab API is approved
            # self.client = auth.client_from_token_file(
            #     self.token_path,
            #     self.app_key,
            #     self.app_secret
            # )
            # self.use_fallback = False
            # logger.info("Schwab API client initialized successfully")
            logger.info("Schwab API credentials found. Waiting for API approval.")
        except Exception as e:
            logger.error(f"Failed to initialize Schwab client: {e}")
            logger.info("Falling back to yfinance for data")

    def authenticate(self):
        """Run OAuth flow for initial authentication.

        Call this method once to authenticate and save the token.
        """
        if not self.app_key or not self.app_secret:
            raise ValueError("Schwab API credentials not configured")

        # TODO: Uncomment when Schwab API is approved
        # from schwab import auth
        # auth.client_from_manual_flow(
        #     self.app_key,
        #     self.app_secret,
        #     self.callback_url,
        #     self.token_path
        # )
        logger.info("OAuth flow would run here. Waiting for API approval.")

    def get_quote(self, symbol: str) -> dict:
        """Get real-time quote for a symbol.

        Args:
            symbol: Stock ticker symbol (e.g., 'APP')

        Returns:
            dict with keys: price, bid, ask, volume, change, change_pct
        """
        if self.use_fallback:
            return self._get_quote_yfinance(symbol)

        # TODO: Implement Schwab API call
        # response = self.client.get_quote(symbol)
        # return self._parse_quote_response(response)
        return self._get_quote_yfinance(symbol)

    def _get_quote_yfinance(self, symbol: str) -> dict:
        """Fallback quote fetcher using yfinance."""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period='1d')

            if hist.empty:
                return {}

            current = hist['Close'].iloc[-1]
            prev_close = info.get('previousClose', current)
            change = current - prev_close
            change_pct = (change / prev_close) * 100 if prev_close else 0

            return {
                'symbol': symbol,
                'price': current,
                'bid': info.get('bid', current),
                'ask': info.get('ask', current),
                'volume': int(hist['Volume'].iloc[-1]),
                'change': change,
                'change_pct': change_pct,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return {}

    def get_options_chain(self, symbol: str, expiration: Optional[str] = None) -> dict:
        """Get options chain for a symbol.

        Args:
            symbol: Stock ticker symbol
            expiration: Optional expiration date (YYYY-MM-DD). If None, returns nearest.

        Returns:
            dict with 'calls' and 'puts' DataFrames
        """
        if self.use_fallback:
            return self._get_options_chain_yfinance(symbol, expiration)

        # TODO: Implement Schwab API call
        return self._get_options_chain_yfinance(symbol, expiration)

    def _get_options_chain_yfinance(self, symbol: str, expiration: Optional[str] = None) -> dict:
        """Fallback options chain fetcher using yfinance."""
        try:
            ticker = yf.Ticker(symbol)
            expirations = ticker.options

            if not expirations:
                return {'calls': None, 'puts': None, 'expirations': []}

            # Use specified expiration or nearest
            if expiration and expiration in expirations:
                exp_date = expiration
            else:
                exp_date = expirations[0]

            chain = ticker.option_chain(exp_date)

            return {
                'calls': chain.calls,
                'puts': chain.puts,
                'expiration': exp_date,
                'expirations': list(expirations),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching options chain for {symbol}: {e}")
            return {'calls': None, 'puts': None, 'expirations': []}

    def get_history(self, symbol: str, period: str = '1d', interval: str = '5m') -> dict:
        """Get historical price data.

        Args:
            symbol: Stock ticker symbol
            period: Time period ('1d', '5d', '1mo', '1y')
            interval: Data interval ('1m', '5m', '1h', '1d')

        Returns:
            dict with OHLCV data
        """
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period, interval=interval)

            return {
                'symbol': symbol,
                'data': hist,
                'period': period,
                'interval': interval,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching history for {symbol}: {e}")
            return {'data': None}

    def is_market_open(self) -> bool:
        """Check if the market is currently open."""
        now = datetime.now()

        # Check if weekend
        if now.weekday() >= 5:
            return False

        # Check market hours (9:30 AM - 4:00 PM ET)
        # Note: This is simplified - doesn't account for holidays
        market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)

        return market_open <= now <= market_close


# Singleton instance
_client = None


def get_client() -> SchwabClient:
    """Get the singleton SchwabClient instance."""
    global _client
    if _client is None:
        _client = SchwabClient()
    return _client
