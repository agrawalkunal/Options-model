"""Schwab API client for real-time stock and options data.

This module provides a wrapper around the Schwab API for fetching:
- Real-time stock quotes
- Options chain data with Greeks
- Historical price data

Note: Requires Schwab developer API credentials. Set up at developer.schwab.com
"""

import os
import json
import base64
import requests
from datetime import datetime, timedelta
from typing import Optional
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Schwab API endpoints
BASE_URL = "https://api.schwabapi.com"
MARKET_DATA_URL = f"{BASE_URL}/marketdata/v1"
TOKEN_URL = f"{BASE_URL}/v1/oauth/token"


class SchwabClient:
    """Wrapper for Schwab API with automatic token refresh."""

    def __init__(self):
        self.app_key = os.getenv("SCHWAB_APP_KEY")
        self.app_secret = os.getenv("SCHWAB_APP_SECRET")
        self.token_path = "schwab_token.json"
        self.tokens = None
        self.token_expiry = None
        self.use_fallback = False

        if self.app_key and self.app_secret:
            self._load_tokens()
        else:
            logger.warning("Schwab API credentials not found.")
            self.use_fallback = True

    def _load_tokens(self):
        """Load tokens from file."""
        try:
            if os.path.exists(self.token_path):
                with open(self.token_path, 'r') as f:
                    self.tokens = json.load(f)
                # Set expiry time (tokens expire in 30 min, refresh before that)
                self.token_expiry = datetime.now() + timedelta(seconds=self.tokens.get('expires_in', 1800) - 60)
                logger.info("Schwab API tokens loaded successfully")
            else:
                logger.warning(f"Token file not found: {self.token_path}")
                self.use_fallback = True
        except Exception as e:
            logger.error(f"Failed to load tokens: {e}")
            self.use_fallback = True

    def _save_tokens(self):
        """Save tokens to file."""
        try:
            with open(self.token_path, 'w') as f:
                json.dump(self.tokens, f, indent=2)
            logger.info("Tokens saved successfully")
        except Exception as e:
            logger.error(f"Failed to save tokens: {e}")

    def _refresh_token(self):
        """Refresh the access token using the refresh token."""
        if not self.tokens or not self.tokens.get('refresh_token'):
            logger.error("No refresh token available")
            self.use_fallback = True
            return False

        try:
            credentials = f"{self.app_key}:{self.app_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()

            headers = {
                "Authorization": f"Basic {encoded_credentials}",
                "Content-Type": "application/x-www-form-urlencoded"
            }

            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.tokens['refresh_token']
            }

            response = requests.post(TOKEN_URL, headers=headers, data=data)

            if response.status_code == 200:
                self.tokens = response.json()
                self.token_expiry = datetime.now() + timedelta(seconds=self.tokens.get('expires_in', 1800) - 60)
                self._save_tokens()
                logger.info("Access token refreshed successfully")
                return True
            else:
                logger.error(f"Token refresh failed: {response.status_code} - {response.text}")
                self.use_fallback = True
                return False

        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            self.use_fallback = True
            return False

    def _ensure_valid_token(self):
        """Ensure we have a valid access token, refresh if needed."""
        if self.use_fallback:
            return False

        if not self.tokens:
            self._load_tokens()
            if not self.tokens:
                return False

        # Check if token is expired or about to expire
        if self.token_expiry and datetime.now() >= self.token_expiry:
            logger.info("Access token expired, refreshing...")
            return self._refresh_token()

        return True

    def _get_headers(self):
        """Get headers for API requests."""
        return {
            "Authorization": f"Bearer {self.tokens['access_token']}",
            "Accept": "application/json"
        }

    def get_quote(self, symbol: str) -> dict:
        """Get real-time quote for a symbol.

        Args:
            symbol: Stock ticker symbol (e.g., 'APP')

        Returns:
            dict with keys: price, bid, ask, volume, change, change_pct
        """
        if not self._ensure_valid_token():
            return self._get_quote_fallback(symbol)

        try:
            url = f"{MARKET_DATA_URL}/quotes"
            params = {"symbols": symbol}

            response = requests.get(url, headers=self._get_headers(), params=params)

            if response.status_code == 200:
                data = response.json()
                if symbol in data:
                    quote = data[symbol]['quote']
                    return {
                        'symbol': symbol,
                        'price': quote.get('lastPrice', 0),
                        'bid': quote.get('bidPrice', 0),
                        'ask': quote.get('askPrice', 0),
                        'volume': quote.get('totalVolume', 0),
                        'change': quote.get('netChange', 0),
                        'change_pct': quote.get('netPercentChangeInDouble', 0),
                        'timestamp': datetime.now().isoformat()
                    }
            elif response.status_code == 401:
                # Token might be invalid, try refresh
                if self._refresh_token():
                    return self.get_quote(symbol)

            logger.error(f"Quote request failed: {response.status_code} - {response.text}")
            return self._get_quote_fallback(symbol)

        except Exception as e:
            logger.error(f"Error fetching quote for {symbol}: {e}")
            return self._get_quote_fallback(symbol)

    def _get_quote_fallback(self, symbol: str) -> dict:
        """Fallback quote fetcher using yfinance."""
        try:
            import yfinance as yf
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
            logger.error(f"Fallback quote error for {symbol}: {e}")
            return {}

    def get_options_chain(self, symbol: str, expiration: Optional[str] = None) -> dict:
        """Get options chain for a symbol.

        Args:
            symbol: Stock ticker symbol
            expiration: Optional expiration date (YYYY-MM-DD). If None, returns nearest.

        Returns:
            dict with 'calls' and 'puts' DataFrames
        """
        if not self._ensure_valid_token():
            return self._get_options_chain_fallback(symbol, expiration)

        try:
            url = f"{MARKET_DATA_URL}/chains"
            params = {
                "symbol": symbol,
                "contractType": "ALL",
                "includeUnderlyingQuote": "true",
                "strategy": "SINGLE"
            }

            if expiration:
                params["fromDate"] = expiration
                params["toDate"] = expiration

            response = requests.get(url, headers=self._get_headers(), params=params)

            if response.status_code == 200:
                data = response.json()
                return self._parse_options_chain(data, symbol)
            elif response.status_code == 401:
                if self._refresh_token():
                    return self.get_options_chain(symbol, expiration)

            logger.error(f"Options chain request failed: {response.status_code} - {response.text}")
            return self._get_options_chain_fallback(symbol, expiration)

        except Exception as e:
            logger.error(f"Error fetching options chain for {symbol}: {e}")
            return self._get_options_chain_fallback(symbol, expiration)

    def _parse_options_chain(self, data: dict, symbol: str) -> dict:
        """Parse Schwab options chain response into DataFrames."""
        calls_list = []
        puts_list = []
        expirations = set()

        # Parse call options
        call_map = data.get('callExpDateMap', {})
        for exp_date, strikes in call_map.items():
            exp_date_clean = exp_date.split(':')[0]
            expirations.add(exp_date_clean)
            for strike, options in strikes.items():
                for opt in options:
                    calls_list.append({
                        'strike': float(strike),
                        'expiration': exp_date_clean,
                        'bid': opt.get('bid', 0),
                        'ask': opt.get('ask', 0),
                        'lastPrice': opt.get('last', 0),
                        'volume': opt.get('totalVolume', 0),
                        'openInterest': opt.get('openInterest', 0),
                        'impliedVolatility': opt.get('volatility', 0),
                        'delta': opt.get('delta', 0),
                        'gamma': opt.get('gamma', 0),
                        'theta': opt.get('theta', 0),
                        'vega': opt.get('vega', 0),
                        'contractSymbol': opt.get('symbol', '')
                    })

        # Parse put options
        put_map = data.get('putExpDateMap', {})
        for exp_date, strikes in put_map.items():
            exp_date_clean = exp_date.split(':')[0]
            expirations.add(exp_date_clean)
            for strike, options in strikes.items():
                for opt in options:
                    puts_list.append({
                        'strike': float(strike),
                        'expiration': exp_date_clean,
                        'bid': opt.get('bid', 0),
                        'ask': opt.get('ask', 0),
                        'lastPrice': opt.get('last', 0),
                        'volume': opt.get('totalVolume', 0),
                        'openInterest': opt.get('openInterest', 0),
                        'impliedVolatility': opt.get('volatility', 0),
                        'delta': opt.get('delta', 0),
                        'gamma': opt.get('gamma', 0),
                        'theta': opt.get('theta', 0),
                        'vega': opt.get('vega', 0),
                        'contractSymbol': opt.get('symbol', '')
                    })

        calls_df = pd.DataFrame(calls_list) if calls_list else None
        puts_df = pd.DataFrame(puts_list) if puts_list else None

        return {
            'calls': calls_df,
            'puts': puts_df,
            'expiration': sorted(expirations)[0] if expirations else None,
            'expirations': sorted(list(expirations)),
            'underlying_price': data.get('underlyingPrice', 0),
            'timestamp': datetime.now().isoformat()
        }

    def _get_options_chain_fallback(self, symbol: str, expiration: Optional[str] = None) -> dict:
        """Fallback options chain fetcher using yfinance."""
        try:
            import yfinance as yf
            ticker = yf.Ticker(symbol)
            expirations = ticker.options

            if not expirations:
                return {'calls': None, 'puts': None, 'expirations': []}

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
            logger.error(f"Fallback options chain error for {symbol}: {e}")
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
        # For now, use yfinance for historical data
        # Schwab's historical data endpoint has different parameters
        try:
            import yfinance as yf
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
