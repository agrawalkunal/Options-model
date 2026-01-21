"""Base signal class for APP options trading signals."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from typing import Optional, List, Tuple
import logging

logger = logging.getLogger(__name__)

# Maximum option price for recommendations
MAX_OPTION_PRICE = 1.00


class SignalDirection(Enum):
    """Direction of the trading signal."""
    CALL = "CALL"
    PUT = "PUT"
    NEUTRAL = "NEUTRAL"


class SignalStrength(Enum):
    """Strength/confidence of the signal."""
    WEAK = 1
    MODERATE = 2
    STRONG = 3


@dataclass
class Signal:
    """Represents a trading signal."""
    name: str
    direction: SignalDirection
    strength: SignalStrength
    confidence: float  # 0.0 to 1.0
    timestamp: datetime
    details: dict
    recommended_strikes: List[dict] = None

    def __post_init__(self):
        if self.recommended_strikes is None:
            self.recommended_strikes = []

    @property
    def is_actionable(self) -> bool:
        """Check if signal meets minimum threshold for action."""
        return self.confidence >= 0.5 and self.direction != SignalDirection.NEUTRAL

    def to_dict(self) -> dict:
        """Convert signal to dictionary for serialization."""
        return {
            "name": self.name,
            "direction": self.direction.value,
            "strength": self.strength.name,
            "confidence": self.confidence,
            "timestamp": self.timestamp.isoformat(),
            "details": self.details,
            "recommended_strikes": self.recommended_strikes,
            "is_actionable": self.is_actionable
        }

    def __repr__(self):
        return f"Signal({self.name}, {self.direction.value}, confidence={self.confidence:.2f})"


class BaseSignal(ABC):
    """Abstract base class for signal detectors."""

    def __init__(self, name: str):
        self.name = name
        self.enabled = True

    @abstractmethod
    def check(self) -> Optional[Signal]:
        """Check for signal conditions.

        Returns:
            Signal object if conditions are met, None otherwise.
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """Get a description of what this signal detects."""
        pass

    def is_valid_trading_day(self) -> bool:
        """Check if today is a valid trading day (Thursday or Friday)."""
        today = datetime.now()
        # Thursday = 3, Friday = 4
        return today.weekday() in [3, 4]

    def is_friday(self) -> bool:
        """Check if today is Friday."""
        return datetime.now().weekday() == 4

    def is_thursday(self) -> bool:
        """Check if today is Thursday."""
        return datetime.now().weekday() == 3

    def is_valid_entry_window(self) -> bool:
        """Check if current time is within valid entry window.

        Entry windows:
        - Thursday: 9:30 AM - 4:00 PM ET
        - Friday: 9:30 AM - 3:00 PM ET
        """
        now = datetime.now()
        weekday = now.weekday()
        current_time = now.time()

        market_open = time(9, 30)

        if weekday == 4:  # Friday
            market_close = time(15, 0)  # 3:00 PM ET
        elif weekday == 3:  # Thursday
            market_close = time(16, 0)  # 4:00 PM ET
        else:
            return False

        return market_open <= current_time <= market_close

    def filter_strikes_by_price(self, strikes: List[dict], max_price: float = MAX_OPTION_PRICE) -> List[dict]:
        """Filter strikes to only include options under max price.

        Args:
            strikes: List of strike dictionaries with 'last_price' or 'ask' key
            max_price: Maximum option price (default $1.00)

        Returns:
            Filtered list of strikes
        """
        filtered = []
        for strike in strikes:
            price = strike.get('last_price') or strike.get('ask') or 0
            if price > 0 and price <= max_price:
                filtered.append(strike)
        return filtered

    def calculate_strike_recommendations(self, current_price: float,
                                         direction: SignalDirection) -> List[dict]:
        """Calculate recommended strikes based on current price and direction.

        Args:
            current_price: Current stock price
            direction: CALL or PUT direction

        Returns:
            List of recommended strike dictionaries
        """
        recommendations = []

        if direction == SignalDirection.CALL:
            # OTM calls (5% and 10% above current price)
            otm_5 = round(current_price * 1.05, 0)
            otm_10 = round(current_price * 1.10, 0)

            recommendations = [
                {"strike": otm_5, "type": "CALL", "otm_pct": 5, "risk": "moderate"},
                {"strike": otm_10, "type": "CALL", "otm_pct": 10, "risk": "high"},
            ]
        elif direction == SignalDirection.PUT:
            # OTM puts (5% and 10% below current price)
            otm_5 = round(current_price * 0.95, 0)
            otm_10 = round(current_price * 0.90, 0)

            recommendations = [
                {"strike": otm_5, "type": "PUT", "otm_pct": 5, "risk": "moderate"},
                {"strike": otm_10, "type": "PUT", "otm_pct": 10, "risk": "high"},
            ]

        return recommendations

    def enrich_strikes_with_live_prices(self, strikes: List[dict],
                                         symbol: str = 'APP',
                                         expiration: str = None) -> List[dict]:
        """Fetch live bid/ask prices for recommended strikes from Schwab API.

        Args:
            strikes: List of strike recommendation dicts
            symbol: Stock symbol
            expiration: Optional expiration date (YYYY-MM-DD)

        Returns:
            Enhanced strikes with live 'bid' and 'ask' prices
        """
        try:
            from ..data.schwab_client import get_client
            client = get_client()
            chain = client.get_options_chain(symbol, expiration)

            calls_df = chain.get('calls')
            puts_df = chain.get('puts')

            for strike in strikes:
                strike_price = strike.get('strike')
                option_type = strike.get('type', 'CALL')

                # Select the appropriate dataframe
                df = calls_df if option_type == 'CALL' else puts_df

                if df is not None and not df.empty:
                    # Find matching strike
                    matches = df[df['strike'] == strike_price]
                    if not matches.empty:
                        row = matches.iloc[0]
                        strike['bid'] = row.get('bid', 0) or 0
                        strike['ask'] = row.get('ask', 0) or 0
                        strike['last_price'] = row.get('lastPrice', 0) or 0
                        strike['volume'] = int(row.get('volume', 0)) if row.get('volume') else 0
                        strike['open_interest'] = int(row.get('openInterest', 0)) if row.get('openInterest') else 0

            return strikes

        except Exception as e:
            logger.warning(f"Failed to enrich strikes with live prices: {e}")
            return strikes

    def evaluate_price_comparison(self, strikes: List[dict], stock_price: float,
                                   option_type: str, dte: int = 0) -> Tuple[List[dict], float]:
        """Evaluate strikes against historical price averages.

        Adds price comparison data to each strike and returns confidence boost
        if any strike shows elevated pricing (>34% above 6-week average).

        Args:
            strikes: List of strike recommendation dicts
            stock_price: Current stock price
            option_type: 'CALL' or 'PUT'
            dte: Days to expiration (0 or 1)

        Returns:
            Tuple of (enhanced_strikes, max_confidence_boost)
            - enhanced_strikes: strikes with 'price_comparison' data added
            - max_confidence_boost: highest boost found (0.0 or 0.3)
        """
        try:
            from ..data.options_history import get_price_checker
            checker = get_price_checker()
            return checker.evaluate_strikes(strikes, stock_price, option_type, dte)
        except Exception as e:
            logger.warning(f"Price comparison unavailable: {e}")
            return strikes, 0.0
