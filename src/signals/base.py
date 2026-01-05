"""Base signal class for APP options trading signals."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, List


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
