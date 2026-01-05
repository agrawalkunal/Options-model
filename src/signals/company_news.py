"""Direct company news signal detector.

Monitors APP-specific news for major announcements that could
cause significant price moves.
"""

import logging
from datetime import datetime
from typing import Optional

from .base import BaseSignal, Signal, SignalDirection, SignalStrength
from ..data.news_monitor import FinnhubNewsMonitor, NewsArticle
from ..data.schwab_client import get_client

logger = logging.getLogger(__name__)

# High-impact news keywords
MAJOR_POSITIVE_KEYWORDS = [
    "s&p 500", "s&p500", "index inclusion", "acquisition", "acquires",
    "partnership", "contract", "beats estimates", "raises guidance",
    "record revenue", "upgrade", "buy rating", "outperform"
]

MAJOR_NEGATIVE_KEYWORDS = [
    "index removal", "lawsuit", "sec investigation", "downgrade",
    "misses estimates", "lowers guidance", "sell rating", "underperform",
    "executive departure", "cfo resignation", "ceo leaves"
]


class CompanyNewsSignal(BaseSignal):
    """Detects trading signals based on direct APP company news."""

    def __init__(self):
        super().__init__("Company News")
        self.news_monitor = FinnhubNewsMonitor()
        self.market_client = get_client()
        self.lookback_minutes = 120  # Check last 2 hours

    def get_description(self) -> str:
        return (
            "Monitors APP-specific news for major announcements including "
            "S&P index changes, partnerships, earnings, and analyst ratings."
        )

    def check(self) -> Optional[Signal]:
        """Check for APP-specific news catalyst.

        Returns:
            Signal if major news detected, None otherwise.
        """
        if not self.enabled:
            return None

        if not self.is_valid_trading_day():
            logger.debug("Not a valid trading day (Thursday/Friday)")
            return None

        try:
            # Get recent APP news
            news = self.news_monitor.get_company_news("APP", days=1)

            if not news:
                logger.debug("No recent APP news found")
                return None

            # Find major news
            major_news = self._find_major_news(news)

            if not major_news:
                logger.debug("No major APP news catalyst detected")
                return None

            article, direction, impact_score = major_news

            # Get current price
            quote = self.market_client.get_quote("APP")
            current_price = quote.get("price", 0)

            if not current_price:
                logger.warning("Could not get APP price")
                return None

            # Calculate confidence
            confidence = min(impact_score, 1.0)

            # Determine strength
            if confidence >= 0.8:
                strength = SignalStrength.STRONG
            elif confidence >= 0.6:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            # Calculate strikes
            strikes = self.calculate_strike_recommendations(current_price, direction)

            signal = Signal(
                name=self.name,
                direction=direction,
                strength=strength,
                confidence=confidence,
                timestamp=datetime.now(),
                details={
                    "catalyst_type": "company_news",
                    "headline": article.title,
                    "source": article.source,
                    "published": article.published.isoformat(),
                    "impact_score": impact_score,
                    "news_url": article.url,
                    "current_price": current_price,
                },
                recommended_strikes=strikes
            )

            logger.info(f"Company news signal detected: {signal}")
            return signal

        except Exception as e:
            logger.error(f"Error checking company news signal: {e}")
            return None

    def _find_major_news(self, articles: list) -> Optional[tuple]:
        """Find major news that could move the stock.

        Args:
            articles: List of NewsArticle objects

        Returns:
            Tuple of (article, direction, impact_score) or None
        """
        for article in articles:
            title_lower = article.title.lower()
            summary_lower = article.summary.lower()
            text = f"{title_lower} {summary_lower}"

            # Check for major positive news
            positive_matches = sum(1 for kw in MAJOR_POSITIVE_KEYWORDS if kw in text)
            if positive_matches > 0:
                impact_score = min(0.5 + (positive_matches * 0.15), 1.0)
                return (article, SignalDirection.CALL, impact_score)

            # Check for major negative news
            negative_matches = sum(1 for kw in MAJOR_NEGATIVE_KEYWORDS if kw in text)
            if negative_matches > 0:
                impact_score = min(0.5 + (negative_matches * 0.15), 1.0)
                return (article, SignalDirection.PUT, impact_score)

        return None
