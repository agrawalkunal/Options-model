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

            article, direction, impact_score, breakdown_components = major_news

            # Get current price
            quote = self.market_client.get_quote("APP")
            current_price = quote.get("price", 0)

            if not current_price:
                logger.warning("Could not get APP price")
                return None

            # Calculate confidence
            base_confidence = min(impact_score, 1.0)

            # Determine strength
            if base_confidence >= 0.8:
                strength = SignalStrength.STRONG
            elif base_confidence >= 0.6:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            # Calculate strikes
            strikes = self.calculate_strike_recommendations(current_price, direction)

            # Apply price comparison check
            dte = 0 if self.is_friday() else 1
            option_type = 'CALL' if direction == SignalDirection.CALL else 'PUT'
            enhanced_strikes, price_boost = self.evaluate_price_comparison(
                strikes, current_price, option_type, dte
            )

            # Apply confidence boost from price comparison
            final_confidence = min(base_confidence + price_boost, 1.0)

            # Add price boost to breakdown if applicable
            if price_boost > 0:
                breakdown_components.append({
                    "name": "Price comparison boost",
                    "value": price_boost,
                    "description": "Elevated option pricing detected"
                })

            # Re-evaluate strength with updated confidence
            if final_confidence >= 0.8:
                strength = SignalStrength.STRONG
            elif final_confidence >= 0.6:
                strength = SignalStrength.MODERATE

            signal = Signal(
                name=self.name,
                direction=direction,
                strength=strength,
                confidence=final_confidence,
                timestamp=datetime.now(),
                details={
                    "catalyst_type": "company_news",
                    "headline": article.title,
                    "source": article.source,
                    "published": article.published.isoformat(),
                    "impact_score": impact_score,
                    "news_url": article.url,
                    "current_price": current_price,
                    "price_comparison_boost": price_boost,
                    "confidence_breakdown": {
                        "components": breakdown_components,
                        "base_confidence": base_confidence,
                        "final_confidence": final_confidence
                    },
                },
                recommended_strikes=enhanced_strikes
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
            Tuple of (article, direction, impact_score, breakdown_components) or None
        """
        for article in articles:
            title_lower = article.title.lower()
            summary_lower = article.summary.lower()
            text = f"{title_lower} {summary_lower}"

            # Check for major positive news
            positive_keywords = [kw for kw in MAJOR_POSITIVE_KEYWORDS if kw in text]
            positive_matches = len(positive_keywords)
            if positive_matches > 0:
                impact_score = min(positive_matches * 0.15, 1.0)
                breakdown = [{
                    "name": f"Keyword matches ({positive_matches})",
                    "value": impact_score,
                    "description": ", ".join(positive_keywords[:3]) + ("..." if positive_matches > 3 else "")
                }]
                return (article, SignalDirection.CALL, impact_score, breakdown)

            # Check for major negative news
            negative_keywords = [kw for kw in MAJOR_NEGATIVE_KEYWORDS if kw in text]
            negative_matches = len(negative_keywords)
            if negative_matches > 0:
                impact_score = min(negative_matches * 0.15, 1.0)
                breakdown = [{
                    "name": f"Keyword matches ({negative_matches})",
                    "value": impact_score,
                    "description": ", ".join(negative_keywords[:3]) + ("..." if negative_matches > 3 else "")
                }]
                return (article, SignalDirection.PUT, impact_score, breakdown)

        return None
