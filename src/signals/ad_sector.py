"""Ad sector news signal detector.

Monitors news from META, GOOGL, and digital advertising industry
to detect catalysts that could move APP stock.
"""

import logging
from datetime import datetime
from typing import Optional

from .base import BaseSignal, Signal, SignalDirection, SignalStrength
from ..data.news_monitor import get_news_aggregator
from ..data.schwab_client import get_client

logger = logging.getLogger(__name__)


class AdSectorSignal(BaseSignal):
    """Detects trading signals based on ad sector news."""

    def __init__(self):
        super().__init__("Ad Sector News")
        self.news_aggregator = get_news_aggregator()
        self.market_client = get_client()

        # Thresholds
        self.min_relevance = 0.4
        self.lookback_minutes = 60

    def get_description(self) -> str:
        return (
            "Monitors news from META, GOOGL, and digital advertising industry. "
            "Triggers when high-relevance news with strong sentiment is detected."
        )

    def check(self) -> Optional[Signal]:
        """Check for ad sector news catalyst.

        Returns:
            Signal if catalyst detected, None otherwise.
        """
        if not self.enabled:
            return None

        # Only check on valid trading days
        if not self.is_valid_trading_day():
            logger.debug("Not a valid trading day (Thursday/Friday)")
            return None

        try:
            # Check for catalyst in news
            catalyst = self.news_aggregator.check_for_catalyst()

            if not catalyst:
                logger.debug("No ad sector catalyst detected")
                return None

            # Get current APP price for strike recommendations
            quote = self.market_client.get_quote("APP")
            current_price = quote.get("price", 0)

            if not current_price:
                logger.warning("Could not get APP price for strike recommendations")
                return None

            # Determine direction and strength
            direction = (SignalDirection.CALL if catalyst["direction"] == "CALL"
                        else SignalDirection.PUT)

            # Calculate confidence based on relevance and sentiment strength
            confidence = min(catalyst["relevance"] * 1.2, 1.0)

            # Determine strength
            if confidence >= 0.7:
                strength = SignalStrength.STRONG
            elif confidence >= 0.5:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            # Calculate recommended strikes
            strikes = self.calculate_strike_recommendations(current_price, direction)

            # Apply price comparison check
            dte = 0 if self.is_friday() else 1
            option_type = 'CALL' if direction == SignalDirection.CALL else 'PUT'
            enhanced_strikes, price_boost = self.evaluate_price_comparison(
                strikes, current_price, option_type, dte
            )

            # Apply confidence boost from price comparison
            final_confidence = min(confidence + price_boost, 1.0)

            # Re-evaluate strength with updated confidence
            if final_confidence >= 0.7:
                strength = SignalStrength.STRONG
            elif final_confidence >= 0.5:
                strength = SignalStrength.MODERATE

            signal = Signal(
                name=self.name,
                direction=direction,
                strength=strength,
                confidence=final_confidence,
                timestamp=datetime.now(),
                details={
                    "catalyst_type": "ad_sector_news",
                    "headline": catalyst["title"],
                    "source": catalyst["source"],
                    "sentiment": catalyst["sentiment"],
                    "relevance_score": catalyst["relevance"],
                    "news_url": catalyst["url"],
                    "current_price": current_price,
                    "price_comparison_boost": price_boost,
                },
                recommended_strikes=enhanced_strikes
            )

            logger.info(f"Ad sector signal detected: {signal}")
            return signal

        except Exception as e:
            logger.error(f"Error checking ad sector signal: {e}")
            return None

    def get_sector_sentiment(self) -> dict:
        """Get overall ad sector sentiment from recent news.

        Returns:
            dict with sentiment breakdown
        """
        news = self.news_aggregator.get_ad_sector_news()

        if not news:
            return {"bullish": 0, "bearish": 0, "neutral": 0, "overall": "neutral"}

        sentiment_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
        for article in news:
            sentiment_counts[article.sentiment] += 1

        total = len(news)
        bullish_pct = sentiment_counts["bullish"] / total
        bearish_pct = sentiment_counts["bearish"] / total

        if bullish_pct > bearish_pct + 0.2:
            overall = "bullish"
        elif bearish_pct > bullish_pct + 0.2:
            overall = "bearish"
        else:
            overall = "neutral"

        return {
            "bullish": sentiment_counts["bullish"],
            "bearish": sentiment_counts["bearish"],
            "neutral": sentiment_counts["neutral"],
            "overall": overall,
            "article_count": total
        }
