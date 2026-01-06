"""Live intraday news signal detector.

Reacts to breaking news in real-time during market hours.
Designed to be called frequently (every 2 minutes) to catch
time-sensitive catalysts.
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Set

from .base import BaseSignal, Signal, SignalDirection, SignalStrength, MAX_OPTION_PRICE
from ..data.news_monitor import FinnhubNewsMonitor, NewsArticle
from ..data.schwab_client import get_client

logger = logging.getLogger(__name__)

# Bullish keywords for sentiment analysis
BULLISH_KEYWORDS = [
    "beats", "surge", "upgrade", "partnership", "acquisition",
    "record", "growth", "raises guidance", "buy rating", "outperform",
    "s&p inclusion", "s&p 500", "index addition", "strong results",
    "exceeds expectations", "bullish", "rally", "soar"
]

# Bearish keywords for sentiment analysis
BEARISH_KEYWORDS = [
    "misses", "plunge", "downgrade", "lawsuit", "investigation",
    "lowers guidance", "sell rating", "underperform", "layoffs",
    "ceo departure", "cfo leaves", "index removal", "disappoints",
    "weak results", "bearish", "tumble", "crash"
]

# Major news sources (add confidence boost)
MAJOR_SOURCES = [
    "reuters", "bloomberg", "cnbc", "wall street journal", "wsj",
    "financial times", "ft", "marketwatch", "barron's", "seeking alpha"
]


class LiveNewsSignal(BaseSignal):
    """Detects trading signals from breaking news in real-time."""

    def __init__(self):
        super().__init__("Live Intraday News")
        self.news_monitor = FinnhubNewsMonitor()
        self.market_client = get_client()
        self.lookback_minutes = 15  # Only check news from last 15 minutes
        self._alerted_headlines: Set[str] = set()  # Prevent duplicate alerts

    def get_description(self) -> str:
        return (
            "Monitors breaking news in real-time and triggers alerts "
            "when high-impact news is detected within the last 15 minutes."
        )

    def check(self) -> Optional[Signal]:
        """Check for breaking news catalyst.

        Returns:
            Signal if breaking news detected, None otherwise.
        """
        if not self.enabled:
            return None

        if not self.is_valid_trading_day():
            logger.debug("Not a valid trading day (Thursday/Friday)")
            return None

        if not self.is_valid_entry_window():
            logger.debug("Outside valid entry window")
            return None

        try:
            # Get recent APP news
            news = self.news_monitor.get_company_news("APP", days=1)

            if not news:
                logger.debug("No recent APP news found")
                return None

            # Filter to news from last 15 minutes only
            cutoff = datetime.now() - timedelta(minutes=self.lookback_minutes)
            recent_news = [a for a in news if a.published >= cutoff]

            if not recent_news:
                logger.debug(f"No news in last {self.lookback_minutes} minutes")
                return None

            # Analyze each article for high impact
            for article in recent_news:
                # Skip if we already alerted on this headline
                if article.title in self._alerted_headlines:
                    continue

                result = self._analyze_article(article)

                if result is not None:
                    direction, confidence, matched_keywords = result

                    # Only trigger if confidence >= 0.5 (actionable threshold)
                    if confidence < 0.5:
                        continue

                    # Mark as alerted
                    self._alerted_headlines.add(article.title)

                    # Get current price
                    quote = self.market_client.get_quote("APP")
                    current_price = quote.get("price", 0)

                    if not current_price:
                        logger.warning("Could not get APP price")
                        return None

                    # Determine strength
                    if confidence >= 0.8:
                        strength = SignalStrength.STRONG
                    elif confidence >= 0.6:
                        strength = SignalStrength.MODERATE
                    else:
                        strength = SignalStrength.WEAK

                    # Calculate strikes (filtered by $1.00 max)
                    strikes = self.calculate_strike_recommendations(current_price, direction)
                    strikes = self.filter_strikes_by_price(strikes, MAX_OPTION_PRICE)

                    # Apply price comparison check
                    dte = 0 if self.is_friday() else 1
                    option_type = 'CALL' if direction == SignalDirection.CALL else 'PUT'
                    enhanced_strikes, price_boost = self.evaluate_price_comparison(
                        strikes, current_price, option_type, dte
                    )

                    # Apply confidence boost from price comparison
                    final_confidence = min(confidence + price_boost, 1.0)

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
                            "catalyst_type": "live_news",
                            "headline": article.title,
                            "source": article.source,
                            "published": article.published.isoformat(),
                            "news_url": article.url,
                            "matched_keywords": matched_keywords,
                            "current_price": current_price,
                            "minutes_ago": int((datetime.now() - article.published).total_seconds() / 60),
                            "price_comparison_boost": price_boost,
                        },
                        recommended_strikes=enhanced_strikes
                    )

                    logger.info(f"Live news signal detected: {signal}")
                    return signal

            return None

        except Exception as e:
            logger.error(f"Error checking live news signal: {e}")
            return None

    def _analyze_article(self, article: NewsArticle) -> Optional[tuple]:
        """Analyze an article for sentiment and impact.

        Args:
            article: NewsArticle to analyze

        Returns:
            Tuple of (direction, confidence, matched_keywords) or None
        """
        title_lower = article.title.lower()
        summary_lower = article.summary.lower() if article.summary else ""
        text = f"{title_lower} {summary_lower}"
        source_lower = article.source.lower()

        # Count keyword matches
        bullish_matches = [kw for kw in BULLISH_KEYWORDS if kw in text]
        bearish_matches = [kw for kw in BEARISH_KEYWORDS if kw in text]

        bullish_count = len(bullish_matches)
        bearish_count = len(bearish_matches)

        # Determine direction
        if bullish_count > bearish_count:
            direction = SignalDirection.CALL
            matched_keywords = bullish_matches
            match_count = bullish_count
        elif bearish_count > bullish_count:
            direction = SignalDirection.PUT
            matched_keywords = bearish_matches
            match_count = bearish_count
        else:
            # Neutral - no signal
            return None

        # Calculate confidence
        # Base: 0.4
        # +0.15 per keyword match
        # +0.1 if from major source
        confidence = 0.4
        confidence += match_count * 0.15

        # Check if from major source
        is_major_source = any(src in source_lower for src in MAJOR_SOURCES)
        if is_major_source:
            confidence += 0.1

        confidence = min(confidence, 1.0)

        return (direction, confidence, matched_keywords)

    def clear_alert_history(self):
        """Clear the alert history to allow re-alerting on same headlines."""
        self._alerted_headlines.clear()
        logger.info("Live news alert history cleared")
