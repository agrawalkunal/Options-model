"""News monitoring module for ad sector and APP-specific news.

Monitors multiple news sources for catalysts that could move APP:
1. Ad industry news (META, GOOGL, digital advertising)
2. Direct APP company news
3. Market-wide news affecting tech/growth stocks
"""

import os
import logging
from datetime import datetime, timedelta
from typing import List, Optional
import requests

logger = logging.getLogger(__name__)

# Ad sector keywords to monitor
AD_SECTOR_KEYWORDS = [
    "digital advertising",
    "ad spend",
    "ad revenue",
    "programmatic",
    "mobile advertising",
    "app monetization",
    "connected TV",
    "CTV advertising",
    "ROAS",
    "advertising budget",
    "ad market",
]

# Ad sector tickers to monitor
AD_SECTOR_TICKERS = ["META", "GOOGL", "TTD", "MGNI", "PUBM", "DV", "APP"]

# Sentiment keywords
BULLISH_KEYWORDS = [
    "beats", "surge", "soar", "jump", "rally", "upgrade",
    "growth", "record", "strong", "exceeds", "outperform"
]

BEARISH_KEYWORDS = [
    "miss", "plunge", "drop", "fall", "downgrade", "weak",
    "decline", "cut", "lower", "disappoints", "underperform"
]


class NewsArticle:
    """Represents a news article with sentiment analysis."""

    def __init__(self, title: str, source: str, url: str, published: datetime,
                 summary: str = "", tickers: List[str] = None):
        self.title = title
        self.source = source
        self.url = url
        self.published = published
        self.summary = summary
        self.tickers = tickers or []
        self.sentiment = self._analyze_sentiment()
        self.relevance_score = self._calculate_relevance()

    def _analyze_sentiment(self) -> str:
        """Simple keyword-based sentiment analysis."""
        text = f"{self.title} {self.summary}".lower()

        bullish_count = sum(1 for kw in BULLISH_KEYWORDS if kw in text)
        bearish_count = sum(1 for kw in BEARISH_KEYWORDS if kw in text)

        if bullish_count > bearish_count:
            return "bullish"
        elif bearish_count > bullish_count:
            return "bearish"
        return "neutral"

    def _calculate_relevance(self) -> float:
        """Calculate relevance score for APP trading."""
        score = 0.0
        text = f"{self.title} {self.summary}".lower()

        # Direct APP mention
        if "applovin" in text or "APP" in self.tickers:
            score += 1.0

        # Ad sector keywords
        for keyword in AD_SECTOR_KEYWORDS:
            if keyword.lower() in text:
                score += 0.2

        # Ad sector tickers
        for ticker in self.tickers:
            if ticker in AD_SECTOR_TICKERS:
                score += 0.3

        return min(score, 1.0)

    def is_relevant(self, threshold: float = 0.3) -> bool:
        """Check if article meets relevance threshold."""
        return self.relevance_score >= threshold

    def __repr__(self):
        return f"NewsArticle('{self.title[:50]}...', sentiment={self.sentiment}, relevance={self.relevance_score:.2f})"


class FinnhubNewsMonitor:
    """News monitor using Finnhub API."""

    BASE_URL = "https://finnhub.io/api/v1"

    def __init__(self):
        self.api_key = os.getenv("FINNHUB_API_KEY")
        if not self.api_key:
            logger.warning("Finnhub API key not found. News monitoring disabled.")

    def get_company_news(self, symbol: str, days: int = 1) -> List[NewsArticle]:
        """Get company-specific news."""
        if not self.api_key:
            return []

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        try:
            response = requests.get(
                f"{self.BASE_URL}/company-news",
                params={
                    "symbol": symbol,
                    "from": start_date.strftime("%Y-%m-%d"),
                    "to": end_date.strftime("%Y-%m-%d"),
                    "token": self.api_key
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            articles = []
            for item in data:
                article = NewsArticle(
                    title=item.get("headline", ""),
                    source=item.get("source", ""),
                    url=item.get("url", ""),
                    published=datetime.fromtimestamp(item.get("datetime", 0)),
                    summary=item.get("summary", ""),
                    tickers=[symbol]
                )
                articles.append(article)

            return articles

        except Exception as e:
            logger.error(f"Error fetching Finnhub news for {symbol}: {e}")
            return []

    def get_market_news(self, category: str = "general") -> List[NewsArticle]:
        """Get general market news."""
        if not self.api_key:
            return []

        try:
            response = requests.get(
                f"{self.BASE_URL}/news",
                params={
                    "category": category,
                    "token": self.api_key
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            articles = []
            for item in data[:20]:  # Limit to 20 articles
                article = NewsArticle(
                    title=item.get("headline", ""),
                    source=item.get("source", ""),
                    url=item.get("url", ""),
                    published=datetime.fromtimestamp(item.get("datetime", 0)),
                    summary=item.get("summary", ""),
                    tickers=[]
                )
                articles.append(article)

            return articles

        except Exception as e:
            logger.error(f"Error fetching Finnhub market news: {e}")
            return []


class NewsAPIMonitor:
    """News monitor using NewsAPI.org."""

    BASE_URL = "https://newsapi.org/v2"

    def __init__(self):
        self.api_key = os.getenv("NEWSAPI_KEY")
        if not self.api_key:
            logger.warning("NewsAPI key not found. Backup news monitoring disabled.")

    def search_news(self, query: str, days: int = 1) -> List[NewsArticle]:
        """Search for news articles matching a query."""
        if not self.api_key:
            return []

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        try:
            response = requests.get(
                f"{self.BASE_URL}/everything",
                params={
                    "q": query,
                    "from": start_date.strftime("%Y-%m-%d"),
                    "to": end_date.strftime("%Y-%m-%d"),
                    "language": "en",
                    "sortBy": "publishedAt",
                    "apiKey": self.api_key
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            articles = []
            for item in data.get("articles", [])[:20]:
                published = datetime.fromisoformat(
                    item.get("publishedAt", "").replace("Z", "+00:00")
                ) if item.get("publishedAt") else datetime.now()

                article = NewsArticle(
                    title=item.get("title", ""),
                    source=item.get("source", {}).get("name", ""),
                    url=item.get("url", ""),
                    published=published,
                    summary=item.get("description", ""),
                    tickers=[]
                )
                articles.append(article)

            return articles

        except Exception as e:
            logger.error(f"Error fetching NewsAPI results for '{query}': {e}")
            return []


class NewsAggregator:
    """Aggregates news from multiple sources and filters for relevance."""

    def __init__(self):
        self.finnhub = FinnhubNewsMonitor()
        self.newsapi = NewsAPIMonitor()
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes

    def get_ad_sector_news(self) -> List[NewsArticle]:
        """Get all relevant ad sector news."""
        all_articles = []

        # Get APP-specific news
        app_news = self.finnhub.get_company_news("APP")
        all_articles.extend(app_news)

        # Get META news (highest correlation)
        meta_news = self.finnhub.get_company_news("META")
        all_articles.extend(meta_news)

        # Get GOOGL news
        googl_news = self.finnhub.get_company_news("GOOGL")
        all_articles.extend(googl_news)

        # Search for ad industry news
        ad_news = self.newsapi.search_news("digital advertising")
        all_articles.extend(ad_news)

        # Filter and deduplicate
        seen_titles = set()
        unique_articles = []
        for article in all_articles:
            if article.title not in seen_titles and article.is_relevant():
                seen_titles.add(article.title)
                unique_articles.append(article)

        # Sort by relevance and recency
        unique_articles.sort(key=lambda x: (x.relevance_score, x.published), reverse=True)

        return unique_articles

    def get_breaking_news(self, since_minutes: int = 30) -> List[NewsArticle]:
        """Get breaking news from the last N minutes."""
        cutoff = datetime.now() - timedelta(minutes=since_minutes)
        all_news = self.get_ad_sector_news()

        breaking = [a for a in all_news if a.published >= cutoff]
        return breaking

    def check_for_catalyst(self) -> Optional[dict]:
        """Check if there's a potential catalyst in recent news.

        Returns:
            dict with catalyst info if found, None otherwise
        """
        breaking = self.get_breaking_news(since_minutes=60)

        if not breaking:
            return None

        # Look for high-relevance articles with strong sentiment
        for article in breaking:
            if article.relevance_score >= 0.5 and article.sentiment != "neutral":
                return {
                    "type": "news",
                    "title": article.title,
                    "source": article.source,
                    "sentiment": article.sentiment,
                    "relevance": article.relevance_score,
                    "url": article.url,
                    "published": article.published.isoformat(),
                    "direction": "CALL" if article.sentiment == "bullish" else "PUT"
                }

        return None


# Singleton instance
_aggregator = None


def get_news_aggregator() -> NewsAggregator:
    """Get the singleton NewsAggregator instance."""
    global _aggregator
    if _aggregator is None:
        _aggregator = NewsAggregator()
    return _aggregator
