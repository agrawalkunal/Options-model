"""Historical options price tracking and comparison module.

Provides infrastructure for:
1. Storing option price snapshots at 5-minute intervals
2. Calculating 6-week rolling averages by strike distance
3. Comparing current prices to historical averages for signal boosting
"""

import os
import sqlite3
import logging
from datetime import datetime, timedelta, time
from typing import Optional, List, Dict, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'options_history.db')
HISTORY_WEEKS = 6
PRICE_ELEVATION_THRESHOLD = 0.34  # 34% above average
PRICE_ELEVATION_BOOST = 0.3


class OptionsHistoryDB:
    """SQLite database manager for historical options data."""

    def __init__(self, db_path: str = None):
        """Initialize database connection and create tables if needed.

        Args:
            db_path: Path to SQLite database file. Defaults to data/options_history.db
        """
        self.db_path = db_path or DEFAULT_DB_PATH

        # Ensure directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        """Get a database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        """Create tables and indexes if they don't exist."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Option snapshots table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS option_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp DATETIME NOT NULL,
                    symbol VARCHAR(10) NOT NULL DEFAULT 'APP',
                    stock_price REAL NOT NULL,
                    expiration_date DATE NOT NULL,
                    dte INTEGER NOT NULL,
                    option_type VARCHAR(4) NOT NULL,
                    strike REAL NOT NULL,
                    strike_distance REAL NOT NULL,
                    mid_price REAL,
                    last_price REAL,
                    bid REAL,
                    ask REAL,
                    volume INTEGER,
                    open_interest INTEGER,
                    UNIQUE (timestamp, symbol, expiration_date, strike, option_type)
                )
            """)

            # Create indexes for fast lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
                ON option_snapshots(symbol, option_type, strike_distance, dte)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_timestamp
                ON option_snapshots(timestamp)
            """)

            # Weekly averages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weekly_averages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    calculated_at DATETIME NOT NULL,
                    symbol VARCHAR(10) NOT NULL DEFAULT 'APP',
                    option_type VARCHAR(4) NOT NULL,
                    strike_distance REAL NOT NULL,
                    dte INTEGER NOT NULL,
                    avg_mid_price REAL NOT NULL,
                    sample_count INTEGER NOT NULL,
                    min_price REAL,
                    max_price REAL,
                    UNIQUE (calculated_at, symbol, option_type, strike_distance, dte)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_averages_lookup
                ON weekly_averages(symbol, option_type, strike_distance, dte)
            """)

            # Data collection log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS data_collection_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    collection_date DATE NOT NULL,
                    day_of_week VARCHAR(10) NOT NULL,
                    start_time DATETIME NOT NULL,
                    end_time DATETIME,
                    snapshots_collected INTEGER DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'in_progress',
                    error_message TEXT,
                    UNIQUE (collection_date)
                )
            """)

            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()

    def store_snapshot(self, snapshot: dict) -> bool:
        """Store a single option price snapshot.

        Args:
            snapshot: Dict with keys: timestamp, symbol, stock_price, expiration_date,
                     dte, option_type, strike, strike_distance, mid_price, last_price,
                     bid, ask, volume, open_interest

        Returns:
            True if stored successfully, False otherwise
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO option_snapshots
                (timestamp, symbol, stock_price, expiration_date, dte, option_type,
                 strike, strike_distance, mid_price, last_price, bid, ask, volume, open_interest)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot.get('timestamp'),
                snapshot.get('symbol', 'APP'),
                snapshot.get('stock_price'),
                snapshot.get('expiration_date'),
                snapshot.get('dte'),
                snapshot.get('option_type'),
                snapshot.get('strike'),
                snapshot.get('strike_distance'),
                snapshot.get('mid_price'),
                snapshot.get('last_price'),
                snapshot.get('bid'),
                snapshot.get('ask'),
                snapshot.get('volume'),
                snapshot.get('open_interest')
            ))
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error storing snapshot: {e}")
            return False
        finally:
            conn.close()

    def store_snapshots_batch(self, snapshots: List[dict]) -> int:
        """Store multiple snapshots in a single transaction.

        Args:
            snapshots: List of snapshot dictionaries

        Returns:
            Count of successfully stored snapshots
        """
        if not snapshots:
            return 0

        conn = self._get_connection()
        count = 0
        try:
            cursor = conn.cursor()
            for snapshot in snapshots:
                try:
                    cursor.execute("""
                        INSERT OR REPLACE INTO option_snapshots
                        (timestamp, symbol, stock_price, expiration_date, dte, option_type,
                         strike, strike_distance, mid_price, last_price, bid, ask, volume, open_interest)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        snapshot.get('timestamp'),
                        snapshot.get('symbol', 'APP'),
                        snapshot.get('stock_price'),
                        snapshot.get('expiration_date'),
                        snapshot.get('dte'),
                        snapshot.get('option_type'),
                        snapshot.get('strike'),
                        snapshot.get('strike_distance'),
                        snapshot.get('mid_price'),
                        snapshot.get('last_price'),
                        snapshot.get('bid'),
                        snapshot.get('ask'),
                        snapshot.get('volume'),
                        snapshot.get('open_interest')
                    ))
                    count += 1
                except Exception as e:
                    logger.warning(f"Failed to store snapshot: {e}")
            conn.commit()
            return count
        except Exception as e:
            logger.error(f"Error in batch store: {e}")
            return count
        finally:
            conn.close()

    def get_average_price(self, option_type: str, strike_distance: float,
                          dte: int, symbol: str = 'APP') -> Optional[float]:
        """Get the most recent 6-week average mid price for a strike distance bucket.

        Args:
            option_type: 'CALL' or 'PUT'
            strike_distance: Dollar distance rounded to nearest $0.50
            dte: Days to expiration (0 or 1)
            symbol: Stock symbol

        Returns:
            Average mid price or None if no data available
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # First try to get from pre-calculated averages
            cursor.execute("""
                SELECT avg_mid_price FROM weekly_averages
                WHERE symbol = ? AND option_type = ? AND strike_distance = ? AND dte = ?
                ORDER BY calculated_at DESC
                LIMIT 1
            """, (symbol, option_type, strike_distance, dte))

            row = cursor.fetchone()
            if row:
                return row['avg_mid_price']

            # Fallback: calculate from raw snapshots if no pre-calculated average
            six_weeks_ago = datetime.now() - timedelta(weeks=HISTORY_WEEKS)
            cursor.execute("""
                SELECT AVG(mid_price) as avg_price
                FROM option_snapshots
                WHERE symbol = ? AND option_type = ? AND strike_distance = ? AND dte = ?
                  AND timestamp >= ?
                  AND mid_price IS NOT NULL AND mid_price > 0
            """, (symbol, option_type, strike_distance, dte, six_weeks_ago))

            row = cursor.fetchone()
            if row and row['avg_price']:
                return row['avg_price']

            return None

        except Exception as e:
            logger.error(f"Error getting average price: {e}")
            return None
        finally:
            conn.close()

    def calculate_and_store_averages(self, symbol: str = 'APP') -> bool:
        """Calculate 6-week averages for all strike distance buckets.

        Called at end of each trading day (Thursday/Friday).

        Args:
            symbol: Stock symbol

        Returns:
            True if successful, False otherwise
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            six_weeks_ago = datetime.now() - timedelta(weeks=HISTORY_WEEKS)
            calculated_at = datetime.now()

            # Calculate averages grouped by option_type, strike_distance, dte
            cursor.execute("""
                SELECT
                    option_type,
                    strike_distance,
                    dte,
                    AVG(mid_price) as avg_mid_price,
                    COUNT(*) as sample_count,
                    MIN(mid_price) as min_price,
                    MAX(mid_price) as max_price
                FROM option_snapshots
                WHERE symbol = ?
                  AND timestamp >= ?
                  AND mid_price IS NOT NULL
                  AND mid_price > 0
                GROUP BY option_type, strike_distance, dte
            """, (symbol, six_weeks_ago))

            rows = cursor.fetchall()

            for row in rows:
                cursor.execute("""
                    INSERT OR REPLACE INTO weekly_averages
                    (calculated_at, symbol, option_type, strike_distance, dte,
                     avg_mid_price, sample_count, min_price, max_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    calculated_at,
                    symbol,
                    row['option_type'],
                    row['strike_distance'],
                    row['dte'],
                    row['avg_mid_price'],
                    row['sample_count'],
                    row['min_price'],
                    row['max_price']
                ))

            conn.commit()
            logger.info(f"Calculated and stored {len(rows)} averages")
            return True

        except Exception as e:
            logger.error(f"Error calculating averages: {e}")
            return False
        finally:
            conn.close()

    def cleanup_old_data(self, weeks: int = HISTORY_WEEKS) -> int:
        """Remove data older than specified weeks.

        Args:
            weeks: Number of weeks to retain

        Returns:
            Count of deleted rows
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cutoff = datetime.now() - timedelta(weeks=weeks)

            cursor.execute("""
                DELETE FROM option_snapshots WHERE timestamp < ?
            """, (cutoff,))

            deleted = cursor.rowcount

            # Also clean up old averages (keep last 2 calculations)
            cursor.execute("""
                DELETE FROM weekly_averages
                WHERE calculated_at NOT IN (
                    SELECT DISTINCT calculated_at FROM weekly_averages
                    ORDER BY calculated_at DESC LIMIT 2
                )
            """)

            conn.commit()
            logger.info(f"Cleaned up {deleted} old snapshots")
            return deleted

        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            return 0
        finally:
            conn.close()

    def get_snapshot_count(self, symbol: str = 'APP') -> int:
        """Get total count of snapshots in database."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM option_snapshots WHERE symbol = ?", (symbol,))
            return cursor.fetchone()['count']
        except Exception as e:
            logger.error(f"Error getting snapshot count: {e}")
            return 0
        finally:
            conn.close()


class OptionsDataCollector:
    """Collects option price data at 5-minute intervals during market hours."""

    def __init__(self, db: OptionsHistoryDB = None):
        """Initialize collector with database and market client.

        Args:
            db: OptionsHistoryDB instance. Created if not provided.
        """
        self.db = db or OptionsHistoryDB()

        # Import here to avoid circular imports
        from .schwab_client import get_client
        self.market_client = get_client()

    def calculate_strike_distance(self, strike: float, stock_price: float,
                                   option_type: str) -> float:
        """Calculate strike distance in dollars, rounded to nearest $0.50.

        For CALLS (OTM means strike > stock_price):
            distance = round_to_half(strike - stock_price)
            Result: +2.5, +5.0, +7.5, +10.0, etc. (actual dollar distance)

        For PUTS (OTM means strike < stock_price):
            distance = -round_to_half(stock_price - strike)
            Result: -2.5, -5.0, -7.5, -10.0, etc. (actual dollar distance)

        Args:
            strike: Strike price
            stock_price: Current stock price
            option_type: 'CALL' or 'PUT'

        Returns:
            Float strike distance rounded to nearest 0.50 (positive for calls, negative for puts)
        """
        def round_to_half(value: float) -> float:
            """Round to nearest 0.50"""
            return round(value * 2) / 2

        if option_type == 'CALL':
            distance = round_to_half(strike - stock_price)
            # Ensure at least +0.5 for OTM calls
            return max(0.5, distance)
        else:  # PUT
            distance = round_to_half(stock_price - strike)
            # Return negative for puts, ensure at least -0.5 for OTM puts
            return -max(0.5, distance)

    def collect_snapshot(self, symbol: str = 'APP') -> int:
        """Collect current option prices for 0DTE and 1DTE options.

        Args:
            symbol: Stock symbol to collect

        Returns:
            Count of options stored
        """
        try:
            # Get current stock price
            quote = self.market_client.get_quote(symbol)
            stock_price = quote.get('price', 0)

            if not stock_price:
                logger.warning(f"Could not get stock price for {symbol}")
                return 0

            # Get options chain
            chain = self.market_client.get_options_chain(symbol)

            if chain.get('calls') is None or chain.get('puts') is None:
                logger.warning(f"Could not get options chain for {symbol}")
                return 0

            timestamp = datetime.now()
            snapshots = []

            # Get available expirations
            expirations = chain.get('expirations', [])[:2]  # 0DTE and 1DTE

            for i, expiration in enumerate(expirations):
                dte = i  # 0 for nearest, 1 for next

                # Get chain for this expiration
                try:
                    exp_chain = self.market_client.get_options_chain(symbol, expiration)
                    calls_df = exp_chain.get('calls')
                    puts_df = exp_chain.get('puts')
                except:
                    # Use the default chain if specific expiration fetch fails
                    calls_df = chain.get('calls')
                    puts_df = chain.get('puts')

                # Process calls (10 nearest OTM)
                if calls_df is not None and len(calls_df) > 0:
                    otm_calls = calls_df[calls_df['strike'] > stock_price].head(10)
                    for _, row in otm_calls.iterrows():
                        strike = row['strike']
                        bid = row.get('bid', 0) or 0
                        ask = row.get('ask', 0) or 0
                        mid_price = (bid + ask) / 2 if bid and ask else row.get('lastPrice', 0)

                        snapshots.append({
                            'timestamp': timestamp,
                            'symbol': symbol,
                            'stock_price': stock_price,
                            'expiration_date': expiration,
                            'dte': dte,
                            'option_type': 'CALL',
                            'strike': strike,
                            'strike_distance': self.calculate_strike_distance(strike, stock_price, 'CALL'),
                            'mid_price': mid_price,
                            'last_price': row.get('lastPrice', 0),
                            'bid': bid,
                            'ask': ask,
                            'volume': int(row.get('volume', 0)) if row.get('volume') else 0,
                            'open_interest': int(row.get('openInterest', 0)) if row.get('openInterest') else 0
                        })

                # Process puts (10 nearest OTM)
                if puts_df is not None and len(puts_df) > 0:
                    otm_puts = puts_df[puts_df['strike'] < stock_price].tail(10).iloc[::-1]
                    for _, row in otm_puts.iterrows():
                        strike = row['strike']
                        bid = row.get('bid', 0) or 0
                        ask = row.get('ask', 0) or 0
                        mid_price = (bid + ask) / 2 if bid and ask else row.get('lastPrice', 0)

                        snapshots.append({
                            'timestamp': timestamp,
                            'symbol': symbol,
                            'stock_price': stock_price,
                            'expiration_date': expiration,
                            'dte': dte,
                            'option_type': 'PUT',
                            'strike': strike,
                            'strike_distance': self.calculate_strike_distance(strike, stock_price, 'PUT'),
                            'mid_price': mid_price,
                            'last_price': row.get('lastPrice', 0),
                            'bid': bid,
                            'ask': ask,
                            'volume': int(row.get('volume', 0)) if row.get('volume') else 0,
                            'open_interest': int(row.get('openInterest', 0)) if row.get('openInterest') else 0
                        })

            # Store all snapshots
            stored = self.db.store_snapshots_batch(snapshots)
            logger.debug(f"Collected {stored} option snapshots for {symbol}")
            return stored

        except Exception as e:
            logger.error(f"Error collecting snapshot: {e}")
            return 0

    def is_collection_time(self) -> bool:
        """Check if current time is within collection window.

        Collection: 9:30 AM - 4:00 PM ET on Thursday/Friday

        Returns:
            True if should collect data, False otherwise
        """
        now = datetime.now()
        weekday = now.weekday()
        current_time = now.time()

        # Thursday = 3, Friday = 4
        if weekday not in [3, 4]:
            return False

        market_open = time(9, 30)
        market_close = time(16, 0)

        return market_open <= current_time <= market_close

    def is_eod_calculation_time(self) -> bool:
        """Check if it's time for end-of-day average calculation.

        Returns:
            True if within 5 minutes after 4:00 PM on Thu/Fri
        """
        now = datetime.now()
        weekday = now.weekday()
        current_time = now.time()

        if weekday not in [3, 4]:
            return False

        eod_start = time(16, 0)
        eod_end = time(16, 5)

        return eod_start <= current_time <= eod_end


class PriceComparisonChecker:
    """Checks if current option prices exceed historical averages."""

    THRESHOLD_PERCENTAGE = PRICE_ELEVATION_THRESHOLD  # 34%
    CONFIDENCE_BOOST = PRICE_ELEVATION_BOOST  # 0.3

    def __init__(self, db: OptionsHistoryDB = None):
        """Initialize checker with database.

        Args:
            db: OptionsHistoryDB instance. Created if not provided.
        """
        self.db = db or OptionsHistoryDB()

    def check_price_elevation(self, current_price: float, option_type: str,
                               strike_distance: float, dte: int,
                               symbol: str = 'APP') -> dict:
        """Check if current price exceeds 6-week average by threshold.

        Args:
            current_price: Current option mid price
            option_type: 'CALL' or 'PUT'
            strike_distance: Dollar distance bucket
            dte: Days to expiration
            symbol: Stock symbol

        Returns:
            Dict with:
                - is_elevated: bool
                - current_price: float
                - avg_price: float or None
                - elevation_pct: float or None
                - confidence_boost: float (0.0 or 0.3)
                - has_historical_data: bool
        """
        try:
            avg_price = self.db.get_average_price(option_type, strike_distance, dte, symbol)

            if avg_price is None or avg_price <= 0:
                return {
                    'is_elevated': False,
                    'current_price': current_price,
                    'avg_price': None,
                    'elevation_pct': None,
                    'confidence_boost': 0.0,
                    'has_historical_data': False
                }

            if current_price <= 0:
                return {
                    'is_elevated': False,
                    'current_price': current_price,
                    'avg_price': avg_price,
                    'elevation_pct': None,
                    'confidence_boost': 0.0,
                    'has_historical_data': True
                }

            elevation_pct = (current_price - avg_price) / avg_price
            is_elevated = elevation_pct >= self.THRESHOLD_PERCENTAGE

            return {
                'is_elevated': is_elevated,
                'current_price': current_price,
                'avg_price': avg_price,
                'elevation_pct': elevation_pct,
                'confidence_boost': self.CONFIDENCE_BOOST if is_elevated else 0.0,
                'has_historical_data': True
            }

        except Exception as e:
            logger.warning(f"Price elevation check failed: {e}")
            return {
                'is_elevated': False,
                'current_price': current_price,
                'avg_price': None,
                'elevation_pct': None,
                'confidence_boost': 0.0,
                'has_historical_data': False
            }

    def evaluate_strikes(self, strikes: List[dict], stock_price: float,
                         option_type: str, dte: int,
                         symbol: str = 'APP') -> Tuple[List[dict], float]:
        """Evaluate a list of strike recommendations for price elevation.

        Adds price comparison data to each strike dict.

        Args:
            strikes: List of strike dicts from signal
            stock_price: Current stock price
            option_type: 'CALL' or 'PUT'
            dte: Days to expiration
            symbol: Stock symbol

        Returns:
            Tuple of (enhanced_strikes, max_confidence_boost)
            - enhanced_strikes: strikes with 'price_comparison' data
            - max_confidence_boost: highest boost found (0.0 or 0.3)
        """
        if not strikes:
            return strikes, 0.0

        max_boost = 0.0
        enhanced = []

        for strike_data in strikes:
            strike_price = strike_data.get('strike', 0)
            option_price = strike_data.get('last_price') or strike_data.get('ask') or 0

            # Calculate strike distance (rounded to nearest $0.50)
            def round_to_half(value: float) -> float:
                return round(value * 2) / 2

            if option_type == 'CALL':
                distance = round_to_half(strike_price - stock_price)
                distance = max(0.5, distance)  # At least +0.5 for OTM calls
            else:
                distance = round_to_half(stock_price - strike_price)
                distance = -max(0.5, distance)  # Negative, at least -0.5 for OTM puts

            # Check price elevation
            comparison = self.check_price_elevation(
                current_price=option_price,
                option_type=option_type,
                strike_distance=distance,
                dte=dte,
                symbol=symbol
            )

            # Add comparison data to strike
            enhanced_strike = strike_data.copy()
            enhanced_strike['price_comparison'] = comparison
            enhanced.append(enhanced_strike)

            # Track max boost
            if comparison.get('confidence_boost', 0) > max_boost:
                max_boost = comparison['confidence_boost']

        return enhanced, max_boost


# Singleton instances
_db_instance: Optional[OptionsHistoryDB] = None
_collector_instance: Optional[OptionsDataCollector] = None
_checker_instance: Optional[PriceComparisonChecker] = None


def get_options_db() -> OptionsHistoryDB:
    """Get singleton database instance."""
    global _db_instance
    if _db_instance is None:
        _db_instance = OptionsHistoryDB()
    return _db_instance


def get_collector() -> OptionsDataCollector:
    """Get singleton collector instance."""
    global _collector_instance
    if _collector_instance is None:
        _collector_instance = OptionsDataCollector(get_options_db())
    return _collector_instance


def get_price_checker() -> PriceComparisonChecker:
    """Get singleton price checker instance."""
    global _checker_instance
    if _checker_instance is None:
        _checker_instance = PriceComparisonChecker(get_options_db())
    return _checker_instance
