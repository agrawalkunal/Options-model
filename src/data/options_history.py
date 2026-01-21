"""Historical options price tracking and comparison module.

Provides infrastructure for:
1. Storing option price snapshots at 5-minute intervals
2. Calculating 6-week rolling averages by strike distance (using ASK prices)
3. Comparing current prices to historical averages for signal boosting
4. Excluding earnings weeks from averages to avoid skewed data
"""

import os
import sqlite3
import logging
from datetime import datetime, timedelta, time, date
from typing import Optional, List, Dict, Tuple
from pathlib import Path

import yfinance as yf

logger = logging.getLogger(__name__)

# Configuration constants
DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'options_history.db')
HISTORY_WEEKS = 10
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

            # Earnings calendar table (for excluding earnings weeks from averages)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS earnings_calendar (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol VARCHAR(10) NOT NULL DEFAULT 'APP',
                    earnings_date DATE NOT NULL,
                    week_start DATE NOT NULL,
                    week_end DATE NOT NULL,
                    source VARCHAR(20),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (symbol, earnings_date)
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_earnings_week
                ON earnings_calendar(symbol, week_start, week_end)
            """)

            # Add avg_ask_price column to weekly_averages if not exists
            try:
                cursor.execute("ALTER TABLE weekly_averages ADD COLUMN avg_ask_price REAL")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Add time-slot columns to option_snapshots for time-specific comparisons
            try:
                cursor.execute("ALTER TABLE option_snapshots ADD COLUMN day_of_week INTEGER")
            except sqlite3.OperationalError:
                pass  # Column already exists

            try:
                cursor.execute("ALTER TABLE option_snapshots ADD COLUMN time_slot VARCHAR(5)")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Add time-slot columns to weekly_averages
            try:
                cursor.execute("ALTER TABLE weekly_averages ADD COLUMN day_of_week INTEGER")
            except sqlite3.OperationalError:
                pass  # Column already exists

            try:
                cursor.execute("ALTER TABLE weekly_averages ADD COLUMN time_slot VARCHAR(5)")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Add ordinal_position column to option_snapshots (1-10, nearest to farthest OTM)
            try:
                cursor.execute("ALTER TABLE option_snapshots ADD COLUMN ordinal_position INTEGER")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Add ordinal_position column to weekly_averages
            try:
                cursor.execute("ALTER TABLE weekly_averages ADD COLUMN ordinal_position INTEGER")
            except sqlite3.OperationalError:
                pass  # Column already exists

            # Create index for ordinal position lookups on snapshots
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_ordinal_lookup
                ON option_snapshots(symbol, day_of_week, time_slot, option_type, ordinal_position, dte)
            """)

            # Create index for ordinal position lookups on averages
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_averages_ordinal_lookup
                ON weekly_averages(symbol, day_of_week, time_slot, option_type, ordinal_position, dte)
            """)

            # Create index for time-slot lookups on snapshots (legacy)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_time_lookup
                ON option_snapshots(symbol, day_of_week, time_slot, option_type, strike_distance, dte)
            """)

            # Create index for time-slot lookups on averages
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_averages_time_lookup
                ON weekly_averages(symbol, day_of_week, time_slot, option_type, strike_distance, dte)
            """)

            conn.commit()
            logger.info(f"Database initialized at {self.db_path}")

        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
        finally:
            conn.close()

        # Migrate existing data to include time metadata
        self.migrate_time_metadata()

        # Migrate existing data to include ordinal positions
        self.migrate_ordinal_positions()

    def store_snapshot(self, snapshot: dict) -> bool:
        """Store a single option price snapshot.

        Args:
            snapshot: Dict with keys: timestamp, symbol, stock_price, expiration_date,
                     dte, option_type, strike, strike_distance, mid_price, last_price,
                     bid, ask, volume, open_interest, day_of_week, time_slot, ordinal_position

        Returns:
            True if stored successfully, False otherwise
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO option_snapshots
                (timestamp, symbol, stock_price, expiration_date, dte, option_type,
                 strike, strike_distance, mid_price, last_price, bid, ask, volume, open_interest,
                 day_of_week, time_slot, ordinal_position)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                snapshot.get('open_interest'),
                snapshot.get('day_of_week'),
                snapshot.get('time_slot'),
                snapshot.get('ordinal_position')
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
                         strike, strike_distance, mid_price, last_price, bid, ask, volume, open_interest,
                         day_of_week, time_slot, ordinal_position)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        snapshot.get('open_interest'),
                        snapshot.get('day_of_week'),
                        snapshot.get('time_slot'),
                        snapshot.get('ordinal_position')
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

    def get_average_price(self, option_type: str, ordinal_position: int,
                          dte: int, day_of_week: int = None, time_slot: str = None,
                          symbol: str = 'APP',
                          earnings_manager: 'EarningsCalendarManager' = None) -> Optional[float]:
        """Get the 6-week average ASK price for a specific time slot and ordinal position.

        Compares prices at the same day of week and time slot for accurate comparison.
        For example: Thursday 9:35 AM current price vs 6-week avg of Thursday 9:35 AM.

        Excludes earnings weeks from the calculation.

        Args:
            option_type: 'CALL' or 'PUT'
            ordinal_position: Position 1-10 (1 = nearest OTM, 10 = farthest OTM)
            dte: Days to expiration (0 or 1)
            day_of_week: Day of week (3=Thursday, 4=Friday). If None, uses current day.
            time_slot: Time slot string "HH:MM" (e.g., "09:35"). If None, uses current time.
            symbol: Stock symbol
            earnings_manager: EarningsCalendarManager for exclusion (created if not provided)

        Returns:
            Average ask price or None if no data available
        """
        # Default to current day/time if not provided
        if day_of_week is None or time_slot is None:
            now = datetime.now()
            if day_of_week is None:
                day_of_week = now.weekday()
            if time_slot is None:
                minute_slot = (now.minute // 5) * 5
                time_slot = f"{now.hour:02d}:{minute_slot:02d}"

        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # First try to get from pre-calculated time-slot specific averages
            cursor.execute("""
                SELECT avg_ask_price, avg_mid_price FROM weekly_averages
                WHERE symbol = ? AND day_of_week = ? AND time_slot = ?
                  AND option_type = ? AND ordinal_position = ? AND dte = ?
                ORDER BY calculated_at DESC
                LIMIT 1
            """, (symbol, day_of_week, time_slot, option_type, ordinal_position, dte))

            row = cursor.fetchone()
            if row:
                # Prefer avg_ask_price, fallback to avg_mid_price for old data
                if row['avg_ask_price']:
                    return row['avg_ask_price']
                elif row['avg_mid_price']:
                    return row['avg_mid_price']

            # Fallback: calculate from raw snapshots if no pre-calculated average
            # Get earnings weeks to exclude
            if earnings_manager is None:
                earnings_manager = EarningsCalendarManager(self)
            earnings_weeks = earnings_manager.get_earnings_weeks(symbol, HISTORY_WEEKS)

            # Build exclusion clause for earnings weeks
            exclusion_clauses = []
            exclusion_params = []
            for week_start, week_end in earnings_weeks:
                exclusion_clauses.append("NOT (DATE(timestamp) BETWEEN ? AND ?)")
                exclusion_params.extend([week_start.isoformat(), week_end.isoformat()])

            earnings_exclusion = " AND ".join(exclusion_clauses) if exclusion_clauses else "1=1"

            six_weeks_ago = datetime.now() - timedelta(weeks=HISTORY_WEEKS)
            # Query for time-slot specific average by ordinal position
            query = f"""
                SELECT AVG(ask) as avg_price
                FROM option_snapshots
                WHERE symbol = ? AND day_of_week = ? AND time_slot = ?
                  AND option_type = ? AND ordinal_position = ? AND dte = ?
                  AND timestamp >= ?
                  AND ask IS NOT NULL AND ask > 0
                  AND {earnings_exclusion}
            """
            cursor.execute(query, (symbol, day_of_week, time_slot, option_type, ordinal_position, dte,
                                   six_weeks_ago, *exclusion_params))

            row = cursor.fetchone()
            if row and row['avg_price']:
                return row['avg_price']

            return None

        except Exception as e:
            logger.error(f"Error getting average price: {e}")
            return None
        finally:
            conn.close()

    def calculate_and_store_averages(self, symbol: str = 'APP',
                                       earnings_manager: 'EarningsCalendarManager' = None) -> bool:
        """Calculate 6-week averages for all strike distance buckets.

        Uses ASK prices and excludes earnings weeks from the calculation.
        Called at end of each trading day (Thursday/Friday).

        Args:
            symbol: Stock symbol
            earnings_manager: EarningsCalendarManager for exclusion (created if not provided)

        Returns:
            True if successful, False otherwise
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            six_weeks_ago = datetime.now() - timedelta(weeks=HISTORY_WEEKS)
            calculated_at = datetime.now()

            # Get earnings weeks to exclude
            if earnings_manager is None:
                earnings_manager = EarningsCalendarManager(self)
            earnings_weeks = earnings_manager.get_earnings_weeks(symbol, HISTORY_WEEKS)

            # Build exclusion clause for earnings weeks
            exclusion_clauses = []
            exclusion_params = []
            for week_start, week_end in earnings_weeks:
                exclusion_clauses.append("NOT (DATE(timestamp) BETWEEN ? AND ?)")
                exclusion_params.extend([week_start.isoformat(), week_end.isoformat()])

            earnings_exclusion = " AND ".join(exclusion_clauses) if exclusion_clauses else "1=1"

            # Log earnings exclusion info
            if earnings_weeks:
                logger.info(f"Excluding {len(earnings_weeks)} earnings week(s) from average calculation")

            # Calculate averages grouped by day_of_week, time_slot, option_type, ordinal_position, dte
            # Using ASK prices, excluding earnings weeks
            # This enables time-slot specific comparisons (e.g., Thursday 9:35 AM vs historical Thursday 9:35 AM)
            query = f"""
                SELECT
                    day_of_week,
                    time_slot,
                    option_type,
                    ordinal_position,
                    dte,
                    AVG(ask) as avg_ask_price,
                    AVG(mid_price) as avg_mid_price,
                    COUNT(*) as sample_count,
                    MIN(ask) as min_price,
                    MAX(ask) as max_price
                FROM option_snapshots
                WHERE symbol = ?
                  AND timestamp >= ?
                  AND ask IS NOT NULL
                  AND ask > 0
                  AND day_of_week IS NOT NULL
                  AND time_slot IS NOT NULL
                  AND ordinal_position IS NOT NULL
                  AND {earnings_exclusion}
                GROUP BY day_of_week, time_slot, option_type, ordinal_position, dte
            """
            cursor.execute(query, (symbol, six_weeks_ago, *exclusion_params))

            rows = cursor.fetchall()

            for row in rows:
                cursor.execute("""
                    INSERT OR REPLACE INTO weekly_averages
                    (calculated_at, symbol, day_of_week, time_slot, option_type, ordinal_position, dte,
                     avg_mid_price, avg_ask_price, sample_count, min_price, max_price, strike_distance)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, (
                    calculated_at,
                    symbol,
                    row['day_of_week'],
                    row['time_slot'],
                    row['option_type'],
                    row['ordinal_position'],
                    row['dte'],
                    row['avg_mid_price'],
                    row['avg_ask_price'],
                    row['sample_count'],
                    row['min_price'],
                    row['max_price']
                ))

            conn.commit()
            logger.info(f"Calculated and stored {len(rows)} time-slot averages (using ASK prices, earnings excluded)")
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

    def migrate_time_metadata(self) -> int:
        """Backfill day_of_week and time_slot for existing snapshots.

        Extracts time metadata from the timestamp column for any rows
        where day_of_week or time_slot is NULL.

        Returns:
            Count of rows updated
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # SQLite doesn't have a direct weekday function, so we use strftime
            # %w returns day of week as 0-6 (Sunday=0), but we need Monday=0
            # So we calculate: (strftime('%w', timestamp) + 6) % 7
            # Thursday = 3, Friday = 4 (which matches Python's weekday())

            # Update rows where time metadata is missing
            cursor.execute("""
                UPDATE option_snapshots
                SET day_of_week = CAST((CAST(strftime('%w', timestamp) AS INTEGER) + 6) % 7 AS INTEGER),
                    time_slot = printf('%02d:%02d',
                                       CAST(strftime('%H', timestamp) AS INTEGER),
                                       (CAST(strftime('%M', timestamp) AS INTEGER) / 5) * 5)
                WHERE day_of_week IS NULL OR time_slot IS NULL
            """)

            updated = cursor.rowcount
            conn.commit()

            if updated > 0:
                logger.info(f"Migrated time metadata for {updated} existing snapshots")
            else:
                logger.debug("No snapshots needed time metadata migration")

            return updated

        except Exception as e:
            logger.error(f"Error migrating time metadata: {e}")
            return 0
        finally:
            conn.close()

    def migrate_ordinal_positions(self) -> int:
        """Backfill ordinal_position for existing snapshots.

        For each timestamp/symbol/option_type group, assigns ordinal positions 1-10
        based on strike order (ascending for CALL, descending for PUT).

        Returns:
            Count of rows updated
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()

            # Check if migration is needed
            cursor.execute("""
                SELECT COUNT(*) as count FROM option_snapshots
                WHERE ordinal_position IS NULL
            """)
            null_count = cursor.fetchone()['count']

            if null_count == 0:
                logger.debug("No snapshots needed ordinal position migration")
                return 0

            logger.info(f"Migrating ordinal positions for {null_count} snapshots...")

            # Get all unique timestamp/symbol/option_type combinations with NULL ordinal_position
            cursor.execute("""
                SELECT DISTINCT timestamp, symbol, option_type
                FROM option_snapshots
                WHERE ordinal_position IS NULL
            """)
            groups = cursor.fetchall()

            updated = 0
            for group in groups:
                ts, symbol, opt_type = group['timestamp'], group['symbol'], group['option_type']

                # Get all snapshots for this group, ordered by strike
                # For CALL: ascending strike (nearest OTM first)
                # For PUT: descending strike (nearest OTM first)
                order = "ASC" if opt_type == "CALL" else "DESC"
                cursor.execute(f"""
                    SELECT id, strike FROM option_snapshots
                    WHERE timestamp = ? AND symbol = ? AND option_type = ?
                    ORDER BY strike {order}
                """, (ts, symbol, opt_type))

                rows = cursor.fetchall()
                for pos, row in enumerate(rows, start=1):
                    if pos <= 10:  # Only assign positions 1-10
                        cursor.execute("""
                            UPDATE option_snapshots
                            SET ordinal_position = ?
                            WHERE id = ?
                        """, (pos, row['id']))
                        updated += 1

            conn.commit()
            logger.info(f"Migrated ordinal positions for {updated} snapshots")
            return updated

        except Exception as e:
            logger.error(f"Error migrating ordinal positions: {e}")
            return 0
        finally:
            conn.close()


class EarningsCalendarManager:
    """Manages earnings calendar data for excluding earnings weeks from averages."""

    def __init__(self, db: 'OptionsHistoryDB' = None):
        """Initialize manager with database.

        Args:
            db: OptionsHistoryDB instance. Created if not provided.
        """
        self.db = db or OptionsHistoryDB()

    def fetch_earnings_dates_yfinance(self, symbol: str = 'APP') -> List[date]:
        """Fetch earnings dates from yfinance.

        Args:
            symbol: Stock symbol

        Returns:
            List of earnings dates (may include past and future)
        """
        try:
            ticker = yf.Ticker(symbol)
            calendar = ticker.calendar

            if calendar is None:
                logger.warning(f"No earnings calendar data for {symbol}")
                return []

            dates = []

            # yfinance calendar can be a DataFrame or dict depending on version
            if isinstance(calendar, dict):
                # New format: dict with 'Earnings Date' key
                if 'Earnings Date' in calendar:
                    earnings_dates = calendar['Earnings Date']
                    if isinstance(earnings_dates, list):
                        for ed in earnings_dates:
                            if hasattr(ed, 'date'):
                                dates.append(ed.date())
                            elif isinstance(ed, str):
                                try:
                                    dates.append(datetime.strptime(ed, '%Y-%m-%d').date())
                                except:
                                    pass
                    elif hasattr(earnings_dates, 'date'):
                        dates.append(earnings_dates.date())
            elif hasattr(calendar, 'empty'):
                # Old format: DataFrame
                if not calendar.empty and 'Earnings Date' in calendar.columns:
                    earnings_dates = calendar['Earnings Date']
                    for ed in earnings_dates:
                        if hasattr(ed, 'date'):
                            dates.append(ed.date())
                        elif isinstance(ed, str):
                            try:
                                dates.append(datetime.strptime(ed, '%Y-%m-%d').date())
                            except:
                                pass

            if dates:
                logger.info(f"Fetched {len(dates)} earnings dates for {symbol} from yfinance")
            else:
                logger.warning(f"No earnings dates found for {symbol} in yfinance calendar")

            return dates

        except Exception as e:
            logger.error(f"Error fetching earnings from yfinance: {e}")
            return []

    def calculate_earnings_week(self, earnings_date: date) -> Tuple[date, date]:
        """Calculate Monday-Friday of the week containing earnings date.

        Args:
            earnings_date: The earnings announcement date

        Returns:
            Tuple of (week_start, week_end) - Monday to Friday
        """
        # weekday() returns 0=Monday, 6=Sunday
        days_since_monday = earnings_date.weekday()
        week_start = earnings_date - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=4)  # Friday
        return week_start, week_end

    def store_earnings_date(self, symbol: str, earnings_date: date,
                            source: str = 'yfinance') -> bool:
        """Store earnings date and calculated week bounds in database.

        Args:
            symbol: Stock symbol
            earnings_date: Earnings announcement date
            source: Data source identifier

        Returns:
            True if stored successfully
        """
        week_start, week_end = self.calculate_earnings_week(earnings_date)

        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO earnings_calendar
                (symbol, earnings_date, week_start, week_end, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, earnings_date.isoformat(), week_start.isoformat(),
                  week_end.isoformat(), source, datetime.now()))
            conn.commit()
            logger.info(f"Stored earnings date {earnings_date} for {symbol} (week: {week_start} to {week_end})")
            return True
        except Exception as e:
            logger.error(f"Error storing earnings date: {e}")
            return False
        finally:
            conn.close()

    def is_earnings_week(self, check_date: date, symbol: str = 'APP') -> bool:
        """Check if a given date falls within any stored earnings week.

        Args:
            check_date: Date to check
            symbol: Stock symbol

        Returns:
            True if date is within an earnings week
        """
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 1 FROM earnings_calendar
                WHERE symbol = ? AND ? BETWEEN week_start AND week_end
                LIMIT 1
            """, (symbol, check_date.isoformat()))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking earnings week: {e}")
            return False
        finally:
            conn.close()

    def get_earnings_weeks(self, symbol: str = 'APP',
                           weeks_back: int = HISTORY_WEEKS) -> List[Tuple[date, date]]:
        """Get list of earnings week date ranges in the lookback period.

        Args:
            symbol: Stock symbol
            weeks_back: Number of weeks to look back

        Returns:
            List of (week_start, week_end) tuples
        """
        cutoff = date.today() - timedelta(weeks=weeks_back)

        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT week_start, week_end FROM earnings_calendar
                WHERE symbol = ? AND week_end >= ?
                ORDER BY week_start
            """, (symbol, cutoff.isoformat()))

            weeks = []
            for row in cursor.fetchall():
                week_start = date.fromisoformat(row['week_start'])
                week_end = date.fromisoformat(row['week_end'])
                weeks.append((week_start, week_end))

            return weeks
        except Exception as e:
            logger.error(f"Error getting earnings weeks: {e}")
            return []
        finally:
            conn.close()

    def refresh_earnings_calendar(self, symbol: str = 'APP') -> bool:
        """Fetch and store latest earnings dates from yfinance.

        Args:
            symbol: Stock symbol

        Returns:
            True if any earnings dates were stored
        """
        logger.info(f"Refreshing earnings calendar for {symbol}")

        dates = self.fetch_earnings_dates_yfinance(symbol)

        if not dates:
            logger.warning(f"No earnings dates found for {symbol}")
            return False

        stored_count = 0
        for ed in dates:
            if self.store_earnings_date(symbol, ed, 'yfinance'):
                stored_count += 1

        logger.info(f"Stored {stored_count} earnings dates for {symbol}")
        return stored_count > 0


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

            # Calculate time metadata for time-slot specific comparisons
            day_of_week = timestamp.weekday()  # 3=Thursday, 4=Friday
            # Round minutes to nearest 5-minute slot
            minute_slot = (timestamp.minute // 5) * 5
            time_slot = f"{timestamp.hour:02d}:{minute_slot:02d}"

            # Find THIS Friday's expiration only (not next week)
            today = timestamp.date()
            days_until_friday = (4 - today.weekday()) % 7  # 4 = Friday
            if today.weekday() == 4:  # It's Friday
                this_friday = today
            else:
                this_friday = today + timedelta(days=days_until_friday)

            # Find matching expiration from chain
            expirations = chain.get('expirations', [])
            target_expiration = None
            for exp in expirations:
                try:
                    exp_date = datetime.strptime(exp, '%Y-%m-%d').date()
                    if exp_date == this_friday:
                        target_expiration = exp
                        break
                except ValueError:
                    continue

            if not target_expiration:
                logger.warning(f"No options expiring this Friday ({this_friday})")
                return 0

            # Calculate actual DTE (1 on Thursday, 0 on Friday)
            expiration = target_expiration
            exp_date = datetime.strptime(expiration, '%Y-%m-%d').date()
            dte = (exp_date - today).days

            # Get chain for this expiration
            try:
                exp_chain = self.market_client.get_options_chain(symbol, expiration)
                calls_df = exp_chain.get('calls')
                puts_df = exp_chain.get('puts')
            except:
                # Use the default chain if specific expiration fetch fails
                calls_df = chain.get('calls')
                puts_df = chain.get('puts')

            # Process calls (10 nearest OTM) with ordinal positions 1-10
            if calls_df is not None and len(calls_df) > 0:
                otm_calls = calls_df[calls_df['strike'] > stock_price].head(10)
                for ordinal_pos, (_, row) in enumerate(otm_calls.iterrows(), start=1):
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
                        'open_interest': int(row.get('openInterest', 0)) if row.get('openInterest') else 0,
                        'day_of_week': day_of_week,
                        'time_slot': time_slot,
                        'ordinal_position': ordinal_pos
                    })

            # Process puts (10 nearest OTM) with ordinal positions 1-10
            if puts_df is not None and len(puts_df) > 0:
                otm_puts = puts_df[puts_df['strike'] < stock_price].tail(10).iloc[::-1]
                for ordinal_pos, (_, row) in enumerate(otm_puts.iterrows(), start=1):
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
                        'open_interest': int(row.get('openInterest', 0)) if row.get('openInterest') else 0,
                        'day_of_week': day_of_week,
                        'time_slot': time_slot,
                        'ordinal_position': ordinal_pos
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
                               ordinal_position: int, dte: int,
                               day_of_week: int = None, time_slot: str = None,
                               symbol: str = 'APP') -> dict:
        """Check if current price exceeds 6-week time-slot average by threshold.

        Compares current price to historical average at the same day/time slot.

        Args:
            current_price: Current option ASK price
            option_type: 'CALL' or 'PUT'
            ordinal_position: Position 1-10 (1 = nearest OTM, 10 = farthest OTM)
            dte: Days to expiration
            day_of_week: Day of week (3=Thursday, 4=Friday). Auto-detected if None.
            time_slot: Time slot "HH:MM" (e.g., "09:35"). Auto-detected if None.
            symbol: Stock symbol

        Returns:
            Dict with:
                - is_elevated: bool
                - current_price: float
                - avg_price: float or None
                - elevation_pct: float or None
                - confidence_boost: float (0.0 or 0.3)
                - has_historical_data: bool
                - day_of_week: int
                - time_slot: str
                - ordinal_position: int
        """
        # Auto-detect day/time if not provided
        if day_of_week is None or time_slot is None:
            now = datetime.now()
            if day_of_week is None:
                day_of_week = now.weekday()
            if time_slot is None:
                minute_slot = (now.minute // 5) * 5
                time_slot = f"{now.hour:02d}:{minute_slot:02d}"

        try:
            avg_price = self.db.get_average_price(
                option_type, ordinal_position, dte,
                day_of_week=day_of_week, time_slot=time_slot, symbol=symbol
            )

            if avg_price is None or avg_price <= 0:
                return {
                    'is_elevated': False,
                    'current_price': current_price,
                    'avg_price': None,
                    'elevation_pct': None,
                    'confidence_boost': 0.0,
                    'has_historical_data': False,
                    'day_of_week': day_of_week,
                    'time_slot': time_slot,
                    'ordinal_position': ordinal_position
                }

            if current_price <= 0:
                return {
                    'is_elevated': False,
                    'current_price': current_price,
                    'avg_price': avg_price,
                    'elevation_pct': None,
                    'confidence_boost': 0.0,
                    'has_historical_data': True,
                    'day_of_week': day_of_week,
                    'time_slot': time_slot,
                    'ordinal_position': ordinal_position
                }

            elevation_pct = (current_price - avg_price) / avg_price
            is_elevated = elevation_pct >= self.THRESHOLD_PERCENTAGE

            return {
                'is_elevated': is_elevated,
                'current_price': current_price,
                'avg_price': avg_price,
                'elevation_pct': elevation_pct,
                'confidence_boost': self.CONFIDENCE_BOOST if is_elevated else 0.0,
                'has_historical_data': True,
                'day_of_week': day_of_week,
                'time_slot': time_slot,
                'ordinal_position': ordinal_position
            }

        except Exception as e:
            logger.warning(f"Price elevation check failed: {e}")
            return {
                'is_elevated': False,
                'current_price': current_price,
                'avg_price': None,
                'elevation_pct': None,
                'confidence_boost': 0.0,
                'has_historical_data': False,
                'day_of_week': day_of_week,
                'time_slot': time_slot,
                'ordinal_position': ordinal_position
            }

    def evaluate_strikes(self, strikes: List[dict], stock_price: float,
                         option_type: str, dte: int,
                         symbol: str = 'APP') -> Tuple[List[dict], float]:
        """Evaluate a list of strike recommendations for price elevation.

        Compares current ASK prices to time-slot specific historical averages.
        For example: Thursday 9:35 AM prices vs 6-week avg of Thursday 9:35 AM.

        Adds price comparison data to each strike dict.

        Args:
            strikes: List of strike dicts from signal (ordered nearest to farthest OTM)
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

        # Calculate current day/time slot for time-specific comparison
        now = datetime.now()
        day_of_week = now.weekday()  # 3=Thursday, 4=Friday
        minute_slot = (now.minute // 5) * 5
        time_slot = f"{now.hour:02d}:{minute_slot:02d}"

        max_boost = 0.0
        enhanced = []

        # Strikes are passed in order (nearest to farthest OTM), so index+1 = ordinal position
        for ordinal_pos, strike_data in enumerate(strikes, start=1):
            # Prioritize ASK price for comparison (ask vs ask)
            option_price = strike_data.get('ask') or strike_data.get('last_price') or 0

            # Check price elevation with time-slot context using ordinal position
            comparison = self.check_price_elevation(
                current_price=option_price,
                option_type=option_type,
                ordinal_position=ordinal_pos,
                dte=dte,
                day_of_week=day_of_week,
                time_slot=time_slot,
                symbol=symbol
            )

            # Add comparison data to strike
            enhanced_strike = strike_data.copy()
            enhanced_strike['price_comparison'] = comparison
            enhanced_strike['ordinal_position'] = ordinal_pos
            enhanced.append(enhanced_strike)

            # Track max boost
            if comparison.get('confidence_boost', 0) > max_boost:
                max_boost = comparison['confidence_boost']

        return enhanced, max_boost


# Singleton instances
_db_instance: Optional[OptionsHistoryDB] = None
_collector_instance: Optional[OptionsDataCollector] = None
_checker_instance: Optional[PriceComparisonChecker] = None
_earnings_manager_instance: Optional[EarningsCalendarManager] = None


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


def get_earnings_manager() -> EarningsCalendarManager:
    """Get singleton earnings calendar manager instance."""
    global _earnings_manager_instance
    if _earnings_manager_instance is None:
        _earnings_manager_instance = EarningsCalendarManager(get_options_db())
    return _earnings_manager_instance
