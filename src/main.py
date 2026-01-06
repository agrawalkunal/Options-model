"""Main entry point for APP Options Trading Alert System.

This module runs the signal detection loop and sends alerts via Discord.
Designed to run on Thursday and Friday during market hours.
"""

import os
import sys
import logging
import argparse
from datetime import datetime, time
from typing import List

import schedule

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from src.signals import AdSectorSignal, CompanyNewsSignal, Friday0DTESignal, LiveNewsSignal, Signal
from src.alerts import get_notifier
from src.data.schwab_client import get_client
from src.data.options_history import get_collector, get_options_db

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app_options.log')
    ]
)
logger = logging.getLogger(__name__)

# Market hours (Eastern Time)
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)
PREMARKET_START = time(9, 0)

# Signal check intervals in minutes
CHECK_INTERVAL = 5  # Standard signals
LIVE_NEWS_INTERVAL = 2  # Live news checks (more frequent)


class TradingAlertSystem:
    """Main trading alert system that orchestrates signal detection and notifications."""

    def __init__(self):
        # Standard signals (checked every 5 minutes)
        self.signals = [
            AdSectorSignal(),
            CompanyNewsSignal(),
            Friday0DTESignal(),
        ]
        # Live news signal (checked every 2 minutes)
        self.live_news_signal = LiveNewsSignal()
        self.notifier = get_notifier()
        self.market_client = get_client()
        self.signals_today: List[Signal] = []
        self.last_check = None
        self.last_live_news_check = None

        # Options history data collector
        self.options_collector = get_collector()
        self.options_db = get_options_db()

    def is_trading_day(self) -> bool:
        """Check if today is Thursday or Friday."""
        weekday = datetime.now().weekday()
        return weekday in [3, 4]  # Thursday=3, Friday=4

    def is_market_hours(self) -> bool:
        """Check if current time is within market hours."""
        now = datetime.now().time()
        return PREMARKET_START <= now <= MARKET_CLOSE

    def check_signals(self) -> List[Signal]:
        """Run all signal checks and return detected signals."""
        detected = []

        for signal_detector in self.signals:
            try:
                signal = signal_detector.check()
                if signal and signal.is_actionable:
                    detected.append(signal)
                    logger.info(f"Signal detected: {signal}")
            except Exception as e:
                logger.error(f"Error checking {signal_detector.name}: {e}")

        return detected

    def process_signals(self, signals: List[Signal]):
        """Process detected signals and send notifications."""
        for signal in signals:
            # Avoid duplicate notifications for similar signals
            if self._is_duplicate(signal):
                logger.debug(f"Skipping duplicate signal: {signal.name}")
                continue

            # Send Discord notification
            success = self.notifier.send_signal(signal)
            if success:
                self.signals_today.append(signal)
                logger.info(f"Alert sent for {signal.name}")
            else:
                logger.error(f"Failed to send alert for {signal.name}")

    def _is_duplicate(self, signal: Signal) -> bool:
        """Check if a similar signal was already sent recently."""
        for recent in self.signals_today[-10:]:
            # Same signal type within last hour
            if (recent.name == signal.name and
                recent.direction == signal.direction and
                (signal.timestamp - recent.timestamp).seconds < 3600):
                return True
        return False

    def run_check(self):
        """Run a single signal check cycle for standard signals."""
        if not self.is_trading_day():
            logger.info("Not a trading day (Thursday/Friday). Skipping check.")
            return

        if not self.is_market_hours():
            logger.info("Outside market hours. Skipping check.")
            return

        logger.info("Running standard signal check...")
        self.last_check = datetime.now()

        signals = self.check_signals()

        if signals:
            logger.info(f"Detected {len(signals)} actionable signal(s)")
            self.process_signals(signals)
        else:
            logger.info("No actionable signals detected")

    def run_live_news_check(self):
        """Run live news check (more frequent than standard signals)."""
        if not self.is_trading_day():
            return

        if not self.is_market_hours():
            return

        logger.debug("Running live news check...")
        self.last_live_news_check = datetime.now()

        try:
            signal = self.live_news_signal.check()
            if signal and signal.is_actionable:
                logger.info(f"Live news signal detected: {signal}")
                self.process_signals([signal])
        except Exception as e:
            logger.error(f"Error checking live news: {e}")

    def send_daily_summary(self):
        """Send end-of-day summary."""
        try:
            quote = self.market_client.get_quote("APP")
            change_pct = quote.get("change_pct", 0)

            self.notifier.send_daily_summary(self.signals_today, change_pct)
            logger.info("Daily summary sent")

            # Reset for next day
            self.signals_today = []
        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")

    def collect_options_data(self):
        """Collect option price snapshot for historical tracking."""
        if not self.is_trading_day():
            return

        if not self.is_market_hours():
            return

        try:
            count = self.options_collector.collect_snapshot()
            if count > 0:
                logger.debug(f"Collected {count} option price snapshots")
        except Exception as e:
            logger.error(f"Options data collection error: {e}")

    def recalculate_averages(self):
        """Recalculate 6-week rolling averages at end of trading day."""
        if not self.is_trading_day():
            return

        try:
            success = self.options_db.calculate_and_store_averages()
            if success:
                logger.info("Historical price averages recalculated")
            else:
                logger.warning("Failed to recalculate historical averages")
        except Exception as e:
            logger.error(f"Average calculation error: {e}")

    def cleanup_old_history(self):
        """Remove option price data older than 6 weeks."""
        if not self.is_trading_day():
            return

        try:
            deleted = self.options_db.cleanup_old_data(weeks=6)
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old option price snapshots")
        except Exception as e:
            logger.error(f"History cleanup error: {e}")

    def run(self):
        """Start the main loop with scheduled checks."""
        logger.info("=" * 50)
        logger.info("APP Options Trading Alert System Starting")
        logger.info("=" * 50)
        logger.info(f"Standard signal check interval: {CHECK_INTERVAL} minutes")
        logger.info(f"Live news check interval: {LIVE_NEWS_INTERVAL} minutes")
        logger.info(f"Options data collection: Every {CHECK_INTERVAL} minutes")
        logger.info(f"Active days: Thursday, Friday")
        logger.info(f"Market hours: {PREMARKET_START} - {MARKET_CLOSE}")

        # Log options history status
        snapshot_count = self.options_db.get_snapshot_count()
        logger.info(f"Historical option snapshots in DB: {snapshot_count}")
        logger.info("=" * 50)

        # Schedule standard signal checks (every 5 minutes)
        schedule.every(CHECK_INTERVAL).minutes.do(self.run_check)

        # Schedule live news checks (every 2 minutes)
        schedule.every(LIVE_NEWS_INTERVAL).minutes.do(self.run_live_news_check)

        # Schedule options data collection (every 5 minutes during market hours)
        schedule.every(CHECK_INTERVAL).minutes.do(self.collect_options_data)

        # Schedule average recalculation at market close
        schedule.every().day.at("16:01").do(self.recalculate_averages)

        # Schedule cleanup of old data
        schedule.every().day.at("16:05").do(self.cleanup_old_history)

        # Schedule daily summary at market close
        schedule.every().day.at("16:10").do(self.send_daily_summary)

        # Run initial checks
        self.run_check()
        self.run_live_news_check()

        # Main loop
        logger.info("Entering main loop. Press Ctrl+C to stop.")
        try:
            while True:
                schedule.run_pending()
                import time as time_module
                time_module.sleep(30)  # Check every 30 seconds for scheduled tasks
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            sys.exit(0)


def test_mode():
    """Run in test mode - single check without scheduling."""
    logger.info("Running in TEST MODE")

    system = TradingAlertSystem()

    # Test Discord webhook
    logger.info("Testing Discord webhook...")
    if system.notifier.send_test_message():
        logger.info("Discord webhook test successful!")
    else:
        logger.warning("Discord webhook test failed - check your DISCORD_WEBHOOK_URL")

    # Run a single check on standard signals
    logger.info("Running standard signal check...")
    signals = system.check_signals()

    if signals:
        logger.info(f"Found {len(signals)} standard signals:")
        for signal in signals:
            logger.info(f"  - {signal}")
    else:
        logger.info("No standard signals detected (this is normal if no catalyst present)")

    # Run live news check
    logger.info("Running live news signal check...")
    live_signal = system.live_news_signal.check()
    if live_signal:
        logger.info(f"Live news signal: {live_signal}")
    else:
        logger.info("No live news signal detected")

    # Get current APP quote
    quote = system.market_client.get_quote("APP")
    if quote:
        logger.info(f"Current APP price: ${quote.get('price', 'N/A')}")
        logger.info(f"Change: {quote.get('change_pct', 0):+.2f}%")


def main():
    parser = argparse.ArgumentParser(description="APP Options Trading Alert System")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode (single check, no scheduling)"
    )
    parser.add_argument(
        "--test-webhook",
        action="store_true",
        help="Send a test message to Discord webhook"
    )
    args = parser.parse_args()

    if args.test_webhook:
        notifier = get_notifier()
        if notifier.send_test_message():
            print("Test message sent successfully!")
        else:
            print("Failed to send test message. Check your DISCORD_WEBHOOK_URL.")
        return

    if args.test:
        test_mode()
    else:
        system = TradingAlertSystem()
        system.run()


if __name__ == "__main__":
    main()
