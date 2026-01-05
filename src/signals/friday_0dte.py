"""Friday 0DTE setup signal detector.

Detects favorable conditions for 0DTE options plays on Fridays:
- Pre-market momentum
- High open interest on OTM strikes
- Unusual volume patterns
"""

import logging
from datetime import datetime, time
from typing import Optional

from .base import BaseSignal, Signal, SignalDirection, SignalStrength
from ..data.schwab_client import get_client

logger = logging.getLogger(__name__)


class Friday0DTESignal(BaseSignal):
    """Detects favorable 0DTE setups on Fridays."""

    def __init__(self):
        super().__init__("Friday 0DTE Setup")
        self.market_client = get_client()

        # Thresholds
        self.premarket_momentum_threshold = 0.02  # 2% pre-market move
        self.volume_ratio_threshold = 2.0  # 2x average volume
        self.oi_threshold = 100  # Minimum open interest

    def get_description(self) -> str:
        return (
            "Detects favorable 0DTE option setups on Fridays based on "
            "pre-market momentum, unusual volume, and open interest patterns."
        )

    def check(self) -> Optional[Signal]:
        """Check for Friday 0DTE setup conditions.

        Returns:
            Signal if favorable setup detected, None otherwise.
        """
        if not self.enabled:
            return None

        # Only run on Fridays
        if not self.is_friday():
            logger.debug("Not Friday - skipping 0DTE check")
            return None

        try:
            # Get current quote and price data
            quote = self.market_client.get_quote("APP")
            current_price = quote.get("price", 0)
            change_pct = quote.get("change_pct", 0)

            if not current_price:
                logger.warning("Could not get APP price")
                return None

            # Get options chain for nearest expiration (should be 0DTE on Friday)
            chain = self.market_client.get_options_chain("APP")

            if not chain.get("calls") is not None:
                logger.warning("Could not get options chain")
                return None

            # Analyze the setup
            setup = self._analyze_setup(
                current_price=current_price,
                change_pct=change_pct,
                calls=chain["calls"],
                puts=chain["puts"]
            )

            if not setup["is_favorable"]:
                logger.debug("No favorable 0DTE setup detected")
                return None

            direction = setup["direction"]
            confidence = setup["confidence"]

            # Determine strength
            if confidence >= 0.7:
                strength = SignalStrength.STRONG
            elif confidence >= 0.5:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            # Get specific strike recommendations from options chain
            strikes = self._get_best_strikes(
                current_price=current_price,
                direction=direction,
                calls=chain["calls"],
                puts=chain["puts"]
            )

            signal = Signal(
                name=self.name,
                direction=direction,
                strength=strength,
                confidence=confidence,
                timestamp=datetime.now(),
                details={
                    "catalyst_type": "friday_0dte",
                    "current_price": current_price,
                    "premarket_move": change_pct,
                    "momentum_direction": "up" if change_pct > 0 else "down",
                    "expiration": chain.get("expiration"),
                    "setup_factors": setup["factors"],
                },
                recommended_strikes=strikes
            )

            logger.info(f"Friday 0DTE signal detected: {signal}")
            return signal

        except Exception as e:
            logger.error(f"Error checking Friday 0DTE signal: {e}")
            return None

    def _analyze_setup(self, current_price: float, change_pct: float,
                       calls, puts) -> dict:
        """Analyze if current conditions favor a 0DTE play.

        Args:
            current_price: Current stock price
            change_pct: Pre-market/current change percentage
            calls: Calls DataFrame
            puts: Puts DataFrame

        Returns:
            dict with setup analysis
        """
        factors = []
        confidence = 0.0

        # Factor 1: Pre-market momentum
        if abs(change_pct) >= self.premarket_momentum_threshold * 100:
            factors.append(f"Strong pre-market momentum: {change_pct:+.1f}%")
            confidence += 0.3

        # Determine direction from momentum
        if change_pct > self.premarket_momentum_threshold * 100:
            direction = SignalDirection.CALL
            relevant_chain = calls
        elif change_pct < -self.premarket_momentum_threshold * 100:
            direction = SignalDirection.PUT
            relevant_chain = puts
        else:
            # Neutral momentum - check options flow
            direction = SignalDirection.NEUTRAL
            relevant_chain = None

        # Factor 2: Check options chain for unusual activity
        if relevant_chain is not None and len(relevant_chain) > 0:
            # Find OTM options
            if direction == SignalDirection.CALL:
                otm_options = relevant_chain[relevant_chain['strike'] > current_price]
            else:
                otm_options = relevant_chain[relevant_chain['strike'] < current_price]

            if len(otm_options) > 0:
                # Check for high open interest
                high_oi = otm_options[otm_options['openInterest'] >= self.oi_threshold]
                if len(high_oi) > 0:
                    factors.append(f"High OI on {len(high_oi)} OTM strikes")
                    confidence += 0.2

                # Check for unusual volume
                avg_volume = otm_options['volume'].mean() if 'volume' in otm_options else 0
                if avg_volume > 0:
                    high_vol = otm_options[otm_options['volume'] > avg_volume * self.volume_ratio_threshold]
                    if len(high_vol) > 0:
                        factors.append(f"Unusual volume on {len(high_vol)} strikes")
                        confidence += 0.2

        # Factor 3: Time of day (early morning is better for 0DTE)
        now = datetime.now().time()
        if time(9, 30) <= now <= time(11, 0):
            factors.append("Optimal entry window (morning)")
            confidence += 0.1

        # Factor 4: It's Friday with 0DTE available
        factors.append("Friday 0DTE expiration available")
        confidence += 0.2

        is_favorable = confidence >= 0.5 and direction != SignalDirection.NEUTRAL

        return {
            "is_favorable": is_favorable,
            "direction": direction,
            "confidence": min(confidence, 1.0),
            "factors": factors
        }

    def _get_best_strikes(self, current_price: float, direction: SignalDirection,
                          calls, puts) -> list:
        """Get the best strikes from the options chain.

        Args:
            current_price: Current stock price
            direction: CALL or PUT
            calls: Calls DataFrame
            puts: Puts DataFrame

        Returns:
            List of recommended strike dictionaries
        """
        recommendations = []

        if direction == SignalDirection.CALL and calls is not None:
            otm = calls[calls['strike'] > current_price].head(5)
            for _, row in otm.iterrows():
                otm_pct = ((row['strike'] - current_price) / current_price) * 100
                recommendations.append({
                    "strike": row['strike'],
                    "type": "CALL",
                    "otm_pct": round(otm_pct, 1),
                    "last_price": row.get('lastPrice', 0),
                    "bid": row.get('bid', 0),
                    "ask": row.get('ask', 0),
                    "volume": int(row.get('volume', 0)) if row.get('volume') else 0,
                    "open_interest": int(row.get('openInterest', 0)) if row.get('openInterest') else 0,
                    "iv": row.get('impliedVolatility', 0),
                })

        elif direction == SignalDirection.PUT and puts is not None:
            otm = puts[puts['strike'] < current_price].tail(5).iloc[::-1]
            for _, row in otm.iterrows():
                otm_pct = ((current_price - row['strike']) / current_price) * 100
                recommendations.append({
                    "strike": row['strike'],
                    "type": "PUT",
                    "otm_pct": round(otm_pct, 1),
                    "last_price": row.get('lastPrice', 0),
                    "bid": row.get('bid', 0),
                    "ask": row.get('ask', 0),
                    "volume": int(row.get('volume', 0)) if row.get('volume') else 0,
                    "open_interest": int(row.get('openInterest', 0)) if row.get('openInterest') else 0,
                    "iv": row.get('impliedVolatility', 0),
                })

        return recommendations[:3]  # Return top 3
