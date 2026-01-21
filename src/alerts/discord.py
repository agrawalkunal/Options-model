"""Discord webhook notifications for APP options signals."""

import os
import logging
from datetime import datetime
from typing import Optional

from discord_webhook import DiscordWebhook, DiscordEmbed

from ..signals.base import Signal, SignalDirection, SignalStrength

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Sends trading signals to Discord via webhook."""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
        if not self.webhook_url:
            logger.warning("Discord webhook URL not configured. Notifications disabled.")

    def send_signal(self, signal: Signal) -> bool:
        """Send a trading signal notification to Discord.

        Args:
            signal: The Signal object to send

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.webhook_url:
            logger.warning("Cannot send notification: webhook URL not configured")
            return False

        try:
            webhook = DiscordWebhook(url=self.webhook_url)

            # Create embed based on signal direction
            if signal.direction == SignalDirection.CALL:
                color = "03fc07"  # Green
                emoji = "📈"
                direction_text = "LONG CALL"
            elif signal.direction == SignalDirection.PUT:
                color = "fc0303"  # Red
                emoji = "📉"
                direction_text = "LONG PUT"
            else:
                color = "808080"  # Gray
                emoji = "⚪"
                direction_text = "NEUTRAL"

            # Create the embed
            embed = DiscordEmbed(
                title=f"{emoji} APP OPTIONS ALERT {emoji}",
                description=f"**Signal:** {signal.name}\n**Direction:** {direction_text}",
                color=color
            )

            # Add timestamp
            embed.set_timestamp(signal.timestamp.isoformat())

            # Stock data field
            details = signal.details
            current_price = details.get("current_price", "N/A")
            embed.add_embed_field(
                name="📊 Stock Data",
                value=f"**Price:** ${current_price:.2f}" if isinstance(current_price, (int, float)) else f"**Price:** {current_price}",
                inline=True
            )

            # Signal strength field (compact)
            strength_emoji = {
                SignalStrength.STRONG: "🔥",
                SignalStrength.MODERATE: "⚡",
                SignalStrength.WEAK: "💡"
            }
            strength_value = f"{strength_emoji.get(signal.strength, '')} {signal.strength.name}"

            embed.add_embed_field(
                name="💪 Strength",
                value=strength_value,
                inline=True
            )

            # Confidence breakdown field
            breakdown_text = self._format_confidence_breakdown(details, signal.confidence)
            embed.add_embed_field(
                name="📊 Confidence Breakdown",
                value=breakdown_text,
                inline=False
            )

            # Catalyst details field
            catalyst_type = details.get("catalyst_type", "unknown")
            catalyst_info = self._format_catalyst_info(details)
            embed.add_embed_field(
                name="⚡ Catalyst",
                value=catalyst_info,
                inline=False
            )

            # Recommended strikes field
            if signal.recommended_strikes:
                strikes_text = self._format_strikes(signal.recommended_strikes)
                embed.add_embed_field(
                    name="🎯 Recommended Strikes",
                    value=strikes_text,
                    inline=False
                )

            # Risk warning
            embed.add_embed_field(
                name="⚠️ Risk Warning",
                value="0-2 DTE options are extremely risky. Only trade with money you can afford to lose.",
                inline=False
            )

            # Footer
            embed.set_footer(text="APP Options Trading Model | Not Financial Advice")

            webhook.add_embed(embed)
            response = webhook.execute()

            if response.status_code in [200, 204]:
                logger.info(f"Discord notification sent successfully for {signal.name}")
                return True
            else:
                logger.error(f"Failed to send Discord notification: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error sending Discord notification: {e}")
            return False

    def _format_catalyst_info(self, details: dict) -> str:
        """Format catalyst details for display."""
        catalyst_type = details.get("catalyst_type", "unknown")

        if catalyst_type == "ad_sector_news":
            headline = details.get("headline", "N/A")[:100]
            source = details.get("source", "Unknown")
            sentiment = details.get("sentiment", "neutral").upper()
            return f"**Type:** Ad Sector News\n**Headline:** {headline}...\n**Source:** {source}\n**Sentiment:** {sentiment}"

        elif catalyst_type == "company_news":
            headline = details.get("headline", "N/A")[:100]
            source = details.get("source", "Unknown")
            return f"**Type:** Company News\n**Headline:** {headline}...\n**Source:** {source}"

        elif catalyst_type == "friday_0dte":
            premarket = details.get("premarket_move", 0)
            factors = details.get("setup_factors", [])
            factors_text = "\n".join([f"• {f}" for f in factors[:3]])
            return f"**Type:** Friday 0DTE Setup\n**Pre-market:** {premarket:+.1f}%\n**Factors:**\n{factors_text}"

        else:
            return f"**Type:** {catalyst_type}"

    def _format_confidence_breakdown(self, details: dict, final_confidence: float) -> str:
        """Format confidence breakdown for display.

        Args:
            details: Signal details containing confidence_breakdown
            final_confidence: The final confidence value

        Returns:
            Formatted string showing breakdown
        """
        breakdown = details.get("confidence_breakdown")

        if not breakdown or not breakdown.get("components"):
            # Fallback if no breakdown available
            return f"**Total:** {final_confidence:.0%}"

        components = breakdown["components"]
        lines = []

        # Add each component
        for comp in components:
            name = comp["name"]
            value = comp["value"]
            description = comp.get("description", "")

            # Format: "+ 15% Keyword matches (3) - description"
            if description:
                lines.append(f"+ **{value:.0%}** {name}\n  _{description}_")
            else:
                lines.append(f"+ **{value:.0%}** {name}")

        # Add separator and total
        lines.append("─" * 18)

        # Check if total was capped
        raw_total = sum(c["value"] for c in components)
        if raw_total > 1.0:
            lines.append(f"= **{final_confidence:.0%}** Total _(capped)_")
        else:
            lines.append(f"= **{final_confidence:.0%}** Total")

        return "\n".join(lines)

    def _format_strikes(self, strikes: list) -> str:
        """Format strike recommendations for display."""
        if not strikes:
            return "No specific strikes recommended"

        lines = []
        for strike in strikes[:3]:
            strike_price = strike.get("strike", 0)
            strike_type = strike.get("type", "?")
            otm_pct = strike.get("otm_pct", 0)
            last_price = strike.get("last_price", 0)
            bid = strike.get("bid", 0)
            ask = strike.get("ask", 0)

            if last_price or bid or ask:
                price_info = f"@ ${last_price:.2f}" if last_price else f"Bid/Ask: ${bid:.2f}/${ask:.2f}"
                line = f"• **${strike_price:.0f}{strike_type[0]}** ({otm_pct:.1f}% OTM) {price_info}"
            else:
                line = f"• **${strike_price:.0f}{strike_type[0]}** ({otm_pct:.1f}% OTM)"

            # Add price comparison indicator if present
            comparison = strike.get("price_comparison", {})
            if comparison.get("is_elevated"):
                elevation_pct = comparison.get("elevation_pct", 0) * 100
                line += f" **[+{elevation_pct:.0f}% vs avg]**"
            elif comparison.get("has_historical_data") is False:
                line += " *(no history)*"

            lines.append(line)

        return "\n".join(lines)

    def send_test_message(self) -> bool:
        """Send a test message to verify webhook configuration."""
        if not self.webhook_url:
            logger.warning("Cannot send test: webhook URL not configured")
            return False

        try:
            webhook = DiscordWebhook(
                url=self.webhook_url,
                content="🧪 **APP Options Alert System Test**\n\nWebhook is configured correctly!"
            )
            response = webhook.execute()

            if response.status_code in [200, 204]:
                logger.info("Test message sent successfully")
                return True
            else:
                logger.error(f"Failed to send test message: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Error sending test message: {e}")
            return False

    def send_daily_summary(self, signals_today: list, price_change: float) -> bool:
        """Send end-of-day summary.

        Args:
            signals_today: List of signals generated today
            price_change: APP's price change for the day

        Returns:
            True if sent successfully
        """
        if not self.webhook_url:
            return False

        try:
            webhook = DiscordWebhook(url=self.webhook_url)

            embed = DiscordEmbed(
                title="📋 Daily Summary - APP Options",
                description=f"**Date:** {datetime.now().strftime('%Y-%m-%d')}",
                color="1E90FF"
            )

            # Price summary
            direction = "📈" if price_change > 0 else "📉" if price_change < 0 else "➡️"
            embed.add_embed_field(
                name="📊 APP Performance",
                value=f"{direction} {price_change:+.2f}%",
                inline=True
            )

            # Signals summary
            embed.add_embed_field(
                name="🔔 Signals Today",
                value=str(len(signals_today)),
                inline=True
            )

            # List signals if any
            if signals_today:
                signals_text = "\n".join([
                    f"• {s.name} ({s.direction.value}) @ {s.timestamp.strftime('%H:%M')}"
                    for s in signals_today[:5]
                ])
                embed.add_embed_field(
                    name="📝 Signal Details",
                    value=signals_text,
                    inline=False
                )

            embed.set_footer(text="APP Options Trading Model")
            embed.set_timestamp()

            webhook.add_embed(embed)
            response = webhook.execute()

            return response.status_code in [200, 204]

        except Exception as e:
            logger.error(f"Error sending daily summary: {e}")
            return False


# Singleton instance
_notifier = None


def get_notifier() -> DiscordNotifier:
    """Get the singleton DiscordNotifier instance."""
    global _notifier
    if _notifier is None:
        _notifier = DiscordNotifier()
    return _notifier
