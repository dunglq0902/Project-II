"""
alert-engine/notifier/notifiers.py
Notification dispatchers for Email and Webhook channels.

Each notifier exposes an async send(event, rule) coroutine.
The FastAPI notification service (below) routes to the correct notifier.
"""

import logging
import os
import smtplib
import ssl
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger("Notifiers")


# ═══════════════════════════════════════════════════════════════════════════════
# Base Notifier
# ═══════════════════════════════════════════════════════════════════════════════

class BaseNotifier(ABC):
    @abstractmethod
    async def send(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """Send a notification. Returns True on success."""

    @staticmethod
    def _format_plain_text(event: Dict[str, Any], rule: Dict[str, Any]) -> str:
        """Build a human-readable plain-text alert message (no Markdown)."""
        action       = event.get("action", "SIGNAL")
        symbol       = event.get("symbol", "UNKNOWN")
        close_price  = event.get("close_price")
        rsi          = event.get("rsi_14")
        macd         = event.get("macd")
        volume_ratio = event.get("volume_ratio")
        pattern      = event.get("candle_pattern", "N/A")
        timeframe    = event.get("timeframe", "")
        triggered_at = event.get("triggered_at", datetime.now(tz=timezone.utc).isoformat())

        action_label = {
            "BUY":  "[BUY SIGNAL]",
            "SELL": "[SELL SIGNAL]",
        }.get(action, f"[{action} SIGNAL]")

        close_str = f"{close_price:.4f} USDT" if close_price is not None else "N/A"
        rsi_str   = f"{rsi:.2f}"              if rsi   is not None else "N/A"
        macd_str  = f"{macd:.4f}"             if macd  is not None else "N/A"
        vol_str   = f"{volume_ratio:.2f}x"    if volume_ratio      else "N/A"

        lines = [
            f"{action_label}",
            f"",
            f"Symbol:       {symbol} ({timeframe})",
            f"Price:        {close_str}",
            f"RSI(14):      {rsi_str}",
            f"MACD:         {macd_str}",
            f"Volume Ratio: {vol_str}",
            f"Pattern:      {pattern}",
            f"Triggered At: {triggered_at}",
            f"Rule ID:      {event.get('rule_id', 'N/A')[:12]}...",
            f"",
            f"---",
            f"Crypto Analytics Platform - Automated Alert",
        ]
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# Email Notifier
# ═══════════════════════════════════════════════════════════════════════════════

class EmailNotifier(BaseNotifier):
    """
    Sends alert notifications via SMTP (TLS).
    The recipient email is stored in the alert rule document.

    SMTP configuration is read lazily from environment variables so that
    dotenv / k8s secrets injected after module import are respected.
    """

    def _get_smtp_config(self) -> Dict[str, Any]:
        """Read SMTP configuration from environment at call time (not module load)."""
        return {
            "host":     os.getenv("SMTP_HOST",     "smtp.gmail.com"),
            "port":     int(os.getenv("SMTP_PORT",  "587")),
            "user":     os.getenv("SMTP_USER",      ""),
            "password": os.getenv("SMTP_PASSWORD",  ""),
            "from":     os.getenv("EMAIL_FROM",     "") or os.getenv("SMTP_USER", ""),
        }

    # ── HTML Template ─────────────────────────────────────────────────────

    def _build_html(self, event: Dict[str, Any], rule: Dict[str, Any]) -> str:
        action      = event.get("action", "SIGNAL")
        symbol      = event.get("symbol", "")
        close_price = event.get("close_price")
        rsi         = event.get("rsi_14")
        macd        = event.get("macd")
        macd_signal_v = event.get("macd_signal")
        volume_ratio= event.get("volume_ratio")
        timeframe   = event.get("timeframe", "")
        pattern     = event.get("candle_pattern", "N/A")
        triggered   = str(event.get("triggered_at", ""))[:19]
        rule_id     = event.get("rule_id", "")[:12]

        # Color scheme per action
        if action == "BUY":
            bg_gradient = "linear-gradient(135deg, #00b894 0%, #00cec9 100%)"
            badge_bg    = "#00b894"
            emoji       = "🟢"
        elif action == "SELL":
            bg_gradient = "linear-gradient(135deg, #d63031 0%, #e17055 100%)"
            badge_bg    = "#d63031"
            emoji       = "🔴"
        else:
            bg_gradient = "linear-gradient(135deg, #0984e3 0%, #74b9ff 100%)"
            badge_bg    = "#0984e3"
            emoji       = "🔵"

        # Format values safely
        close_str = f"{close_price:,.4f} USDT" if close_price is not None else "N/A"
        rsi_str   = f"{rsi:.2f}"                if rsi  is not None else "N/A"
        macd_str  = f"{macd:.4f}"               if macd is not None else "N/A"
        vol_str   = f"{volume_ratio:.2f}x"      if volume_ratio     else "N/A"

        # RSI color hint
        rsi_color = "#2d3436"
        if rsi is not None:
            if rsi < 30:
                rsi_color = "#00b894"  # Oversold → bullish
            elif rsi > 70:
                rsi_color = "#d63031"  # Overbought → bearish

        return f"""\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{action} Signal - {symbol}</title>
</head>
<body style="margin:0;padding:0;background:#f0f2f5;font-family:'Segoe UI',Roboto,Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f0f2f5;padding:20px 0;">
    <tr>
      <td align="center">
        <table role="presentation" width="520" cellpadding="0" cellspacing="0" style="max-width:520px;width:100%;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">
          <!-- Header -->
          <tr>
            <td style="background:{bg_gradient};padding:28px 32px;text-align:center;">
              <div style="font-size:36px;margin-bottom:8px;">{emoji}</div>
              <h1 style="margin:0;color:#ffffff;font-size:22px;font-weight:700;letter-spacing:0.5px;">
                {action} Signal – {symbol}
              </h1>
              <p style="margin:6px 0 0;color:rgba(255,255,255,0.85);font-size:13px;">
                {timeframe} &bull; {triggered} UTC
              </p>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="background:#ffffff;padding:24px 32px 16px;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                <!-- Price -->
                <tr>
                  <td style="padding:10px 0;border-bottom:1px solid #f0f2f5;">
                    <span style="color:#636e72;font-size:12px;text-transform:uppercase;letter-spacing:1px;">Price</span><br>
                    <span style="font-size:20px;font-weight:700;color:#2d3436;">{close_str}</span>
                  </td>
                </tr>
                <!-- Indicators Row -->
                <tr>
                  <td style="padding:16px 0 8px;">
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                      <tr>
                        <td width="33%" style="text-align:center;padding:8px;">
                          <div style="color:#636e72;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">RSI(14)</div>
                          <div style="font-size:18px;font-weight:700;color:{rsi_color};margin-top:4px;">{rsi_str}</div>
                        </td>
                        <td width="33%" style="text-align:center;padding:8px;border-left:1px solid #f0f2f5;border-right:1px solid #f0f2f5;">
                          <div style="color:#636e72;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">MACD</div>
                          <div style="font-size:18px;font-weight:700;color:#2d3436;margin-top:4px;">{macd_str}</div>
                        </td>
                        <td width="33%" style="text-align:center;padding:8px;">
                          <div style="color:#636e72;font-size:11px;text-transform:uppercase;letter-spacing:0.5px;">Vol Ratio</div>
                          <div style="font-size:18px;font-weight:700;color:#2d3436;margin-top:4px;">{vol_str}</div>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <!-- Pattern -->
                <tr>
                  <td style="padding:12px 0;border-top:1px solid #f0f2f5;">
                    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
                      <tr>
                        <td>
                          <span style="color:#636e72;font-size:12px;text-transform:uppercase;letter-spacing:1px;">Candle Pattern</span>
                        </td>
                        <td style="text-align:right;">
                          <span style="display:inline-block;background:{badge_bg};color:#fff;font-size:12px;font-weight:600;padding:4px 12px;border-radius:12px;">
                            {pattern}
                          </span>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#fafbfc;padding:16px 32px;border-top:1px solid #f0f2f5;">
              <p style="margin:0;color:#b2bec3;font-size:11px;text-align:center;line-height:1.6;">
                Rule ID: {rule_id}&hellip;<br>
                You are receiving this email because you set up an alert rule on
                <strong>Crypto Analytics Platform</strong>.<br>
                To manage your alerts, visit the Dashboard.
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    # ── Send ──────────────────────────────────────────────────────────────

    async def send(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        recipient = rule.get("email_address") or os.getenv("DEFAULT_ALERT_EMAIL", "")
        if not recipient:
            logger.warning("No email_address in rule %s and no DEFAULT_ALERT_EMAIL set", rule.get("rule_id"))
            return False

        cfg = self._get_smtp_config()

        # Validate SMTP credentials are present
        if not cfg["user"] or not cfg["password"]:
            logger.error(
                "SMTP credentials missing — SMTP_USER=%s SMTP_PASSWORD=%s",
                "set" if cfg["user"] else "EMPTY",
                "set" if cfg["password"] else "EMPTY",
            )
            return False

        # Build the email
        action  = event.get("action", "SIGNAL")
        symbol  = event.get("symbol", "")
        subject = f"[Crypto Alert] {action} Signal – {symbol}"

        # Use SMTP_USER as the sender address if EMAIL_FROM is a custom domain
        # Gmail will reject/override custom FROM domains that aren't verified aliases
        sender = cfg["from"] if cfg["from"] else cfg["user"]
        # For Gmail: if EMAIL_FROM is a non-Gmail domain, force use SMTP_USER
        if cfg["host"] in ("smtp.gmail.com", "smtp.googlemail.com"):
            if sender and "@gmail.com" not in sender and "@googlemail.com" not in sender:
                logger.info(
                    "Gmail detected — overriding EMAIL_FROM '%s' with SMTP_USER '%s'",
                    sender, cfg["user"],
                )
                sender = cfg["user"]

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"Crypto Analytics <{sender}>"
        msg["To"]      = recipient
        msg["Reply-To"] = sender

        # Attach plain text first, then HTML (email clients prefer last alternative)
        msg.attach(MIMEText(self._format_plain_text(event, rule), "plain", "utf-8"))
        msg.attach(MIMEText(self._build_html(event, rule), "html", "utf-8"))

        try:
            logger.info(
                "Sending email | host=%s:%d user=%s from=%s to=%s",
                cfg["host"], cfg["port"], cfg["user"], sender, recipient,
            )
            context = ssl.create_default_context()
            with smtplib.SMTP(cfg["host"], cfg["port"], timeout=30) as server:
                server.ehlo()
                server.starttls(context=context)
                server.ehlo()
                server.login(cfg["user"], cfg["password"])
                server.sendmail(sender, recipient, msg.as_string())
            logger.info(
                "Email sent successfully | to=%s rule=%s symbol=%s action=%s",
                recipient, event.get("rule_id"), symbol, action,
            )
            return True
        except smtplib.SMTPAuthenticationError as exc:
            logger.error(
                "SMTP authentication failed (check SMTP_USER/SMTP_PASSWORD or App Password): %s",
                exc,
            )
        except smtplib.SMTPRecipientsRefused as exc:
            logger.error("SMTP recipients refused (%s): %s", recipient, exc)
        except smtplib.SMTPException as exc:
            logger.error(
                "SMTP error | host=%s:%d error=%s", cfg["host"], cfg["port"], exc,
            )
        except ConnectionError as exc:
            logger.error(
                "Cannot connect to SMTP server %s:%d — %s", cfg["host"], cfg["port"], exc,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Unexpected email send error: %s", exc)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# Webhook Notifier
# ═══════════════════════════════════════════════════════════════════════════════

class WebhookNotifier(BaseNotifier):
    """
    POSTs alert event payload as JSON to the user-configured webhook URL.
    Supports optional HMAC-SHA256 signature for verification.
    """

    async def send(self, event: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        webhook_url = rule.get("webhook_url")
        if not webhook_url:
            logger.warning("No webhook_url in rule %s", rule.get("rule_id"))
            return False

        payload = {
            "event_type":    "alert_triggered",
            "rule_id":       event.get("rule_id"),
            "user_id":       event.get("user_id"),
            "symbol":        event.get("symbol"),
            "timeframe":     event.get("timeframe"),
            "action":        event.get("action"),
            "triggered_at":  str(event.get("triggered_at", "")),
            "data": {
                "close_price":    event.get("close_price"),
                "rsi_14":         event.get("rsi_14"),
                "macd":           event.get("macd"),
                "volume_ratio":   event.get("volume_ratio"),
                "candle_pattern": event.get("candle_pattern"),
            },
        }

        # Optional HMAC signature
        webhook_secret = rule.get("webhook_secret", "")
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if webhook_secret:
            import hashlib, hmac, json
            body      = json.dumps(payload).encode()
            signature = hmac.new(
                webhook_secret.encode(), body, hashlib.sha256
            ).hexdigest()
            headers["X-Crypto-Signature"] = f"sha256={signature}"

        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(webhook_url, json=payload, headers=headers)
                resp.raise_for_status()
                logger.info(
                    "Webhook delivered | url=%s rule=%s status=%d",
                    webhook_url, event.get("rule_id"), resp.status_code,
                )
                return True
        except httpx.HTTPStatusError as exc:
            logger.error("Webhook HTTP error %d: %s", exc.response.status_code, exc.response.text)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Webhook error: %s", exc)
        return False


# ─────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────
NOTIFIER_REGISTRY: Dict[str, BaseNotifier] = {
    "email":    EmailNotifier(),
    "webhook":  WebhookNotifier(),
}


def get_notifier(channel: str) -> Optional[BaseNotifier]:
    """Return the notifier instance for the given channel name."""
    notifier = NOTIFIER_REGISTRY.get(channel.lower())
    if not notifier:
        logger.warning("Unknown notification channel: %s", channel)
    return notifier
