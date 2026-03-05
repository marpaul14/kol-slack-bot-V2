"""
KOL Slack Bot — Main entry point
Handles Slack slash commands and events.
"""

import os
import logging
import threading
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv

from kol_engine import KOLEngine

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = App(token=os.environ["SLACK_BOT_TOKEN"])
engine = KOLEngine()

# ─────────────────────────────────────────────
# /scanall  — scrape every name row in the sheet
# ─────────────────────────────────────────────
@app.command("/scanall")
def handle_scanall(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    user    = command["user_id"]

    client.chat_postMessage(
        channel=channel,
        text=f"🔍 <@{user}> triggered *Scan All*. Starting full scan — I'll update you when done.",
    )

    def run():
        try:
            result = engine.scan_all(progress_callback=lambda msg: client.chat_postMessage(channel=channel, text=msg))
            client.chat_postMessage(
                channel=channel,
                text=(
                    f"✅ *Scan All complete!*\n"
                    f"• Scanned: {result['scanned']}\n"
                    f"• Updated: {result['updated']}\n"
                    f"• Skipped (cached): {result['cached']}\n"
                    f"• Errors: {result['errors']}"
                ),
            )
        except Exception as e:
            logger.exception("scanall failed")
            client.chat_postMessage(channel=channel, text=f"❌ Scan All failed: {e}")

    threading.Thread(target=run, daemon=True).start()


# ─────────────────────────────────────────────
# /findkol <query>  — find matching KOLs
# ─────────────────────────────────────────────
@app.command("/findkol")
def handle_findkol(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    user    = command["user_id"]
    query   = command.get("text", "").strip()

    if not query:
        client.chat_postMessage(
            channel=channel,
            text="⚠️ Usage: `/findkol <niche> [platform] [language] [location]`\nExample: `/findkol crypto X english philippines`",
        )
        return

    client.chat_postMessage(
        channel=channel,
        text=f"🔎 <@{user}> is searching for KOLs matching: *{query}*…",
    )

    def run():
        try:
            results = engine.find_kol(query)
            if not results:
                client.chat_postMessage(channel=channel, text="😕 No matching KOLs found.")
                return

            blocks = _build_kol_blocks(results, query)
            client.chat_postMessage(channel=channel, blocks=blocks, text=f"Found {len(results)} KOL(s)")
        except Exception as e:
            logger.exception("findkol failed")
            client.chat_postMessage(channel=channel, text=f"❌ Find KOL failed: {e}")

    threading.Thread(target=run, daemon=True).start()


# ─────────────────────────────────────────────
# /kolstatus  — show cache stats
# ─────────────────────────────────────────────
@app.command("/kolstatus")
def handle_status(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    stats   = engine.get_status()
    client.chat_postMessage(
        channel=channel,
        text=(
            f"📊 *KOL Cache Status*\n"
            f"• Total rows in sheet: {stats['total_rows']}\n"
            f"• Cached / scanned: {stats['cached']}\n"
            f"• Never scanned: {stats['unscanned']}\n"
            f"• Last full scan: {stats['last_scan'] or 'Never'}"
        ),
    )


# ─────────────────────────────────────────────
# Block builder helpers
# ─────────────────────────────────────────────
def _build_kol_blocks(results: list[dict], query: str) -> list:
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🎯 KOL Results for \"{query}\""},
        },
        {"type": "divider"},
    ]

    for kol in results[:10]:  # Slack block limit
        platform_emoji = {"X": "🐦", "TikTok": "🎵", "YouTube": "▶️", "Instagram": "📸"}.get(kol.get("platform", ""), "🌐")
        rates = _format_rates(kol)

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{platform_emoji} *{kol.get('name', 'N/A')}* — `{kol.get('handle', 'N/A')}`\n"
                        f"📍 {kol.get('location', '—')}  |  🌐 {kol.get('language', '—')}  |  👥 {kol.get('followers', '—')}\n"
                        f"🏷 Niche: {kol.get('niche', '—')}  |  🏷 Tags: {kol.get('tags', '—')}\n"
                        f"{rates}"
                    ),
                },
            }
        )
        blocks.append({"type": "divider"})

    if len(results) > 10:
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"_{len(results) - 10} more results not shown. Refine your query._"}],
            }
        )

    return blocks


def _format_rates(kol: dict) -> str:
    platform = kol.get("platform", "")
    parts = []

    if platform == "X":
        for key, label in [("qt", "QT"), ("tweet", "Tweet"), ("longform", "Longform"), ("article", "Article")]:
            val = kol.get(key)
            if val:
                parts.append(f"{label}: {val}")
    else:
        val = kol.get("video_rate") or kol.get("qt")
        if val:
            parts.append(f"Video Rate: {val}")

    return "💰 " + "  |  ".join(parts) if parts else ""


if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    logger.info("KOL Bot starting…")
    handler.start()
