"""
KOL Slack Bot — Main entry point

Commands:
- /scanall: Scrapes all KOLs using Apify, analyzes 10 posts each, caches results
- /scannew: Only scans rows missing Handle, Language, or Niche (cost-effective!)
- /findkol <query>: Searches cached database (no scraping = cost effective!)
- /kolstatus: Shows cache statistics
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


def send_private(client, channel: str, user: str, text: str):
    """Send ephemeral message - only the specified user sees it."""
    try:
        client.chat_postEphemeral(channel=channel, user=user, text=text)
    except Exception as e:
        logger.warning(f"Failed to send private message: {e}")


# ─────────────────────────────────────────────
# /scanall — Scrape all KOLs using Apify
# ─────────────────────────────────────────────
@app.command("/scanall")
def handle_scanall(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    user = command["user_id"]

    logger.info(f"[/scanall] Triggered by {user}")

    # Public announcement
    client.chat_postMessage(
        channel=channel,
        text=f"🔍 <@{user}> started *Scan All*.\n"
             f"• Scraping profiles via Apify\n"
             f"• Analyzing 10 recent posts per KOL\n"
             f"• Results will be posted when complete.",
    )

    def run():
        try:
            # Progress updates are PRIVATE
            def progress(msg):
                logger.info(f"[/scanall] {msg}")
                send_private(client, channel, user, msg)

            result = engine.scan_all(progress_callback=progress)
            
            logger.info(f"[/scanall] Complete: {result}")
            
            # Final result is PUBLIC
            client.chat_postMessage(
                channel=channel,
                text=(
                    f"✅ *Scan All Complete!*\n"
                    f"• Scanned: {result['scanned']}\n"
                    f"• Updated: {result['updated']}\n"
                    f"• Errors: {result['errors']}\n\n"
                    f"_Use `/findkol <niche>` to search the database._"
                ),
            )
        except Exception as e:
            logger.exception("scanall failed")
            client.chat_postMessage(channel=channel, text=f"❌ Scan All failed: {e}")

    threading.Thread(target=run, daemon=True).start()


# ─────────────────────────────────────────────
# /scannew — Only scan rows missing data
# ─────────────────────────────────────────────
@app.command("/scannew")
def handle_scannew(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    user = command["user_id"]

    logger.info(f"[/scannew] Triggered by {user}")

    # Public announcement
    client.chat_postMessage(
        channel=channel,
        text=f"🔍 <@{user}> started *Scan New/Incomplete*.\n"
             f"• Only scanning rows missing Handle, Language, Location, or Niche\n"
             f"• Skipping rows that already have all data\n"
             f"• Results will be posted when complete.",
    )

    def run():
        try:
            # Progress updates are PRIVATE
            def progress(msg):
                logger.info(f"[/scannew] {msg}")
                send_private(client, channel, user, msg)

            result = engine.scan_incomplete(progress_callback=progress)
            
            logger.info(f"[/scannew] Complete: {result}")
            
            # Final result is PUBLIC
            client.chat_postMessage(
                channel=channel,
                text=(
                    f"✅ *Scan Incomplete Complete!*\n"
                    f"• Scanned: {result['scanned']}\n"
                    f"• Updated: {result['updated']}\n"
                    f"• Skipped (no link): {result['skipped']}\n"
                    f"• Errors: {result['errors']}\n\n"
                    f"_Use `/findkol <niche>` to search the database._"
                ),
            )
        except Exception as e:
            logger.exception("scannew failed")
            client.chat_postMessage(channel=channel, text=f"❌ Scan New failed: {e}")

    threading.Thread(target=run, daemon=True).start()


# ─────────────────────────────────────────────
# /findkol <query> — Search cached database only
# ─────────────────────────────────────────────
@app.command("/findkol")
def handle_findkol(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    user = command["user_id"]
    query = command.get("text", "").strip()

    logger.info(f"[/findkol] Query: '{query}' by {user}")

    if not query:
        send_private(client, channel, user,
            "⚠️ *Usage:* `/findkol <query>`\n\n"
            "*Examples:*\n"
            "• `/findkol crypto` — Find crypto KOLs\n"
            "• `/findkol defi philippines` — DeFi KOLs from PH\n"
            "• `/findkol gaming english` — Gaming KOLs in English\n"
            "• `/findkol nft X` — NFT KOLs on X/Twitter\n\n"
            "_💡 Run `/scanall` first to populate the database._"
        )
        return

    def run():
        try:
            results = engine.find_kol(query)
            logger.info(f"[/findkol] Found {len(results)} results")
            
            if not results:
                client.chat_postMessage(
                    channel=channel, 
                    text=f"😕 No KOLs found matching: *{query}*\n\n"
                         f"_Try `/scanall` first, or use different keywords._"
                )
                return

            blocks = _build_kol_blocks(results, query)
            client.chat_postMessage(channel=channel, blocks=blocks, text=f"Found {len(results)} KOL(s)")
            
        except Exception as e:
            logger.exception("findkol failed")
            client.chat_postMessage(channel=channel, text=f"❌ Find KOL failed: {e}")

    threading.Thread(target=run, daemon=True).start()


# ─────────────────────────────────────────────
# /kolstatus — Show cache statistics (private)
# ─────────────────────────────────────────────
@app.command("/kolstatus")
def handle_status(ack, say, command, client):
    ack()
    channel = command["channel_id"]
    user = command["user_id"]
    
    logger.info(f"[/kolstatus] Requested by {user}")
    
    stats = engine.get_status()
    
    send_private(client, channel, user,
        f"📊 *KOL Database Status*\n"
        f"• Total rows in sheet: {stats['total_rows']}\n"
        f"• Scanned & cached: {stats['cached']}\n"
        f"• Not yet scanned: {stats['unscanned']}\n"
        f"• Incomplete (missing Handle/Language/Location/Niche): {stats.get('incomplete', 'N/A')}\n"
        f"• Last scan: {stats['last_scan'] or 'Never'}\n\n"
        f"_Commands:_\n"
        f"• `/scanall` — Scan all rows\n"
        f"• `/scannew` — Only scan incomplete rows (saves cost!)"
    )


# ─────────────────────────────────────────────
# Block Builder for /findkol results
# ─────────────────────────────────────────────
def _build_kol_blocks(results: list, query: str) -> list:
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🎯 KOL Results for \"{query}\""},
        },
        {"type": "divider"},
    ]

    for kol in results[:10]:
        platform = kol.get("platform", "")
        platform_emoji = {
            "X": "🐦", "TikTok": "🎵", "YouTube": "▶️", "Instagram": "📸"
        }.get(platform, "🌐")
        
        # Build info line
        info_parts = []
        if kol.get("followers"):
            info_parts.append(f"👥 {kol['followers']}")
        if kol.get("niche"):
            info_parts.append(f"🏷 {kol['niche']}")
        if kol.get("language"):
            info_parts.append(f"🌐 {kol['language']}")
        
        info_line = "  |  ".join(info_parts) if info_parts else "—"
        
        # Build rates line (from manual columns)
        rates = _format_rates(kol)
        
        text = (
            f"{platform_emoji} *{kol.get('name', 'N/A')}* — `{kol.get('handle', 'N/A')}`\n"
            f"{info_line}\n"
        )
        
        if kol.get("location"):
            text += f"📍 {kol['location']}\n"
        
        if rates:
            text += f"{rates}\n"
        
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": text.strip()},
        })
        blocks.append({"type": "divider"})

    if len(results) > 10:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"_{len(results) - 10} more results not shown._"}],
        })

    return blocks


def _format_rates(kol: dict) -> str:
    """Format rate info from manual columns."""
    parts = []
    
    if kol.get("qt"):
        parts.append(f"QT: {kol['qt']}")
    if kol.get("tweet"):
        parts.append(f"Tweet: {kol['tweet']}")
    if kol.get("longform"):
        parts.append(f"Thread: {kol['longform']}")
    if kol.get("article"):
        parts.append(f"Article: {kol['article']}")
    
    return "💰 " + "  |  ".join(parts) if parts else ""


# ─────────────────────────────────────────────
# Start Bot
# ─────────────────────────────────────────────
if __name__ == "__main__":
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    logger.info("KOL Bot starting…")
    handler.start()
