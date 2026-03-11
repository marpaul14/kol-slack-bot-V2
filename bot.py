"""
KOL Slack Bot — Main entry point

Commands:
- /scanall: Scrapes all KOLs using Apify, analyzes 5 posts each, caches results
- /scannew: Only scans rows missing Handle, Language, Location, or Niche (cost-effective!)
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
             f"• Analyzing 5 recent posts per KOL\n"
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
        text=f"🔍 <@{user}> started *Scan Incomplete Rows*.\n"
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
                    f"✅ *Scan Complete!*\n"
                    f"• Scanned: {result['scanned']}\n"
                    f"• Updated: {result['updated']}\n"
                    f"• Skipped (already complete): {result.get('skipped_complete', 0)}\n"
                    f"• Skipped (no link): {result.get('skipped_no_link', 0)}\n"
                    f"• Errors: {result['errors']}\n\n"
                    f"_Use `/findkol <niche>` to search the database._"
                ),
            )
        except Exception as e:
            logger.exception("scannew failed")
            client.chat_postMessage(channel=channel, text=f"❌ Scan failed: {e}")

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

            # Format as compact code blocks (splits if >30 results)
            messages = _format_kol_results(results, query)
            for msg in messages:
                client.chat_postMessage(channel=channel, text=msg)
            
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
        f"• `/scannew` — Only scan rows missing data (saves cost!)"
    )


# ─────────────────────────────────────────────
# Format /findkol results as compact code block
# ─────────────────────────────────────────────
MAX_RESULTS_PER_MESSAGE = 30  # Slack message limit ~4000 chars

def _format_kol_results(results: list, query: str, page: int = 1) -> list:
    """
    Format KOL results as compact code blocks.
    Returns a list of messages (splits if too many results).
    """
    if not results:
        return [f"😕 No KOLs found matching: *{query}*"]
    
    messages = []
    total = len(results)
    
    # Split results into chunks
    for i in range(0, total, MAX_RESULTS_PER_MESSAGE):
        chunk = results[i:i + MAX_RESULTS_PER_MESSAGE]
        chunk_start = i + 1
        chunk_end = min(i + MAX_RESULTS_PER_MESSAGE, total)
        
        lines = []
        
        # Header only on first message
        if i == 0:
            lines.append(f"🎯 Found *{total}* KOL(s) for \"{query}\"")
            lines.append("")
        else:
            lines.append(f"📄 Results {chunk_start}-{chunk_end} of {total}")
            lines.append("")
        
        lines.append("```")
        lines.append(f"{'Name':<20} {'Handle':<18} {'Niche':<40} {'Lang':<8} {'Loc':<12}")
        lines.append("-" * 100)
        
        for kol in chunk:
            name = (kol.get("name") or "N/A")[:19]
            handle = (kol.get("handle") or "N/A")[:17]
            niche = (kol.get("niche") or "N/A")[:39]
            lang = (kol.get("language") or "N/A")[:7]
            location = (kol.get("location") or "N/A")[:11]
            
            lines.append(f"{name:<20} {handle:<18} {niche:<40} {lang:<8} {location:<12}")
        
        lines.append("```")
        
        messages.append("\n".join(lines))
    
    return messages


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
