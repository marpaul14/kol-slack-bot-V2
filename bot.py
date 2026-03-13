"""
KOL Slack Bot — Main entry point

Commands:
- /scanall: Scrapes all KOLs using Apify, analyzes 5 posts each, caches results
- /scannew: Only scans rows missing Handle, Language, Location, or Niche (cost-effective!)
- /findkol <query>: Searches the sheet directly (no scraping = cost effective!)
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

# Channel-based access control: comma-separated channel IDs (e.g. "C01ABC,C02DEF")
# If not set, all channels are allowed (no restriction).
ALLOWED_CHANNEL_IDS = {
    cid.strip()
    for cid in os.environ.get("ALLOWED_CHANNEL_IDS", "").split(",")
    if cid.strip()
}

if ALLOWED_CHANNEL_IDS:
    logger.info(f"Access control enabled — allowed channels: {ALLOWED_CHANNEL_IDS}")
else:
    logger.warning("ALLOWED_CHANNEL_IDS not set — bot commands are accessible from ALL channels")

app = App(token=os.environ["SLACK_BOT_TOKEN"])
engine = KOLEngine()


def check_channel_access(command, client) -> bool:
    """Return True if the command is allowed in this channel, False otherwise."""
    if not ALLOWED_CHANNEL_IDS:
        return True
    channel = command["channel_id"]
    if channel in ALLOWED_CHANNEL_IDS:
        return True
    user = command["user_id"]
    logger.warning(f"Access denied: user {user} tried command in channel {channel}")
    try:
        client.chat_postEphemeral(
            channel=channel,
            user=user,
            text="⛔ This command is not available in this channel.",
        )
    except Exception:
        pass
    return False


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
    if not check_channel_access(command, client):
        return
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
                    f"_Use `/findkol <niche>` to search._"
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
    if not check_channel_access(command, client):
        return
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
                    f"_Use `/findkol <niche>` to search._"
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
    if not check_channel_access(command, client):
        return
    channel = command["channel_id"]
    user = command["user_id"]
    query = command.get("text", "").strip()

    logger.info(f"[/findkol] Query: '{query}' by {user}")

    if not query:
        send_private(client, channel, user,
            "⚠️ *Usage:* `/findkol <query>`\n\n"
            "*Key:Value Filters:*\n"
            "• `/findkol niche:DeFi location:USA` — DeFi KOLs from USA\n"
            "• `/findkol niche:Trading platform:X qt:300-500` — Traders on X with QT rate $300-$500\n"
            "• `/findkol niche:Gaming tweet:>200` — Gaming KOLs with tweet rate above $200\n"
            "• `/findkol niche:NFT followers:>10000` — NFT KOLs with 10k+ followers\n\n"
            "*Available filters:* `niche`, `platform`, `language` (or `lang`), `location` (or `loc`), "
            "`qt` (or `qt_rate`), `tweet` (or `tweet_rate`), `longform` (or `thread`), `article`, `followers`, "
            "`cookie3` (or `c3`), `smart` (or `sf`)\n\n"
            "*Rate formats:* `300` (exact), `300-500` (range), `>300` (min), `<500` (max)\n\n"
            "*Free-text also works:*\n"
            "• `/findkol crypto` — Find crypto KOLs\n"
            "• `/findkol defi philippines` — DeFi KOLs from PH\n\n"
            "_💡 Run `/scanall` first to populate the sheet._"
        )
        return

    def run():
        try:
            results, filters = engine.find_kol(query)
            logger.info(f"[/findkol] Found {len(results)} results")

            if not results:
                client.chat_postMessage(
                    channel=channel,
                    text=f"😕 No KOLs found matching: *{query}*\n\n"
                         f"_Try `/scanall` first, or use different keywords._"
                )
                return

            # Show rates in output when rate filters are active
            show_rates = any(filters.get(k) for k in
                            ("qt_rate", "tweet_rate", "longform_rate", "article_rate"))
            show_scores = any(filters.get(k) for k in
                             ("cookie3_score", "smart_followers"))

            # Format as compact code blocks (splits if >30 results)
            messages = _format_kol_results(results, query, show_rates=show_rates, show_scores=show_scores)
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
    if not check_channel_access(command, client):
        return
    channel = command["channel_id"]
    user = command["user_id"]

    logger.info(f"[/kolstatus] Requested by {user}")
    
    stats = engine.get_status()
    
    send_private(client, channel, user,
        f"📊 *KOL Sheet Status*\n"
        f"• Total rows: {stats['total_rows']}\n"
        f"• Scanned: {stats['scanned']}\n"
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

def _format_kol_results(results: list, query: str, page: int = 1, show_rates: bool = False, show_scores: bool = False) -> list:
    """
    Format KOL results as compact code blocks.
    Returns a list of messages (splits if too many results).
    When show_rates=True, includes QT/Tweet/Thread/Article rate columns.
    When show_scores=True, includes Cookie3 Score/Smart Followers columns.
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
        score_suffix = f" {'C3':<6} {'Smart':<8}" if show_scores else ""
        if show_rates:
            lines.append(
                f"{'Name':<18} {'Handle':<16} {'Niche':<30} "
                f"{'QT':<8} {'Tweet':<8} {'Thread':<8} {'Article':<8} "
                f"{'Lang':<7} {'Loc':<10}" + score_suffix
            )
            lines.append("-" * (125 + (16 if show_scores else 0)))
        else:
            lines.append(f"{'Name':<20} {'Handle':<18} {'Niche':<40} {'Lang':<8} {'Loc':<12}" + score_suffix)
            lines.append("-" * (100 + (16 if show_scores else 0)))

        for kol in chunk:
            score_vals = ""
            if show_scores:
                c3 = (kol.get("cookie3_score") or "-")[:5]
                smart = (kol.get("smart_followers") or "-")[:7]
                score_vals = f" {c3:<6} {smart:<8}"

            if show_rates:
                name = (kol.get("name") or "N/A")[:17]
                handle = (kol.get("handle") or "N/A")[:15]
                niche = (kol.get("niche") or "N/A")[:29]
                qt = (kol.get("qt") or "-")[:7]
                tweet = (kol.get("tweet") or "-")[:7]
                longform = (kol.get("longform") or "-")[:7]
                article = (kol.get("article") or "-")[:7]
                lang = (kol.get("language") or "N/A")[:6]
                location = (kol.get("location") or "N/A")[:9]

                lines.append(
                    f"{name:<18} {handle:<16} {niche:<30} "
                    f"{qt:<8} {tweet:<8} {longform:<8} {article:<8} "
                    f"{lang:<7} {location:<10}" + score_vals
                )
            else:
                name = (kol.get("name") or "N/A")[:19]
                handle = (kol.get("handle") or "N/A")[:17]
                niche = (kol.get("niche") or "N/A")[:39]
                lang = (kol.get("language") or "N/A")[:7]
                location = (kol.get("location") or "N/A")[:11]

                lines.append(f"{name:<20} {handle:<18} {niche:<40} {lang:<8} {location:<12}" + score_vals)

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
