"""
kol_engine.py — Orchestrates scan_all and find_kol operations.

scan_all:
  1. Batch-read all hyperlinks from the Name column
  2. Skip rows that are already cached (unless force=True)
  3. Scrape each new link → enrich with AI → write back to sheet & DB
  4. Update "Last Scanned" and "Link Status" columns

find_kol:
  1. Parse the query with AI
  2. Search the local DB cache
  3. Scrape + cache any Name-column rows that haven't been scanned yet
  4. Return matching results
"""

import logging
from datetime import datetime, timezone
from typing import Callable, Optional

import database as db
import scraper as sc
import ai_analyzer as ai
from sheets import SheetsClient

logger = logging.getLogger(__name__)

# Rows to process per progress update
PROGRESS_BATCH = 5


class KOLEngine:
    def __init__(self):
        db.init_db()
        self._sheets = SheetsClient()
        self._sheets.ensure_headers()

    # ─── Scan All ────────────────────────────────────────────────────────────

    def scan_all(self, progress_callback: Optional[Callable] = None) -> dict:
        """
        Full scan: scrape every row that has a link in the Name column.
        Overwrites cache for all rows (this is the refresh operation).
        Returns summary dict.
        """
        stats = {"scanned": 0, "updated": 0, "cached": 0, "errors": 0}

        rows        = self._sheets.get_all_rows()
        all_links   = self._sheets.get_all_hyperlinks()
        total       = len(rows)

        if not rows:
            return stats

        for i, row in enumerate(rows, start=1):
            row_num = row["_row"]
            url     = all_links.get(row_num)

            if not url:
                # No link — mark status
                self._sheets.update_row_fields(row_num, {"link_status": "No Link"})
                continue

            try:
                profile  = sc.scrape_profile(url)
                enriched = {}

                if profile.get("link_status") == "OK":
                    bio      = profile.get("raw_bio", "")
                    enriched = ai.analyze_profile(
                        platform  = profile.get("platform", ""),
                        followers = profile.get("followers", ""),
                        bio       = bio,
                        location  = profile.get("location") or row.get("location", ""),
                        handle    = profile.get("handle", ""),
                    )

                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                fields = {
                    "handle":       profile.get("handle", "") or row.get("handle", ""),
                    "platform":     profile.get("platform", "") or row.get("platform", ""),
                    "followers":    profile.get("followers", "") or row.get("followers", ""),
                    "language":     enriched.get("language", "") or row.get("language", ""),
                    "location":     enriched.get("location", "") or profile.get("location", "") or row.get("location", ""),
                    "niche":        enriched.get("niche", "") or row.get("niche", ""),
                    "qt":           enriched.get("qt", "") or row.get("qt", ""),
                    "tweet":        enriched.get("tweet", "") or row.get("tweet", ""),
                    "longform":     enriched.get("longform", "") or row.get("longform", ""),
                    "article":      enriched.get("article", "") or row.get("article", ""),
                    "last_scanned": now,
                    "link_status":  profile.get("link_status", "OK"),
                }

                # Write to sheet
                self._sheets.update_row_fields(row_num, fields)

                # Write to cache
                db.upsert(row_num, {
                    **fields,
                    "name":        row.get("name", ""),
                    "tags":        row.get("tags", ""),
                    "contact":     row.get("contact", ""),
                    "notes":       row.get("notes", ""),
                    "profile_url": url,
                    "raw_bio":     profile.get("raw_bio", ""),
                })

                stats["scanned"] += 1
                stats["updated"] += 1

            except Exception as e:
                logger.error(f"Error processing row {row_num}: {e}")
                self._sheets.update_row_fields(row_num, {"link_status": "Error"})
                stats["errors"] += 1

            # Progress update every N rows
            if progress_callback and i % PROGRESS_BATCH == 0:
                pct = int(i / total * 100)
                progress_callback(f"⏳ Scanning… {i}/{total} ({pct}%) — last: {row.get('name', row_num)}")

        db.set_meta("last_scan", datetime.now(timezone.utc).isoformat())
        return stats

    # ─── Find KOL ────────────────────────────────────────────────────────────

    def find_kol(self, query: str) -> list[dict]:
        """
        1. Parse query → filters
        2. Scan any rows that haven't been cached yet
        3. Search cache → return matches
        """
        # 1. Parse query
        filters = ai.parse_find_query(query)

        # 2. Scan uncached rows first (cost-efficient: only new ones)
        self._scan_uncached_rows()

        # 3. Search cache
        results = db.search_kols(
            niche    = filters.get("niche"),
            platform = filters.get("platform"),
            language = filters.get("language"),
            location = filters.get("location"),
        )

        return results

    # ─── Status ──────────────────────────────────────────────────────────────

    def get_status(self) -> dict:
        total_rows  = self._sheets.get_row_count()
        cached_nums = db.get_cached_row_nums()
        return {
            "total_rows": total_rows,
            "cached":     len(cached_nums),
            "unscanned":  max(0, total_rows - len(cached_nums)),
            "last_scan":  db.get_meta("last_scan"),
        }

    # ─── Internal helpers ────────────────────────────────────────────────────

    def _scan_uncached_rows(self) -> None:
        """Scan only rows that are not yet in the cache (light pass for /findkol)."""
        all_links    = self._sheets.get_all_hyperlinks()
        cached_nums  = db.get_cached_row_nums()
        rows         = self._sheets.get_all_rows()

        for row in rows:
            row_num = row["_row"]
            if row_num in cached_nums:
                continue  # Already cached — skip

            url = all_links.get(row_num)
            if not url:
                continue

            try:
                profile  = sc.scrape_profile(url)
                enriched = {}

                if profile.get("link_status") == "OK":
                    enriched = ai.analyze_profile(
                        platform  = profile.get("platform", ""),
                        followers = profile.get("followers", ""),
                        bio       = profile.get("raw_bio", ""),
                        location  = profile.get("location") or row.get("location", ""),
                        handle    = profile.get("handle", ""),
                    )

                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                fields = {
                    "handle":       profile.get("handle", ""),
                    "platform":     profile.get("platform", ""),
                    "followers":    profile.get("followers", ""),
                    "language":     enriched.get("language", ""),
                    "location":     enriched.get("location", "") or profile.get("location", ""),
                    "niche":        enriched.get("niche", ""),
                    "qt":           enriched.get("qt", ""),
                    "tweet":        enriched.get("tweet", ""),
                    "longform":     enriched.get("longform", ""),
                    "article":      enriched.get("article", ""),
                    "last_scanned": now,
                    "link_status":  profile.get("link_status", "OK"),
                }

                # Only write sheet columns that are empty
                update_fields = {k: v for k, v in fields.items() if not row.get(k)}
                if update_fields:
                    self._sheets.update_row_fields(row_num, update_fields)

                db.upsert(row_num, {
                    **fields,
                    "name":        row.get("name", ""),
                    "tags":        row.get("tags", ""),
                    "contact":     row.get("contact", ""),
                    "notes":       row.get("notes", ""),
                    "profile_url": url,
                    "raw_bio":     profile.get("raw_bio", ""),
                })

            except Exception as e:
                logger.warning(f"[find_kol] Could not scan row {row_num}: {e}")
