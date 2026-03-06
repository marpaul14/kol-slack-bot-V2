"""
kol_engine.py — Orchestrates scan_all and find_kol operations.
"""

import time
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

import database as db
import scraper as sc
import ai_analyzer as ai
from sheets import SheetsClient, COL, NUM_COLS

logger = logging.getLogger(__name__)

PROGRESS_BATCH = 5


class KOLEngine:
    def __init__(self):
        db.init_db()
        self._sheets = SheetsClient()
        self._sheets.ensure_headers()

    def scan_all(self, progress_callback: Optional[Callable] = None) -> dict:
        stats = {"scanned": 0, "updated": 0, "cached": 0, "errors": 0}
        rows      = self._sheets.get_all_rows()
        all_links = self._sheets.get_all_hyperlinks()
        total     = len(rows)
        if not rows:
            return stats

        for i, row in enumerate(rows, start=1):
            row_num = row["_row"]
            url     = all_links.get(row_num)
            if not url:
                self._rate_limited_write(row_num, row, {"link_status": "No Link"})
                continue
            try:
                profile  = sc.scrape_profile(url)
                enriched = {}
                if profile.get("link_status") == "OK":
                    enriched = ai.analyze_profile(
                        platform=profile.get("platform", ""),
                        followers=profile.get("followers", ""),
                        bio=profile.get("raw_bio", ""),
                        location=profile.get("location") or row.get("location", ""),
                        handle=profile.get("handle", ""),
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
                self._rate_limited_write(row_num, row, fields)
                db.upsert(row_num, {**fields, "name": row.get("name",""), "tags": row.get("tags",""),
                    "contact": row.get("contact",""), "notes": row.get("notes",""),
                    "profile_url": url, "raw_bio": profile.get("raw_bio","")})
                stats["scanned"] += 1
                stats["updated"] += 1
            except Exception as e:
                logger.error(f"Error processing row {row_num}: {e}")
                self._rate_limited_write(row_num, row, {"link_status": "Error"})
                stats["errors"] += 1

            if progress_callback and i % PROGRESS_BATCH == 0:
                pct = int(i / total * 100)
                progress_callback(f"⏳ Scanning… {i}/{total} ({pct}%) — last: {row.get('name', row_num)}")

        db.set_meta("last_scan", datetime.now(timezone.utc).isoformat())
        return stats

    def find_kol(self, query: str) -> list:
        filters = ai.parse_find_query(query)
        self._scan_uncached_rows()
        return db.search_kols(
            niche=filters.get("niche"),
            platform=filters.get("platform"),
            language=filters.get("language"),
            location=filters.get("location"),
        )

    def get_status(self) -> dict:
        total_rows  = self._sheets.get_row_count()
        cached_nums = db.get_cached_row_nums()
        return {
            "total_rows": total_rows,
            "cached":     len(cached_nums),
            "unscanned":  max(0, total_rows - len(cached_nums)),
            "last_scan":  db.get_meta("last_scan"),
        }

    def _scan_uncached_rows(self) -> None:
        """Only 2 batch API reads total — no per-row reads."""
        cached_nums = db.get_cached_row_nums()
        rows        = self._sheets.get_all_rows()        # 1 API call
        all_links   = self._sheets.get_all_hyperlinks()  # 1 API call

        uncached = [r for r in rows if r["_row"] not in cached_nums]
        if not uncached:
            return

        for row in uncached:
            row_num = row["_row"]
            url     = all_links.get(row_num)
            if not url:
                continue
            try:
                profile  = sc.scrape_profile(url)
                enriched = {}
                if profile.get("link_status") == "OK":
                    enriched = ai.analyze_profile(
                        platform=profile.get("platform", ""),
                        followers=profile.get("followers", ""),
                        bio=profile.get("raw_bio", ""),
                        location=profile.get("location") or row.get("location", ""),
                        handle=profile.get("handle", ""),
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
                db.upsert(row_num, {**fields, "name": row.get("name",""), "tags": row.get("tags",""),
                    "contact": row.get("contact",""), "notes": row.get("notes",""),
                    "profile_url": url, "raw_bio": profile.get("raw_bio","")})
                update_fields = {k: v for k, v in fields.items() if not row.get(k) and v}
                if update_fields:
                    self._rate_limited_write(row_num, row, update_fields)
            except Exception as e:
                logger.warning(f"[find_kol] Could not scan row {row_num}: {e}")

    def _rate_limited_write(self, row_num: int, current_row: dict, fields: dict) -> None:
        """Write to sheet using already-fetched row data. No extra API read."""
        try:
            padded = [""] * NUM_COLS
            for k, idx in COL.items():
                padded[idx] = current_row.get(k, "")
            for field, value in fields.items():
                if field in COL:
                    padded[COL[field]] = value if value is not None else ""
            col_end = chr(ord("A") + NUM_COLS - 1)
            self._sheets._update(
                f"{self._sheets._range_prefix}A{row_num}:{col_end}{row_num}",
                [padded],
            )
            time.sleep(1.1)  # ~55 writes/min — safely under 60/min quota
        except Exception as e:
            logger.warning(f"Sheet write failed for row {row_num}: {e}")
