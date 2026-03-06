"""
kol_engine.py — Orchestrates scan_all and find_kol operations.

COLUMN BEHAVIOR:
  - Bot ONLY writes to: B (Handle), D (Followers), I (Language), J (Location), N (Niche), O (Last Scanned), P (Link Status)
  - Bot NEVER modifies: A (Name), C (Platform), E-H (Rates), K-M (Tags/Contact/Notes)

COST EFFICIENCY:
  - /scanall: Scrapes using Apify, analyzes with AI, caches to database
  - /findkol: ONLY queries database (no scraping, no AI) — instant & free!
"""

import time
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

import database as db
import scraper as sc
import ai_analyzer as ai
from sheets import SheetsClient

logger = logging.getLogger(__name__)

PROGRESS_BATCH = 10  # Update progress every 10 rows


class KOLEngine:
    def __init__(self):
        db.init_db()
        self._sheets = SheetsClient()
        self._sheets.ensure_headers()

    def scan_all(self, progress_callback: Optional[Callable] = None) -> dict:
        """
        Scan all rows in the sheet:
        1. Extract links from Name column (column A)
        2. Scrape profiles + 5 recent posts using Apify
        3. Analyze posts with AI to determine niche
        4. Save to database (cache)
        5. Write to sheet (ONLY columns B, D, I, J, N, O, P)
        
        This is the ONLY command that scrapes and calls AI.
        """
        stats = {"scanned": 0, "updated": 0, "cached": 0, "errors": 0}
        rows = self._sheets.get_all_rows()
        all_links = self._sheets.get_all_hyperlinks()
        total = len(rows)
        
        logger.info(f"[scan_all] Starting scan of {total} rows")
        logger.info(f"[scan_all] Found {len([l for l in all_links.values() if l])} rows with links")
        
        if not rows:
            return stats

        for i, row in enumerate(rows, start=1):
            row_num = row["_row"]
            url = all_links.get(row_num)
            name = row.get("name", f"Row {row_num}")
            
            if not url:
                logger.debug(f"[scan_all] Row {row_num}: No link")
                self._sheets.update_row_fields(row_num, {"link_status": "No Link"})
                continue
                
            try:
                logger.info(f"[scan_all] Row {row_num}: {name}")
                
                # Step 1: Scrape profile + recent posts using Apify
                profile = sc.scrape_profile(url)
                recent_posts = profile.get("recent_posts", [])
                
                logger.info(f"[scan_all] Scraped: followers={profile.get('followers')}, "
                           f"posts={len(recent_posts)}, status={profile.get('link_status')}")
                
                # Step 2: Analyze with AI to determine niche
                enriched = {}
                if profile.get("link_status") in ("OK", "Limited"):
                    enriched = ai.analyze_profile(
                        platform=row.get("platform", ""),  # Use existing platform (read-only)
                        followers=profile.get("followers", "Unknown"),
                        bio=profile.get("raw_bio", ""),
                        location=profile.get("location", ""),
                        handle=profile.get("handle", ""),
                        recent_posts=recent_posts,
                    )
                    logger.info(f"[scan_all] AI: niche={enriched.get('niche')}, "
                               f"language={enriched.get('language')}")
                
                # Step 3: Prepare fields (ONLY the ones we're allowed to write!)
                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                
                # These are the ONLY fields we write (B, D, I, J, N, O, P)
                fields = {
                    "handle":       profile.get("handle", "") or row.get("handle", ""),
                    "followers":    profile.get("followers", "") or row.get("followers", ""),
                    "language":     enriched.get("language", "") or row.get("language", ""),
                    "location":     enriched.get("location", "") or profile.get("location", "") or row.get("location", ""),
                    "niche":        enriched.get("niche", "") or row.get("niche", ""),
                    "last_scanned": now,
                    "link_status":  profile.get("link_status", "OK"),
                }
                
                # Step 4: Write to sheet (sheets.py ensures only allowed columns)
                self._sheets.update_row_fields(row_num, fields)
                
                # Step 5: Save to database cache (includes all data for searching)
                db.upsert(row_num, {
                    **fields,
                    "name": name,
                    "platform": row.get("platform", ""),  # From sheet (read-only)
                    "qt": row.get("qt", ""),              # From sheet (read-only)
                    "tweet": row.get("tweet", ""),        # From sheet (read-only)
                    "longform": row.get("longform", ""),  # From sheet (read-only)
                    "article": row.get("article", ""),    # From sheet (read-only)
                    "tags": row.get("tags", ""),          # From sheet (read-only)
                    "contact": row.get("contact", ""),    # From sheet (read-only)
                    "notes": row.get("notes", ""),        # From sheet (read-only)
                    "profile_url": url,
                    "raw_bio": profile.get("raw_bio", ""),
                })
                
                stats["scanned"] += 1
                stats["updated"] += 1
                
            except Exception as e:
                logger.exception(f"[scan_all] Error on row {row_num}: {e}")
                self._sheets.update_row_fields(row_num, {"link_status": "Error"})
                stats["errors"] += 1

            # Progress update (private to user who triggered)
            if progress_callback and i % PROGRESS_BATCH == 0:
                pct = int(i / total * 100)
                progress_callback(f"⏳ Scanning… {i}/{total} ({pct}%) — last: {name}")
            
            # Rate limiting (Apify + Google Sheets)
            time.sleep(1.5)

        db.set_meta("last_scan", datetime.now(timezone.utc).isoformat())
        logger.info(f"[scan_all] Complete: {stats}")
        return stats

    def find_kol(self, query: str) -> list:
        """
        Find KOLs matching a query.
        
        COST EFFECTIVE: This ONLY queries the database!
        - No scraping
        - No AI calls (except lightweight query parsing)
        - Instant results
        
        Users should run /scanall first to populate the database.
        """
        logger.info(f"[find_kol] Query: {query}")
        
        # Parse query into filters
        filters = ai.parse_find_query(query)
        logger.info(f"[find_kol] Filters: {filters}")
        
        # Search database ONLY (no scraping!)
        results = db.search_kols(
            niche=filters.get("niche"),
            platform=filters.get("platform"),
            language=filters.get("language"),
            location=filters.get("location"),
        )
        
        logger.info(f"[find_kol] Found {len(results)} matches")
        return results

    def get_status(self) -> dict:
        """Get cache statistics."""
        total_rows = self._sheets.get_row_count()
        cached_nums = db.get_cached_row_nums()
        return {
            "total_rows": total_rows,
            "cached": len(cached_nums),
            "unscanned": max(0, total_rows - len(cached_nums)),
            "last_scan": db.get_meta("last_scan"),
        }
