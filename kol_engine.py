"""
kol_engine.py — Orchestrates scan_all and find_kol operations.

COLUMN BEHAVIOR:
  - Bot ONLY writes to: B (Handle), D (Followers), I (Language), J (Location), N (Niche), O (Last Scanned), P (Link Status)
  - Bot NEVER modifies: A (Name), C (Platform), E-H (Rates), K-M (Tags/Contact/Notes)

DATA SOURCE:
  - The Google Sheet is the single source of truth.
  - /findkol reads directly from the sheet — no database needed.
"""

import re
import time
import logging
from datetime import datetime, timezone
from typing import Callable, Optional

import scraper as sc
import ai_analyzer as ai
from sheets import SheetsClient

logger = logging.getLogger(__name__)

PROGRESS_BATCH = 10  # Update progress every 10 rows


# ─── Rate filter helpers (used by /findkol) ──────────────────────────────────

def _parse_rate_filter(value: str) -> dict:
    """Parse a rate filter value into min/max bounds.

    Formats: "300-500", ">300", "<500", "300"
    """
    value = value.strip()
    if "-" in value and not value.startswith(">") and not value.startswith("<"):
        parts = value.split("-", 1)
        try:
            return {"min": float(parts[0]), "max": float(parts[1])}
        except ValueError:
            return {"min": None, "max": None}
    if value.startswith(">"):
        try:
            return {"min": float(value[1:]), "max": None}
        except ValueError:
            return {"min": None, "max": None}
    if value.startswith("<"):
        try:
            return {"min": None, "max": float(value[1:])}
        except ValueError:
            return {"min": None, "max": None}
    try:
        v = float(value)
        return {"min": v, "max": v}
    except ValueError:
        return {"min": None, "max": None}


def _extract_numeric(text: str) -> Optional[float]:
    """Extract the first numeric value from a text field like '$300', '300 USD', etc."""
    if not text:
        return None
    cleaned = text.replace(",", "")
    m = re.search(r'[\d]+\.?\d*', cleaned)
    return float(m.group()) if m else None


def _matches_rate(row_value: str, filter_value: str) -> bool:
    """Check if a row's rate value matches the filter range."""
    num = _extract_numeric(row_value)
    if num is None:
        return False
    bounds = _parse_rate_filter(filter_value)
    # If filter couldn't be parsed at all, match nothing (not everything)
    if bounds["min"] is None and bounds["max"] is None:
        return False
    if bounds["min"] is not None and num < bounds["min"]:
        return False
    if bounds["max"] is not None and num > bounds["max"]:
        return False
    return True


def _search_rows(rows: list, filters: dict) -> list:
    """
    Filter sheet rows by the parsed filters dict.

    Text filters (niche, platform, language, location) use case-insensitive substring matching.
    Numeric filters (qt, tweet, longform, article, followers) use range matching.
    """
    results = list(rows)

    # --- Text filters ---

    # Niche: use expanded synonym terms if available
    niche_terms = filters.get("niche_terms")
    niche = filters.get("niche")
    if niche_terms and len(niche_terms) > 1:
        def matches_niche(row):
            name_lower = (row.get("name") or "").lower()
            niche_lower = (row.get("niche") or "").lower()
            tags_lower = (row.get("tags") or "").lower()
            return any(
                term.lower() in niche_lower or term.lower() in tags_lower or term.lower() in name_lower
                for term in niche_terms
            )
        results = [r for r in results if matches_niche(r)]
    elif niche:
        niche_lower = niche.lower()
        results = [r for r in results if (
            niche_lower in (r.get("niche") or "").lower()
            or niche_lower in (r.get("tags") or "").lower()
            or niche_lower in (r.get("name") or "").lower()
        )]

    if filters.get("platform"):
        platform_lower = filters["platform"].lower()
        results = [r for r in results if platform_lower in (r.get("platform") or "").lower()]

    if filters.get("language"):
        lang_lower = filters["language"].lower()
        results = [r for r in results if lang_lower in (r.get("language") or "").lower()]

    if filters.get("location"):
        loc_lower = filters["location"].lower()
        results = [r for r in results if loc_lower in (r.get("location") or "").lower()]

    # --- Numeric filters ---
    rate_filters = {
        "qt": filters.get("qt_rate"),
        "tweet": filters.get("tweet_rate"),
        "longform": filters.get("longform_rate"),
        "article": filters.get("article_rate"),
        "followers": filters.get("followers"),
    }

    for col, filter_val in rate_filters.items():
        if filter_val:
            results = [r for r in results if _matches_rate(r.get(col, ""), filter_val)]

    return results


# ─── Main Engine ──────────────────────────────────────────────────────────────

class KOLEngine:
    def __init__(self):
        self._sheets = SheetsClient()
        self._sheets.ensure_headers()

    def scan_all(self, progress_callback: Optional[Callable] = None) -> dict:
        """
        Scan all rows in the sheet:
        1. Extract links from Name column (column A)
        2. Scrape profiles using Apify
        3. Analyze with AI to determine niche
        4. Write results to sheet (ONLY columns B, D, I, J, N, O, P)
        """
        stats = {"scanned": 0, "updated": 0, "errors": 0}
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

                # Step 1: Scrape profile using Apify
                profile = sc.scrape_profile(url)
                recent_posts = profile.get("recent_posts", [])

                logger.info(f"[scan_all] Scraped: followers={profile.get('followers')}, "
                           f"posts={len(recent_posts)}, status={profile.get('link_status')}")

                # Step 2: Analyze with AI to determine niche
                enriched = {}
                if profile.get("link_status") in ("OK", "Limited"):
                    enriched = ai.analyze_profile(
                        platform=row.get("platform", ""),
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

                fields = {
                    "handle":       profile.get("handle", "") or row.get("handle", ""),
                    "followers":    profile.get("followers", "") or row.get("followers", ""),
                    "language":     enriched.get("language", "") or row.get("language", ""),
                    "location":     enriched.get("location", "") or profile.get("location", "") or row.get("location", ""),
                    "niche":        enriched.get("niche", "") or row.get("niche", ""),
                    "last_scanned": now,
                    "link_status":  profile.get("link_status", "OK"),
                }

                # Step 4: Write to sheet
                self._sheets.update_row_fields(row_num, fields)

                stats["scanned"] += 1
                stats["updated"] += 1

            except Exception as e:
                logger.exception(f"[scan_all] Error on row {row_num}: {e}")
                self._sheets.update_row_fields(row_num, {"link_status": "Error"})
                stats["errors"] += 1

            # Progress update (private to user who triggered)
            if progress_callback and i % PROGRESS_BATCH == 0:
                pct = int(i / total * 100)
                progress_callback(f"\u23f3 Scanning\u2026 {i}/{total} ({pct}%) \u2014 last: {name}")

            # Rate limiting (Apify + Google Sheets)
            time.sleep(1.5)

        logger.info(f"[scan_all] Complete: {stats}")
        return stats

    def find_kol(self, query: str) -> tuple:
        """
        Find KOLs matching a query. Returns (results, filters).

        Reads directly from the Google Sheet — no database needed.
        """
        logger.info(f"[find_kol] Query: {query}")

        # Parse query into filters
        filters = ai.parse_find_query(query)
        logger.info(f"[find_kol] Filters: {filters}")

        # Search sheet directly
        rows = self._sheets.get_all_rows()
        results = _search_rows(rows, filters)

        logger.info(f"[find_kol] Found {len(results)} matches")
        return results, filters

    def get_status(self) -> dict:
        """Get sheet statistics."""
        rows = self._sheets.get_all_rows()
        total = len(rows)
        incomplete = sum(1 for r in rows if self._is_row_incomplete(r))
        scanned = sum(1 for r in rows if (r.get("last_scanned") or "").strip())
        last_scanned_dates = [r.get("last_scanned", "") for r in rows
                              if (r.get("last_scanned") or "").strip()]
        last_scan = max(last_scanned_dates) if last_scanned_dates else None
        return {
            "total_rows": total,
            "scanned": scanned,
            "unscanned": max(0, total - scanned),
            "incomplete": incomplete,
            "last_scan": last_scan,
        }

    def _count_incomplete_rows(self) -> int:
        """Count rows missing handle, language, location, or niche."""
        rows = self._sheets.get_all_rows()
        count = 0
        for row in rows:
            if self._is_row_incomplete(row):
                count += 1
        return count

    def _is_row_incomplete(self, row: dict) -> bool:
        """Check if row is missing ANY of: Handle, Language, Location, Niche."""
        handle = (row.get("handle") or "").strip()
        language = (row.get("language") or "").strip()
        location = (row.get("location") or "").strip()
        niche = (row.get("niche") or "").strip()
        return not handle or not language or not location or not niche

    def scan_incomplete(self, progress_callback: Optional[Callable] = None) -> dict:
        """
        Scan ONLY rows missing data in: Handle, Language, Location, or Niche.
        More cost-effective than /scanall when most rows are already scanned.
        """
        stats = {"scanned": 0, "updated": 0, "skipped_complete": 0, "skipped_no_link": 0, "errors": 0}
        rows = self._sheets.get_all_rows()
        all_links = self._sheets.get_all_hyperlinks()

        # Filter to only incomplete rows
        incomplete_rows = [(i, row) for i, row in enumerate(rows, start=1)
                          if self._is_row_incomplete(row)]

        total_all = len(rows)
        total_incomplete = len(incomplete_rows)
        stats["skipped_complete"] = total_all - total_incomplete

        logger.info(f"[scan_incomplete] Found {total_incomplete} incomplete rows out of {total_all} total")

        if not incomplete_rows:
            if progress_callback:
                progress_callback("\u2705 All rows already have data! Nothing to scan.")
            return stats

        for idx, (i, row) in enumerate(incomplete_rows, start=1):
            row_num = row["_row"]
            url = all_links.get(row_num)
            name = row.get("name", f"Row {row_num}")

            if not url:
                logger.debug(f"[scan_incomplete] Row {row_num}: No link")
                self._sheets.update_row_fields(row_num, {"link_status": "No Link"})
                stats["skipped_no_link"] += 1
                continue

            try:
                logger.info(f"[scan_incomplete] Row {row_num}: {name}")

                # Step 1: Scrape profile using Apify
                profile = sc.scrape_profile(url)
                recent_posts = profile.get("recent_posts", [])

                logger.info(f"[scan_incomplete] Scraped: followers={profile.get('followers')}, "
                           f"posts={len(recent_posts)}, status={profile.get('link_status')}")

                # Step 2: Analyze with AI to determine niche
                enriched = {}
                if profile.get("link_status") in ("OK", "Limited"):
                    enriched = ai.analyze_profile(
                        platform=row.get("platform", ""),
                        followers=profile.get("followers", "Unknown"),
                        bio=profile.get("raw_bio", ""),
                        location=profile.get("location", ""),
                        handle=profile.get("handle", ""),
                        recent_posts=recent_posts,
                    )
                    logger.info(f"[scan_incomplete] AI: niche={enriched.get('niche')}, "
                               f"language={enriched.get('language')}")

                # Step 3: Prepare fields
                now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

                fields = {
                    "handle":       profile.get("handle", "") or row.get("handle", ""),
                    "followers":    profile.get("followers", "") or row.get("followers", ""),
                    "language":     enriched.get("language", "") or row.get("language", ""),
                    "location":     enriched.get("location", "") or profile.get("location", "") or row.get("location", ""),
                    "niche":        enriched.get("niche", "") or row.get("niche", ""),
                    "last_scanned": now,
                    "link_status":  profile.get("link_status", "OK"),
                }

                # Step 4: Write to sheet
                self._sheets.update_row_fields(row_num, fields)

                stats["scanned"] += 1
                stats["updated"] += 1

            except Exception as e:
                logger.exception(f"[scan_incomplete] Error on row {row_num}: {e}")
                self._sheets.update_row_fields(row_num, {"link_status": "Error"})
                stats["errors"] += 1

            # Progress update
            if progress_callback and idx % PROGRESS_BATCH == 0:
                pct = int(idx / total_incomplete * 100)
                progress_callback(f"\u23f3 Scanning incomplete\u2026 {idx}/{total_incomplete} ({pct}%) \u2014 last: {name}")

            # Rate limiting
            time.sleep(1.5)

        logger.info(f"[scan_incomplete] Complete: {stats}")
        return stats
