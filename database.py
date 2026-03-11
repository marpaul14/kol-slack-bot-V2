"""
database.py — SQLite-backed cache for KOL data.

All scraped results are stored here so /findkol can query instantly
without needing to scrape or call AI again.
"""

import re
import sqlite3
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = "kol_cache.db"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS kol_cache (
    row_num      INTEGER PRIMARY KEY,
    name         TEXT,
    handle       TEXT,
    platform     TEXT,
    followers    TEXT,
    qt           TEXT,
    tweet        TEXT,
    longform     TEXT,
    article      TEXT,
    language     TEXT,
    location     TEXT,
    tags         TEXT,
    contact      TEXT,
    notes        TEXT,
    niche        TEXT,
    last_scanned TEXT,
    link_status  TEXT,
    profile_url  TEXT,
    raw_bio      TEXT,
    scanned_at   TEXT
);
"""

CREATE_META = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute(CREATE_TABLE)
        conn.execute(CREATE_META)
        conn.commit()
    logger.info("Database initialised.")


def get_cached(row_num: int) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM kol_cache WHERE row_num = ?", (row_num,)
        ).fetchone()
    return dict(row) if row else None


def upsert(row_num: int, data: dict) -> None:
    data["row_num"] = row_num
    data.setdefault("scanned_at", datetime.utcnow().isoformat())

    cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "row_num")

    sql = (
        f"INSERT INTO kol_cache ({col_names}) VALUES ({placeholders}) "
        f"ON CONFLICT(row_num) DO UPDATE SET {updates}"
    )
    with _connect() as conn:
        conn.execute(sql, list(data.values()))
        conn.commit()


def delete(row_num: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM kol_cache WHERE row_num = ?", (row_num,))
        conn.commit()


def get_all_cached() -> list:
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM kol_cache").fetchall()
    return [dict(r) for r in rows]


def get_cached_row_nums() -> set:
    with _connect() as conn:
        rows = conn.execute("SELECT row_num FROM kol_cache").fetchall()
    return {r["row_num"] for r in rows}


def set_meta(key: str, value: str) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, value, value),
        )
        conn.commit()


def get_meta(key: str) -> Optional[str]:
    with _connect() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


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


def search_kols(
    niche: Optional[str] = None,
    niche_terms: Optional[list] = None,
    platform: Optional[str] = None,
    language: Optional[str] = None,
    location: Optional[str] = None,
    qt_rate: Optional[str] = None,
    tweet_rate: Optional[str] = None,
    longform_rate: Optional[str] = None,
    article_rate: Optional[str] = None,
    followers: Optional[str] = None,
) -> list:
    """
    Search cached KOLs by filters.
    This is the ONLY search method - no scraping needed!

    Text filters (niche, platform, language, location) are handled in SQL.
    Numeric filters (rates, followers) are post-filtered in Python.
    """
    conditions = []
    params = []

    # Niche: use expanded synonym terms if available
    if niche_terms and len(niche_terms) > 1:
        term_conditions = []
        for term in niche_terms:
            term_conditions.append("LOWER(niche) LIKE ? OR LOWER(tags) LIKE ? OR LOWER(name) LIKE ?")
            params += [f"%{term.lower()}%", f"%{term.lower()}%", f"%{term.lower()}%"]
        conditions.append("(" + " OR ".join(term_conditions) + ")")
    elif niche:
        conditions.append("(LOWER(niche) LIKE ? OR LOWER(tags) LIKE ? OR LOWER(name) LIKE ?)")
        params += [f"%{niche.lower()}%", f"%{niche.lower()}%", f"%{niche.lower()}%"]

    if platform:
        conditions.append("LOWER(platform) LIKE ?")
        params.append(f"%{platform.lower()}%")
    if language:
        conditions.append("LOWER(language) LIKE ?")
        params.append(f"%{language.lower()}%")
    if location:
        conditions.append("LOWER(location) LIKE ?")
        params.append(f"%{location.lower()}%")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT * FROM kol_cache {where} ORDER BY row_num"

    with _connect() as conn:
        rows = conn.execute(sql, params).fetchall()

    results = [dict(r) for r in rows]

    # Post-filter for numeric fields (rates and followers)
    rate_filters = {
        "qt": qt_rate,
        "tweet": tweet_rate,
        "longform": longform_rate,
        "article": article_rate,
        "followers": followers,
    }

    for col, filter_val in rate_filters.items():
        if filter_val:
            results = [r for r in results if _matches_rate(r.get(col, ""), filter_val)]

    return results
