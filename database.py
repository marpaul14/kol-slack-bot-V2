"""
database.py — SQLite-backed cache for scraped KOL data.

All scraped results are stored here so /findkol and subsequent runs
never pay the scraping cost twice. Cache is only refreshed when
/scanall is explicitly called.
"""

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


# ─── Cache read/write ─────────────────────────────────────────────────────────

def get_cached(row_num: int) -> Optional[dict]:
    with _connect() as conn:
        row = conn.execute(
            "SELECT * FROM kol_cache WHERE row_num = ?", (row_num,)
        ).fetchone()
    return dict(row) if row else None


def upsert(row_num: int, data: dict) -> None:
    data["row_num"] = row_num
    data.setdefault("scanned_at", datetime.utcnow().isoformat())

    cols   = list(data.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names    = ", ".join(cols)
    updates      = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "row_num")

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


def get_all_cached() -> list[dict]:
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM kol_cache").fetchall()
    return [dict(r) for r in rows]


def get_cached_row_nums() -> set[int]:
    with _connect() as conn:
        rows = conn.execute("SELECT row_num FROM kol_cache").fetchall()
    return {r["row_num"] for r in rows}


# ─── Meta helpers ─────────────────────────────────────────────────────────────

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


# ─── Search ──────────────────────────────────────────────────────────────────

def search_kols(
    niche: Optional[str] = None,
    platform: Optional[str] = None,
    language: Optional[str] = None,
    location: Optional[str] = None,
) -> list[dict]:
    conditions = []
    params: list = []

    if niche:
        conditions.append("(LOWER(niche) LIKE ? OR LOWER(tags) LIKE ?)")
        params += [f"%{niche.lower()}%", f"%{niche.lower()}%"]
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
    return [dict(r) for r in rows]
