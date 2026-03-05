"""
sheets.py — Google Sheets read/write layer.

Column layout (1-indexed):
  A=Name  B=Handle  C=Platform  D=Followers  E=QT  F=Tweet  G=Longform
  H=Article  I=Language  J=Location  K=Tags  L=Contact  M=Notes
  N=Niche  O=Last Scanned  P=Link Status
"""

import os
import logging
import re
from typing import Optional
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices (0-based)
COL = {
    "name":         0,   # A
    "handle":       1,   # B
    "platform":     2,   # C
    "followers":    3,   # D
    "qt":           4,   # E
    "tweet":        5,   # F
    "longform":     6,   # G
    "article":      7,   # H
    "language":     8,   # I
    "location":     9,   # J
    "tags":         10,  # K
    "contact":      11,  # L
    "notes":        12,  # M
    "niche":        13,  # N
    "last_scanned": 14,  # O
    "link_status":  15,  # P
}

HEADERS = [
    "Name", "Handle", "Platform", "Followers",
    "QT", "Tweet", "Longform", "Article",
    "Language", "Location", "Tags", "Contact",
    "Notes", "Niche", "Last Scanned", "Link Status",
]

NUM_COLS = len(HEADERS)  # 16


class SheetsClient:
    def __init__(self):
        import json
from google.oauth2.service_account import Credentials

creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
if creds_json:
    creds_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
else:
    creds = Credentials.from_service_account_file(
        os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json"),
        scopes=SCOPES,
    )
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self._spreadsheet_id = os.environ["SPREADSHEET_ID"]
        self._sheet_name = os.environ.get("SHEET_NAME", "Sheet1")
        self._range_prefix = f"{self._sheet_name}!"

    # ─── Low-level helpers ───────────────────────────────────────────────────

    def _range(self, start: str, end: str) -> str:
        return f"{self._range_prefix}{start}:{end}"

    def _get(self, range_: str) -> list[list]:
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._spreadsheet_id, range=range_)
            .execute()
        )
        return result.get("values", [])

    def _update(self, range_: str, values: list[list]) -> None:
        self._service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

    # ─── Public API ──────────────────────────────────────────────────────────

    def ensure_headers(self) -> None:
        """Write header row if the first row is missing or wrong."""
        values = self._get(self._range("A1", "P1"))
        if not values or values[0] != HEADERS:
            self._update(self._range("A1", "P1"), [HEADERS])
            logger.info("Headers written/updated.")

    def get_all_rows(self) -> list[dict]:
        """Return all data rows (skip header) as list of dicts."""
        values = self._get(self._range("A1", "P"))
        if not values:
            return []

        rows = []
        for i, row in enumerate(values[1:], start=2):  # row_num is 1-indexed sheet row
            # Pad row to full width
            padded = row + [""] * (NUM_COLS - len(row))
            rows.append({"_row": i, **{k: padded[v] for k, v in COL.items()}})
        return rows

    def get_row(self, row_num: int) -> dict:
        """Fetch a single row by 1-indexed row number."""
        values = self._get(f"{self._range_prefix}A{row_num}:P{row_num}")
        if not values:
            return {}
        padded = values[0] + [""] * (NUM_COLS - len(values[0]))
        return {"_row": row_num, **{k: padded[v] for k, v in COL.items()}}

    def update_row_fields(self, row_num: int, fields: dict) -> None:
        """
        Update specific columns in a row without overwriting the rest.
        fields: {column_name: value, …}
        """
        # Fetch current row first
        current = self.get_row(row_num)
        padded = [""] * NUM_COLS
        for k, idx in COL.items():
            padded[idx] = current.get(k, "")

        # Apply updates
        for field, value in fields.items():
            if field in COL:
                padded[COL[field]] = value if value is not None else ""

        col_letter_end = chr(ord("A") + NUM_COLS - 1)  # "P"
        self._update(f"{self._range_prefix}A{row_num}:{col_letter_end}{row_num}", [padded])

    def extract_hyperlink_from_name(self, row_num: int) -> Optional[str]:
        """
        Use the spreadsheet API to read the rich-text hyperlink embedded
        in the Name cell (column A).
        """
        try:
            result = (
                self._service.spreadsheets()
                .get(
                    spreadsheetId=self._spreadsheet_id,
                    ranges=[f"{self._range_prefix}A{row_num}"],
                    includeGridData=True,
                )
                .execute()
            )
            sheets = result.get("sheets", [])
            if not sheets:
                return None
            row_data = sheets[0].get("data", [{}])[0].get("rowData", [{}])
            if not row_data:
                return None
            cell = row_data[0].get("values", [{}])[0] if row_data[0].get("values") else {}
            # Check hyperlink in userEnteredFormat or hyperlink field
            hyperlink = cell.get("hyperlink")
            if hyperlink:
                return hyperlink
            # Check textFormatRuns for links
            for run in cell.get("textFormatRuns", []):
                link = run.get("format", {}).get("link", {}).get("uri")
                if link:
                    return link
            return None
        except HttpError as e:
            logger.warning(f"Failed to get hyperlink for row {row_num}: {e}")
            return None

    def get_all_hyperlinks(self) -> dict[int, Optional[str]]:
        """
        Batch-fetch all hyperlinks from column A.
        Returns {row_num: url_or_none}.
        """
        try:
            result = (
                self._service.spreadsheets()
                .get(
                    spreadsheetId=self._spreadsheet_id,
                    ranges=[f"{self._range_prefix}A2:A"],
                    includeGridData=True,
                )
                .execute()
            )
            links = {}
            sheets = result.get("sheets", [])
            if not sheets:
                return links

            row_data = sheets[0].get("data", [{}])[0].get("rowData", [])
            for i, row in enumerate(row_data, start=2):
                values = row.get("values", [{}])
                cell = values[0] if values else {}
                link = cell.get("hyperlink")
                if not link:
                    for run in cell.get("textFormatRuns", []):
                        link = run.get("format", {}).get("link", {}).get("uri")
                        if link:
                            break
                links[i] = link
            return links
        except HttpError as e:
            logger.error(f"Failed to batch-fetch hyperlinks: {e}")
            return {}

    def get_row_count(self) -> int:
        values = self._get(self._range("A2", "A"))
        return len(values)
