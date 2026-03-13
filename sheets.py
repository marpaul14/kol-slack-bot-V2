"""
sheets.py — Google Sheets read/write layer.

Column layout:
  A=Name  B=Handle  C=Platform  D=Followers  E=QT  F=Tweet  G=Longform
  H=Article  I=Language  J=Location  K=Tags  L=Contact  M=Notes
  N=Niche  O=Last Scanned  P=Link Status  Q=Cookie3 Score  R=Smart Followers

COLUMN PERMISSIONS:
  READ-ONLY (never modify): A, C, E, F, G, H, K, L, M, Q, R
  BOT WRITES: B, D, I, J, N, O, P
"""

import os
import json
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
    "name":         0,   # A - READ-ONLY
    "handle":       1,   # B - Bot writes
    "platform":     2,   # C - READ-ONLY
    "followers":    3,   # D - Bot writes
    "qt":           4,   # E - READ-ONLY
    "tweet":        5,   # F - READ-ONLY
    "longform":     6,   # G - READ-ONLY
    "article":      7,   # H - READ-ONLY
    "language":     8,   # I - Bot writes
    "location":     9,   # J - Bot writes
    "tags":         10,  # K - READ-ONLY
    "contact":      11,  # L - READ-ONLY
    "notes":        12,  # M - READ-ONLY
    "niche":           13,  # N - Bot writes
    "last_scanned":    14,  # O - Bot writes
    "link_status":     15,  # P - Bot writes
    "cookie3_score":   16,  # Q - READ-ONLY
    "smart_followers": 17,  # R - READ-ONLY
}

# Columns the bot is ALLOWED to write to
WRITABLE_COLUMNS = {"handle", "followers", "language", "location", "niche", "last_scanned", "link_status"}

HEADERS = [
    "Name", "Handle", "Platform", "Followers",
    "QT", "Tweet", "Longform", "Article",
    "Language", "Location", "Tags", "Contact",
    "Notes", "Niche", "Last Scanned", "Link Status",
    "Cookie3 Score", "Smart Followers",
]

NUM_COLS = len(HEADERS)


class SheetsClient:

    def __init__(self):
        creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if creds_json:
            logger.info("[Sheets] Using GOOGLE_CREDENTIALS_JSON env var")
            creds_info = json.loads(creds_json)
            creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        else:
            creds_file = os.environ.get("GOOGLE_CREDENTIALS_FILE", "credentials.json")
            logger.info(f"[Sheets] Using credentials file: {creds_file}")
            creds = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
            
        self._service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self._spreadsheet_id = os.environ["SPREADSHEET_ID"]
        self._sheet_name = os.environ.get("SHEET_NAME", "Sheet1")
        self._range_prefix = f"{self._sheet_name}!"
        logger.info(f"[Sheets] Connected to spreadsheet: {self._spreadsheet_id}")

    def _get(self, range_: str) -> list:
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._spreadsheet_id, range=range_)
            .execute()
        )
        return result.get("values", [])

    def _update(self, range_: str, values: list) -> None:
        self._service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range=range_,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

    def ensure_headers(self) -> None:
        """Make sure the header row exists."""
        values = self._get(f"{self._range_prefix}A1:R1")
        if not values or values[0] != HEADERS:
            self._update(f"{self._range_prefix}A1:R1", [HEADERS])
            logger.info("[Sheets] Headers written/updated.")

    def get_all_rows(self) -> list:
        """Fetch all data rows (excluding header)."""
        values = self._get(f"{self._range_prefix}A1:R")
        if not values:
            return []
        rows = []
        for i, row in enumerate(values[1:], start=2):
            padded = row + [""] * (NUM_COLS - len(row))
            rows.append({"_row": i, **{k: padded[v] for k, v in COL.items()}})
        logger.info(f"[Sheets] Fetched {len(rows)} data rows")
        return rows

    def update_row_fields(self, row_num: int, fields: dict) -> None:
        """
        Update ONLY the allowed columns (B, D, I, J, N, O, P).
        Never touches: A (Name), C (Platform), E-H (Rates), K-M (Tags/Contact/Notes)
        """
        # Filter to only writable columns
        fields = {k: v for k, v in fields.items() if k in WRITABLE_COLUMNS}
        
        if not fields:
            return

        # We need to update individual cells to avoid overwriting read-only columns
        # Build batch update request
        requests = []
        
        for field, value in fields.items():
            col_index = COL[field]
            col_letter = chr(ord('A') + col_index)
            cell_range = f"{self._range_prefix}{col_letter}{row_num}"
            
            requests.append({
                "range": cell_range,
                "values": [[value if value is not None else ""]]
            })
        
        if requests:
            self._service.spreadsheets().values().batchUpdate(
                spreadsheetId=self._spreadsheet_id,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": requests
                }
            ).execute()
            logger.debug(f"[Sheets] Updated row {row_num}: {list(fields.keys())}")

    def get_all_hyperlinks(self) -> dict:
        """
        Extract all hyperlinks from the Name column (column A).
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
            logger.info(f"[Sheets] Processing {len(row_data)} rows for hyperlinks")
            
            for i, row in enumerate(row_data, start=2):
                values = row.get("values", [{}])
                cell = values[0] if values else {}
                link = None
                
                # Method 1: Direct hyperlink property
                link = cell.get("hyperlink")
                
                # Method 2: Rich text with embedded links
                if not link:
                    for run in cell.get("textFormatRuns", []):
                        uri = run.get("format", {}).get("link", {}).get("uri")
                        if uri:
                            link = uri
                            break
                
                # Method 3: HYPERLINK formula
                if not link:
                    formula = cell.get("userEnteredValue", {}).get("formulaValue", "")
                    if formula.upper().startswith("=HYPERLINK"):
                        match = re.search(r'=HYPERLINK\s*\(\s*"([^"]+)"', formula, re.IGNORECASE)
                        if match:
                            link = match.group(1)
                
                # Method 4: Plain URL in cell
                if not link:
                    text = cell.get("effectiveValue", {}).get("stringValue", "")
                    if text.startswith("http"):
                        link = text
                
                if link:
                    links[i] = link
                    
            found_count = len([l for l in links.values() if l])
            logger.info(f"[Sheets] Found {found_count} hyperlinks")
            return links
            
        except HttpError as e:
            logger.error(f"[Sheets] Failed to fetch hyperlinks: {e}")
            return {}

    def get_row_count(self) -> int:
        """Count total data rows (excluding header)."""
        values = self._get(f"{self._range_prefix}A2:A")
        return len(values)
