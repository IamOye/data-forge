"""
gsheet_sync.py — GSheetSync

Manages the 'Data Queue' tab in a shared Google Sheet for DataForge story tracking.
Adapted from ChannelForge gsheet_topic_sync.py.

Key differences from ChannelForge:
  - Writes ONLY to the 'Data Queue' tab (never touches ChannelForge tabs)
  - Headers: Story ID, Metric, Format, Status, Hook, YouTube ID, YouTube URL,
             Views 24h, Uploaded At, Source
  - Creates the 'Data Queue' tab automatically if it doesn't exist

Usage:
    sync = GSheetSync()
    sync.write_story({"story_id": "df_001", "metric": "Apple Market Cap", ...})
    sync.mark_uploaded("df_001", "abc123", "https://youtu.be/abc123")
    sync.update_views("df_001", 12345)
    pending = sync.get_pending_stories()
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TAB_NAME = "Data Queue"
HEADERS = [
    "Story ID", "Metric", "Format", "Status", "Hook",
    "YouTube ID", "YouTube URL", "Views 24h", "Uploaded At", "Source",
]


# ---------------------------------------------------------------------------
# GSheetSync
# ---------------------------------------------------------------------------

class GSheetSync:
    """
    Syncs DataForge story data to/from a Google Sheet 'Data Queue' tab.

    Requires:
      - GSHEET_ID env var (the spreadsheet ID)
      - GSHEET_SERVICE_ACCOUNT_JSON env var (path to service account JSON)
    """

    def __init__(
        self,
        spreadsheet_id: str | None = None,
        credentials_path: str | None = None,
    ) -> None:
        self.spreadsheet_id = spreadsheet_id or os.environ.get("GSHEET_ID", "")
        self.credentials_path = credentials_path or os.environ.get(
            "GSHEET_SERVICE_ACCOUNT_JSON", ""
        )
        self._sheet = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _connect(self) -> Any:
        """Connect to Google Sheets and return the worksheet for 'Data Queue'."""
        if self._sheet is not None:
            return self._sheet

        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise RuntimeError(
                "gspread and google-auth required. "
                "Run: pip install gspread google-auth"
            )

        if not self.spreadsheet_id:
            raise ValueError("GSHEET_ID env var not set")
        if not self.credentials_path:
            raise ValueError("GSHEET_SERVICE_ACCOUNT_JSON env var not set")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]

        # Resolve credentials: base64-encoded JSON or file path
        import base64 as _b64
        import tempfile as _tmp

        creds_file = self.credentials_path
        if len(self.credentials_path) > 500 or not os.path.exists(self.credentials_path):
            # Likely base64-encoded JSON, not a file path
            try:
                sa_json = _b64.b64decode(self.credentials_path).decode('utf-8')
                tf = _tmp.NamedTemporaryFile(
                    mode='w', suffix='.json', delete=False,
                )
                tf.write(sa_json)
                tf.close()
                creds_file = tf.name
                logger.info("[dataforge] GSheet credentials decoded from base64 to %s", creds_file)
            except Exception as e:
                logger.error("[dataforge] Failed to decode GSHEET_SERVICE_ACCOUNT_JSON: %s", e)
                raise

        creds = Credentials.from_service_account_file(
            creds_file, scopes=scopes
        )
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(self.spreadsheet_id)

        # Create tab if it doesn't exist
        try:
            self._sheet = spreadsheet.worksheet(TAB_NAME)
            logger.info("[dataforge] Connected to existing '%s' tab", TAB_NAME)
        except gspread.exceptions.WorksheetNotFound:
            logger.info("[dataforge] '%s' tab not found — creating it", TAB_NAME)
            self._sheet = spreadsheet.add_worksheet(
                title=TAB_NAME, rows=1000, cols=len(HEADERS)
            )
            self._sheet.append_row(HEADERS)
            logger.info("[dataforge] Created '%s' tab with headers", TAB_NAME)

        return self._sheet

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_story(self, story_dict: dict[str, Any]) -> None:
        """
        Write a story row to the Data Queue tab.

        Args:
            story_dict: Dict with keys matching header columns (case-insensitive).
                        At minimum: story_id, metric, format, status.
        """
        sheet = self._connect()
        row = [
            str(story_dict.get("story_id", "")),
            str(story_dict.get("metric", "")),
            str(story_dict.get("format", "kinetic")),
            str(story_dict.get("status", "QUEUED")),
            str(story_dict.get("hook", "")),
            str(story_dict.get("youtube_id", "")),
            str(story_dict.get("youtube_url", "")),
            str(story_dict.get("views_24h", "")),
            str(story_dict.get("uploaded_at", "")),
            str(story_dict.get("source", "")),
        ]
        sheet.append_row(row)
        logger.info(
            "[dataforge] Wrote story %s to '%s' tab",
            story_dict.get("story_id", "?"), TAB_NAME,
        )

    def mark_uploaded(
        self, story_id: str, video_id: str, youtube_url: str
    ) -> None:
        """
        Update a story row to reflect a successful YouTube upload.

        Args:
            story_id:    The story ID to find and update.
            video_id:    YouTube video ID.
            youtube_url: Full YouTube URL.
        """
        sheet = self._connect()
        cell = sheet.find(story_id, in_column=1)
        if cell is None:
            logger.warning(
                "[dataforge] Story %s not found in sheet — cannot mark uploaded",
                story_id,
            )
            return

        row_num = cell.row
        now_str = datetime.now(timezone.utc).isoformat()
        # Update: Status=UPLOADED, YouTube ID, YouTube URL, Uploaded At
        sheet.update_cell(row_num, 4, "UPLOADED")
        sheet.update_cell(row_num, 6, video_id)
        sheet.update_cell(row_num, 7, youtube_url)
        sheet.update_cell(row_num, 9, now_str)
        logger.info(
            "[dataforge] Marked story %s as UPLOADED (video_id=%s)",
            story_id, video_id,
        )

    def update_views(self, story_id: str, views_24h: int) -> None:
        """
        Update the 24-hour view count for a story.

        Args:
            story_id:  The story ID to find and update.
            views_24h: View count to record.
        """
        sheet = self._connect()
        cell = sheet.find(story_id, in_column=1)
        if cell is None:
            logger.warning(
                "[dataforge] Story %s not found in sheet — cannot update views",
                story_id,
            )
            return

        sheet.update_cell(cell.row, 8, str(views_24h))
        logger.info(
            "[dataforge] Updated views for %s: %d", story_id, views_24h
        )

    def get_pending_stories(self) -> list[dict[str, str]]:
        """
        Return all rows with Status == 'QUEUED' as list of dicts.

        Returns:
            List of dicts with keys matching HEADERS.
        """
        sheet = self._connect()
        all_rows = sheet.get_all_records()
        pending = [
            row for row in all_rows
            if str(row.get("Status", "")).upper() == "QUEUED"
        ]
        logger.info(
            "[dataforge] Found %d pending stories in '%s' tab",
            len(pending), TAB_NAME,
        )
        return pending


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_gsheet_sync() -> None:
    """Connect to the Google Sheet and write one test row."""
    logging.basicConfig(level=logging.INFO)

    print("\n[dataforge] Testing GSheetSync...")

    gsheet_id = os.environ.get("GSHEET_ID", "")
    creds_path = os.environ.get("GSHEET_SERVICE_ACCOUNT_JSON", "")

    if not gsheet_id or not creds_path:
        print("  GSHEET_ID or GSHEET_SERVICE_ACCOUNT_JSON not set — skipping live test")
        print("  (Set these env vars to run the full integration test)")
        print("\n[dataforge] GSheetSync test complete (dry run).")
        return

    sync = GSheetSync()

    test_story = {
        "story_id": "df_test_001",
        "metric": "Apple Market Cap",
        "format": "kinetic",
        "status": "QUEUED",
        "hook": "Apple just lost two hundred billion dollars in market cap.",
        "source": "Yahoo Finance",
    }

    try:
        sync.write_story(test_story)
        print(f"  Wrote test story: {test_story['story_id']}")

        pending = sync.get_pending_stories()
        print(f"  Pending stories: {len(pending)}")

        print("\n[dataforge] GSheetSync test complete.")
    except Exception as e:
        print(f"  Error: {e}")
        print("\n[dataforge] GSheetSync test failed.")


if __name__ == "__main__":
    test_gsheet_sync()
