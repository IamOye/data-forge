#!/usr/bin/env python3
"""
quick_add.py — Quick Topic Add for ChannelForge

Add a topic you already thought of directly to the Google Sheet
with a Claude-generated hook angle.

Usage:
    python tools/quick_add.py "Why your pension will not be enough" money
    python tools/quick_add.py "The salary negotiation move HR hates" career
    python tools/quick_add.py "Why saving money keeps you broke"

Requires: pip install gspread google-auth anthropic
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

load_dotenv(_PROJECT_ROOT / ".env")


def get_hook(title: str, category: str) -> str:
    """Ask Claude Haiku for a one-line hook angle."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return ""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100,
            messages=[{
                "role": "user",
                "content": (
                    f"Write a single punchy hook sentence for this YouTube Shorts topic. "
                    f"Category: {category}. Topic: {title}\n\n"
                    f"Return ONLY the hook sentence, nothing else."
                ),
            }],
        )
        return message.content[0].text.strip()
    except Exception as exc:
        print(f"  Hook generation failed: {exc}")
        return ""


def add_to_sheet(title: str, category: str, hook: str) -> int:
    """Append topic to Google Sheet Topic Queue tab. Returns SEQ number."""
    import gspread
    from google.oauth2.service_account import Credentials
    import base64

    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in .env")

    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_B64", "")
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_b64:
        _b64 = creds_b64.strip()
        _missing = len(_b64) % 4
        if _missing:
            _b64 += "=" * (4 - _missing)
        creds_json = json.loads(base64.b64decode(_b64))
        creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    elif creds_file:
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    else:
        raise ValueError(
            "Set GOOGLE_CREDENTIALS_B64 or GOOGLE_CREDENTIALS_FILE in .env"
        )

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)

    try:
        ws = spreadsheet.worksheet("Topic Queue")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet("Topic Queue", rows=500, cols=10)
        ws.update("A1:G1", [["SEQ", "Title", "Category", "Status",
                              "Date Added", "Hook Angle", "Notes"]])
        ws.format("A1:G1", {"textFormat": {"bold": True}})

    # Get next SEQ
    col_a = ws.col_values(1)
    nums = []
    for v in col_a[1:]:
        try:
            nums.append(int(v))
        except (ValueError, TypeError):
            pass
    seq = max(nums, default=0) + 1

    today = datetime.now().strftime("%Y-%m-%d")
    ws.append_row(
        [seq, title, category, "READY", today, hook, "manual add"],
        value_input_option="USER_ENTERED",
    )

    return seq


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python tools/quick_add.py \"Your topic title\" [category]")
        print("  category: money (default), career, success")
        sys.exit(1)

    title = sys.argv[1].strip()
    category = sys.argv[2].strip().lower() if len(sys.argv) > 2 else "money"

    if category not in ("money", "career", "success"):
        print(f"Invalid category '{category}'. Use: money, career, success")
        sys.exit(1)

    print(f"  Title:    {title}")
    print(f"  Category: {category}")
    print()

    # Get hook from Claude
    print("  Getting hook angle from Claude...")
    hook = get_hook(title, category)
    if hook:
        print(f"  Hook: {hook}")
    print()

    # Add to sheet
    try:
        seq = add_to_sheet(title, category, hook)
        print(f"  Added SEQ #{seq}: {title}")
    except Exception as exc:
        print(f"  Error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
