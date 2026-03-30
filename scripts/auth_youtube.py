"""
auth_youtube.py -- YouTube OAuth2 authorisation helper for DataForge / ChartDrop

One-time OAuth flow to generate chartdrop_token.json for the ChartDrop
YouTube channel.  Run locally before Railway deploy.

Usage:
    python scripts/auth_youtube.py              # run OAuth flow
    python scripts/auth_youtube.py --verify     # verify existing token

Environment variables (optional overrides):
    DATAFORGE_YOUTUBE_CLIENT_SECRET_PATH  (default: config/chartdrop_client_secret.json)
    DATAFORGE_YOUTUBE_TOKEN_PATH          (default: config/chartdrop_token.json)
"""

import argparse
import json
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/youtube",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
]

CLIENT_SECRET_PATH = os.environ.get(
    "DATAFORGE_YOUTUBE_CLIENT_SECRET_PATH",
    "config/chartdrop_client_secret.json",
)

TOKEN_PATH = os.environ.get(
    "DATAFORGE_YOUTUBE_TOKEN_PATH",
    "config/chartdrop_token.json",
)


# ---------------------------------------------------------------------------
# OAuth flow
# ---------------------------------------------------------------------------

def run_oauth_flow() -> None:
    """Run the OAuth2 flow and save the token."""

    # Step 1: Check client secret exists
    if not Path(CLIENT_SECRET_PATH).exists():
        print(
            "\nERROR: Client secret file not found.\n"
            f"  Expected: {CLIENT_SECRET_PATH}\n"
            "\nTo fix this:\n"
            "  1. Go to Google Cloud Console\n"
            "     Console -> APIs & Services -> Credentials -> OAuth 2.0 Client IDs\n"
            "  2. Download the JSON client secret\n"
            "  3. Save it as: config/chartdrop_client_secret.json\n"
        )
        sys.exit(1)

    print(f"Client secret: {CLIENT_SECRET_PATH}")

    # Step 2: Build OAuth flow
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
    except ImportError:
        print(
            "\nERROR: google-auth-oauthlib not installed.\n"
            "  Run: pip install google-auth-oauthlib google-api-python-client\n"
        )
        sys.exit(1)

    print("Building OAuth flow...")
    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRET_PATH, scopes=SCOPES
    )

    # Step 3: Run local server for OAuth consent
    print("\nOpening browser for Google sign-in...")
    print("(If the browser does not open, copy the URL printed below.)\n")

    try:
        credentials = flow.run_local_server(port=0, open_browser=True)
    except Exception as e:
        print(f"\nERROR: OAuth flow failed: {e}")
        sys.exit(1)

    print("\nAuthorisation complete!")

    # Step 4: Save token
    token_data = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": list(credentials.scopes) if credentials.scopes else SCOPES,
    }

    Path(TOKEN_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(TOKEN_PATH, "w") as f:
        json.dump(token_data, f, indent=2)

    # Step 5: Confirmation
    print(f"\nToken saved to {TOKEN_PATH}")
    print("\nScopes authorised:")
    for s in SCOPES:
        print(f"  - {s}")
    print(
        f"\nNext step: upload this file to Railway volume "
        f"at /app/config/chartdrop_token.json"
    )


# ---------------------------------------------------------------------------
# Token verification
# ---------------------------------------------------------------------------

def verify_token() -> None:
    """Load the token, build YouTube service, print channel info."""

    if not Path(TOKEN_PATH).exists():
        print(f"\nERROR: Token file not found at {TOKEN_PATH}")
        print("  Run:  python scripts/auth_youtube.py  (without --verify) first.\n")
        sys.exit(1)

    print(f"Loading token from {TOKEN_PATH}...")

    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        print(
            "\nERROR: google-api-python-client not installed.\n"
            "  Run: pip install google-api-python-client google-auth\n"
        )
        sys.exit(1)

    try:
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, scopes=SCOPES)

        print("Building YouTube service...")
        youtube = build("youtube", "v3", credentials=creds)

        print("Fetching channel info...\n")
        response = (
            youtube.channels()
            .list(part="snippet", mine=True)
            .execute()
        )

        items = response.get("items", [])
        if not items:
            print("ERROR: No channels found for this token.")
            print("  The token may have expired or lack the correct scopes.")
            sys.exit(1)

        channel = items[0]
        channel_id = channel["id"]
        channel_name = channel["snippet"]["title"]

        print(f"  Channel name: {channel_name}")
        print(f"  Channel ID:   {channel_id}")
        print("\nToken is valid and working!")

    except Exception as e:
        print(f"\nERROR: Verification failed: {e}")
        print("  The token may have expired. Re-run the OAuth flow:")
        print("  python scripts/auth_youtube.py")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="YouTube OAuth2 authorisation helper for DataForge / ChartDrop"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify an existing token instead of running the OAuth flow",
    )
    args = parser.parse_args()

    if args.verify:
        verify_token()
    else:
        run_oauth_flow()


if __name__ == "__main__":
    main()
