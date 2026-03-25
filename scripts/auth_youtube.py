"""
auth_youtube.py — YouTube OAuth2 authorisation helper

Runs the OAuth flow for a channel and saves the token to
.credentials/{channel_key}_token.json in the format expected by YouTubeUploader.

Usage:
    python scripts/auth_youtube.py --channel money_debate
    python scripts/auth_youtube.py               # defaults to money_debate
"""

print("Starting OAuth flow...")

import sys
import traceback

try:
    import argparse
    import json
    from pathlib import Path

    print("Imports OK")

    # ---------------------------------------------------------------------------
    # Config
    # ---------------------------------------------------------------------------

    SCOPES = [
        "https://www.googleapis.com/auth/youtube",
        "https://www.googleapis.com/auth/youtube.force-ssl",
        "https://www.googleapis.com/auth/youtube.upload",
        "https://www.googleapis.com/auth/youtube.readonly",
        "https://www.googleapis.com/auth/youtubepartner",
        "https://www.googleapis.com/auth/yt-analytics.readonly",
    ]
    CREDENTIALS_DIR = Path(".credentials")

    # ---------------------------------------------------------------------------
    # Argument parsing
    # ---------------------------------------------------------------------------

    parser = argparse.ArgumentParser(description="Authorise a YouTube channel via OAuth2")
    parser.add_argument(
        "--channel",
        default="money_debate",
        help="Channel key (e.g. money_debate). Used to locate the client secret file "
             "and name the output token file.",
    )
    args = parser.parse_args()
    channel_key = args.channel

    print(f"Channel key: {channel_key}")

    # ---------------------------------------------------------------------------
    # Locate client secret file
    # Accepts both  {key}_client_secret.json  and  {key}_client_secret.json.json
    # ---------------------------------------------------------------------------

    candidates = [
        CREDENTIALS_DIR / f"{channel_key}_client_secret.json",
        CREDENTIALS_DIR / f"{channel_key}_client_secret.json.json",
    ]
    client_secret_path = next((p for p in candidates if p.exists()), None)

    if client_secret_path is None:
        print(
            f"\nERROR: No client secret file found. Looked for:\n"
            + "\n".join(f"  {p}" for p in candidates)
        )
        sys.exit(1)

    print(f"Client secret file: {client_secret_path}")

    # ---------------------------------------------------------------------------
    # Run OAuth flow
    # ---------------------------------------------------------------------------

    print("Importing google_auth_oauthlib...")
    from google_auth_oauthlib.flow import InstalledAppFlow
    print("Import OK")

    print("Building OAuth flow from client secret...")
    flow = InstalledAppFlow.from_client_secrets_file(
        str(client_secret_path),
        scopes=SCOPES,
    )
    print("Flow built")

    print("\nOpening browser for Google sign-in...")
    print("(If the browser does not open automatically, copy the URL printed below.)\n")
    credentials = flow.run_local_server(port=0, open_browser=True)
    print("\nAuthorisation complete!")

    # ---------------------------------------------------------------------------
    # Save token in YouTubeUploader-compatible format
    # ---------------------------------------------------------------------------

    token_path = CREDENTIALS_DIR / f"{channel_key}_token.json"
    token_data = {
        "token":         credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri":     credentials.token_uri,
        "client_id":     credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes":        list(credentials.scopes) if credentials.scopes else SCOPES,
    }

    CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
    with open(token_path, "w") as f:
        json.dump(token_data, f, indent=2)

    print(f"Token saved to: {token_path}")
    print(f"\nDone! You can now upload videos and harvest analytics for channel '{channel_key}'.")
    print("\nScopes authorised:")
    for s in SCOPES:
        print(f"  - {s}")

except Exception:
    print("\n--- ERROR ---")
    traceback.print_exc()
    print("-------------")
    sys.exit(1)
