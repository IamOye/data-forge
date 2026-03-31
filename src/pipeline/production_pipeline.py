"""
production_pipeline.py — ProductionPipeline

Full production flow for DataForge Format 1 (Kinetic) videos:
  1. Fetch daily movers via DataFetcher
  2. Fetch news context for the top mover
  3. Generate script via ScriptAdapter
  4. Generate voiceover via VoiceoverGenerator
  5. Render kinetic video via KineticRenderer
  6. Merge audio + video via ffmpeg
  7. Upload to YouTube
  8. Update DB and Google Sheet
  9. Send Telegram notification

Quota management:
  - YOUTUBE_DAILY_BUDGET = 8000 units
  - UPLOAD_COST = 1600, METADATA_COST = 50
  - Reads/writes 'youtube_units_used_today' in the settings table

Usage:
    pipeline = ProductionPipeline()
    result = pipeline.produce()
    print(result)  # {success, story_id, video_id, youtube_url, error}
"""

import sys
import os
# Ensure project root is in path so "from src.x import" works on Railway
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import base64
import json
import logging
import os
import sqlite3
import subprocess
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("DATAFORGE_DB_PATH", "data/processed/data_forge.db")
OUTPUT_DIR = Path(os.environ.get("DATAFORGE_OUTPUT_DIR", "data/output"))
RAW_DIR = Path(os.environ.get("DATAFORGE_RAW_DIR", "data/raw"))

YOUTUBE_DAILY_BUDGET = 8000
UPLOAD_COST = 1600
METADATA_COST = 50

YOUTUBE_CATEGORY_ID = "25"  # News & Politics


# ---------------------------------------------------------------------------
# ffmpeg resolver (same pattern as kinetic_renderer.py)
# ---------------------------------------------------------------------------

def _resolve_ffmpeg() -> str:
    """
    Resolve the ffmpeg binary path.
    Order: FFMPEG_BINARY env → imageio_ffmpeg → system 'ffmpeg'.
    """
    explicit = os.environ.get("FFMPEG_BINARY", "")
    if explicit and Path(explicit).exists():
        return explicit
    try:
        import imageio_ffmpeg
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path and Path(path).exists():
            return path
    except Exception:
        pass
    return "ffmpeg"


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _ensure_tables(db_path: str) -> None:
    """Create required tables if they don't exist."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS data_stories (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id      TEXT UNIQUE NOT NULL,
                story_type    TEXT NOT NULL,
                data_source   TEXT NOT NULL,
                metric_name   TEXT NOT NULL,
                current_value REAL,
                prev_value    REAL,
                pct_change    REAL,
                script        TEXT,
                hook          TEXT,
                status        TEXT DEFAULT 'QUEUED',
                created_at    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS video_log (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id          TEXT NOT NULL,
                youtube_video_id  TEXT,
                youtube_title     TEXT,
                youtube_url       TEXT,
                upload_status     TEXT DEFAULT 'PENDING',
                views_24h         INTEGER,
                uploaded_at       TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('youtube_units_used_today', '0');
            INSERT OR IGNORE INTO settings (key, value)
            VALUES ('youtube_units_reset_date', '');
        """)
        conn.commit()
    finally:
        conn.close()


def _get_setting(db_path: str, key: str) -> str:
    """Read a value from the settings table."""
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def _set_setting(db_path: str, key: str, value: str) -> None:
    """Write a value to the settings table."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


def _save_story(
    db_path: str,
    story_id: str,
    story_type: str,
    data_source: str,
    metric_name: str,
    current_value: float,
    prev_value: float,
    pct_change: float,
    script: str,
    hook: str,
) -> None:
    """Insert a new story into data_stories."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """INSERT INTO data_stories
               (story_id, story_type, data_source, metric_name,
                current_value, prev_value, pct_change, script, hook,
                status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'PRODUCING', ?)""",
            (
                story_id, story_type, data_source, metric_name,
                current_value, prev_value, pct_change, script, hook,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()
    logger.info("[dataforge] Saved story %s to DB", story_id)


def _update_story_status(db_path: str, story_id: str, status: str) -> None:
    """Update the status column of a data_stories row."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE data_stories SET status = ? WHERE story_id = ?",
            (status, story_id),
        )
        conn.commit()
    finally:
        conn.close()
    logger.info("[dataforge] Story %s status → %s", story_id, status)


def _record_quota(db_path: str, units: int) -> None:
    """Add units to the daily YouTube quota counter, resetting if date changed."""
    today_str = date.today().isoformat()
    reset_date = _get_setting(db_path, "youtube_units_reset_date")

    if reset_date != today_str:
        # New day — reset counter
        _set_setting(db_path, "youtube_units_used_today", str(units))
        _set_setting(db_path, "youtube_units_reset_date", today_str)
        logger.info("[dataforge] Quota reset for new day. Used: %d units", units)
    else:
        current = int(_get_setting(db_path, "youtube_units_used_today") or "0")
        new_total = current + units
        _set_setting(db_path, "youtube_units_used_today", str(new_total))
        logger.info("[dataforge] Quota updated: %d → %d units", current, new_total)


def _check_quota(db_path: str) -> tuple[bool, int]:
    """
    Check if there's enough YouTube quota for an upload.

    Returns:
        (has_budget, units_used_today)
    """
    today_str = date.today().isoformat()
    reset_date = _get_setting(db_path, "youtube_units_reset_date")

    if reset_date != today_str:
        return True, 0

    used = int(_get_setting(db_path, "youtube_units_used_today") or "0")
    remaining = YOUTUBE_DAILY_BUDGET - used
    has_budget = remaining >= (UPLOAD_COST + METADATA_COST)
    return has_budget, used


# ---------------------------------------------------------------------------
# YouTube upload
# ---------------------------------------------------------------------------

def _load_credentials():
    """
    Load YouTube OAuth2 credentials.

    Priority:
      1. CHARTDROP_TOKEN_B64 env var (base64-encoded JSON token)
      2. DATAFORGE_YOUTUBE_TOKEN_PATH env var (path to JSON file)

    Returns:
        google.oauth2.credentials.Credentials

    Raises:
        RuntimeError: If neither source is available.
    """
    from google.oauth2.credentials import Credentials

    # 1. Try base64-encoded token from env var
    b64 = os.environ.get("CHARTDROP_TOKEN_B64", "")
    if b64:
        token_data = json.loads(base64.b64decode(b64))
        logger.info("[dataforge] Loaded YouTube credentials from CHARTDROP_TOKEN_B64")
        return Credentials.from_authorized_user_info(token_data)

    # 2. Fall back to file path
    token_path = os.environ.get("DATAFORGE_YOUTUBE_TOKEN_PATH", "")
    if token_path and Path(token_path).exists():
        logger.info("[dataforge] Loaded YouTube credentials from %s", token_path)
        return Credentials.from_authorized_user_file(token_path)

    raise RuntimeError(
        "YouTube credentials not available. "
        "Set CHARTDROP_TOKEN_B64 (base64 JSON) or "
        "DATAFORGE_YOUTUBE_TOKEN_PATH (file path)."
    )


def _upload_to_youtube(
    video_path: str,
    title: str,
    description: str,
    tags: list[str],
) -> tuple[str, str]:
    """
    Upload a video to YouTube using the Data API v3.

    Args:
        video_path:  Path to the final MP4 file.
        title:       Video title.
        description: Video description.
        tags:        List of tags.

    Returns:
        (video_id, youtube_url)
    """
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

    creds = _load_credentials()
    youtube = build("youtube", "v3", credentials=creds)

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags,
            "categoryId": YOUTUBE_CATEGORY_ID,
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
            "containsSyntheticMedia": True,
        },
    }

    media = MediaFileUpload(video_path, mimetype="video/mp4", resumable=True)
    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = request.execute()
    video_id = response["id"]
    youtube_url = f"https://youtu.be/{video_id}"
    logger.info("[dataforge] YouTube upload complete: %s", youtube_url)
    return video_id, youtube_url


# ---------------------------------------------------------------------------
# Telegram notification
# ---------------------------------------------------------------------------

def _send_telegram(message: str) -> None:
    """Send a Telegram message using the HTTP API (works in any thread)."""
    try:
        import requests
        bot_token = os.environ.get("DATAFORGE_TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("DATAFORGE_TELEGRAM_CHAT_ID", "")
        if not bot_token or not chat_id:
            logger.info("[dataforge] Telegram not configured (missing token or chat_id)")
            return
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        logger.info("[dataforge] Telegram notification sent")
    except Exception as e:
        logger.warning("[dataforge] Telegram notification failed: %s", e)


# ---------------------------------------------------------------------------
# Merge audio + video
# ---------------------------------------------------------------------------

def _merge_audio_video(
    video_path: str, audio_path: str, output_path: str
) -> str:
    """
    Merge video (no audio) and audio into a single MP4 using ffmpeg.
    Uses imageio_ffmpeg to resolve the binary (same pattern as kinetic_renderer.py).

    Returns:
        Path to the merged output file.
    """
    ffmpeg_bin = _resolve_ffmpeg()
    logger.info("[dataforge] Merging audio+video with ffmpeg: %s", ffmpeg_bin)

    cmd = [
        ffmpeg_bin, "-y",
        "-i", video_path,
        "-i", audio_path,
        "-c:v", "copy",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg merge failed:\n{result.stderr[-500:]}"
        )
    logger.info("[dataforge] Merged output: %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# ProductionPipeline
# ---------------------------------------------------------------------------

class ProductionPipeline:
    """
    Full production pipeline for DataForge Format 1 (Kinetic) videos.

    Flow:
      fetch movers → news context → script → voiceover → render →
      merge → upload → DB updates → GSheet → Telegram
    """

    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        _ensure_tables(self.db_path)

    def produce(self) -> dict[str, Any]:
        """
        Run the full production pipeline for one video.

        Returns:
            Dict with keys: success, story_id, video_id, youtube_url, error
        """
        story_id = f"df_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

        result: dict[str, Any] = {
            "success": False,
            "story_id": story_id,
            "video_id": None,
            "youtube_url": None,
            "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = (
                    f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET} units used today"
                )
                logger.warning("[dataforge] %s", result["error"])
                return result

            logger.info(
                "[dataforge] Quota OK: %d/%d units used",
                units_used, YOUTUBE_DAILY_BUDGET,
            )

            # --- Step 1: Fetch daily movers ---
            logger.info("[dataforge] Step 1: Fetching daily movers...")
            try:
                from src.data.data_fetcher import DataFetcher

                fetcher = DataFetcher()
                movers = fetcher.fetch_daily_movers(top_n=5)
            except Exception as e:
                logger.error("[dataforge] Step 1 failed: %s", e, exc_info=True)
                result["error"] = f"Step 1 fetch_daily_movers failed: {e}"
                return result

            if not movers:
                result["error"] = "No movers returned from DataFetcher (yfinance + polygon both empty)"
                logger.error("[dataforge] %s", result["error"])
                return result

            top_mover = movers[0]
            metric_name = top_mover.metric_name
            current_value = top_mover.current_value
            prev_value = top_mover.prev_value
            pct_change = top_mover.pct_change
            data_source = top_mover.data_source

            logger.info(
                "[dataforge] Step 1 complete: source=%s, top_mover=%s (%.2f -> %.2f, %+.2f%%), %d total movers",
                data_source, metric_name, prev_value, current_value, pct_change, len(movers),
            )

            # --- Step 2: Fetch news context ---
            logger.info("[dataforge] Step 2: Fetching news context for '%s'...", metric_name)
            try:
                news_context = fetcher.fetch_news_context(metric_name, max_results=3)
                news_headlines = [item if isinstance(item, str) else str(item) for item in news_context]
                logger.info("[dataforge] Step 2 complete: %d headlines fetched", len(news_headlines))
            except Exception as e:
                logger.error("[dataforge] Step 2 failed: %s", e, exc_info=True)
                news_headlines = []
                logger.warning("[dataforge] Step 2: continuing with 0 headlines")

            # --- Step 3: Generate script ---
            logger.info("[dataforge] Step 3: Generating script via ScriptAdapter...")
            try:
                from src.content.script_adapter import ScriptAdapter

                script_adapter = ScriptAdapter()
                script_result = script_adapter.generate(
                    metric_name=metric_name,
                    current_value=current_value,
                    prev_value=prev_value,
                    pct_change=pct_change,
                    data_source=data_source,
                    news_context=news_headlines,
                    story_type="kinetic",
                )
            except Exception as e:
                logger.error("[dataforge] Step 3 failed: %s", e, exc_info=True)
                result["error"] = f"Step 3 script generation failed: {e}"
                return result

            if not script_result.is_valid:
                result["error"] = f"Script validation failed: {script_result.validation_errors}"
                logger.error("[dataforge] Step 3 validation failed: %s", result["error"])
                return result

            logger.info(
                "[dataforge] Step 3 complete: %d words, valid=%s, hook='%s'",
                script_result.word_count, script_result.is_valid, script_result.hook[:80],
            )

            # --- Step 4: Save story to DB ---
            logger.info("[dataforge] Step 4: Saving story to DB...")
            try:
                _save_story(
                    db_path=self.db_path,
                    story_id=story_id,
                    story_type="kinetic",
                    data_source=data_source,
                    metric_name=metric_name,
                    current_value=current_value,
                    prev_value=prev_value,
                    pct_change=pct_change,
                    script=script_result.full_script,
                    hook=script_result.hook,
                )
                logger.info("[dataforge] Step 4 complete: story %s saved", story_id)
            except Exception as e:
                logger.error("[dataforge] Step 4 failed: %s", e, exc_info=True)
                result["error"] = f"Step 4 DB save failed: {e}"
                return result

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] Step 5: Generating voiceover via ElevenLabs...")
            try:
                from src.media.voiceover import VoiceoverGenerator

                vo_gen = VoiceoverGenerator(output_dir=RAW_DIR)
                vo_result = vo_gen.generate(
                    script_dict=script_result.to_dict(),
                    topic_id=story_id,
                    category="money",
                )
            except Exception as e:
                logger.error("[dataforge] Step 5 failed: %s", e, exc_info=True)
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Step 5 voiceover failed: {e}"
                return result

            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover validation failed: {vo_result.validation_errors}"
                logger.error("[dataforge] Step 5 validation failed: %s", result["error"])
                return result

            logger.info(
                "[dataforge] Step 5 complete: %.1fs duration, path=%s",
                vo_result.duration_seconds, vo_result.audio_path,
            )

            # --- Step 6: Render kinetic video ---
            logger.info("[dataforge] Step 6: Rendering kinetic video...")
            try:
                from src.media.kinetic_renderer import KineticRenderer

                renderer = KineticRenderer(output_dir=RAW_DIR)
                video_duration = vo_result.duration_seconds + 2.0
                video_path = renderer.render(
                    value=current_value,
                    prev_value=prev_value,
                    label=metric_name,
                    currency=top_mover.currency if hasattr(top_mover, "currency") else "$",
                    duration_sec=video_duration,
                    story_id=story_id,
                    source_credit=f"Source: {data_source}",
                )
                logger.info(
                    "[dataforge] Step 6 complete: %.1fs, path=%s",
                    video_duration, video_path,
                )
            except Exception as e:
                logger.error("[dataforge] Step 6 failed: %s", e, exc_info=True)
                _update_story_status(self.db_path, story_id, "RENDER_FAILED")
                result["error"] = f"Step 6 render failed: {e}"
                return result

            # --- Step 7: Merge audio + video ---
            logger.info("[dataforge] Step 7: Merging audio + video...")
            try:
                OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
                final_path = str(OUTPUT_DIR / f"{story_id}_final.mp4")
                _merge_audio_video(video_path, vo_result.audio_path, final_path)
                logger.info("[dataforge] Step 7 complete: %s", final_path)
            except Exception as e:
                logger.error("[dataforge] Step 7 failed: %s", e, exc_info=True)
                _update_story_status(self.db_path, story_id, "MERGE_FAILED")
                result["error"] = f"Step 7 merge failed: {e}"
                return result

            # --- Step 8: Upload to YouTube ---
            logger.info("[dataforge] Step 8: Uploading to YouTube...")
            try:
                video_title = script_result.hook[:100]
                video_description = (
                    f"{script_result.full_script}\n\n"
                    f"Source: {data_source}\n"
                    f"#data #finance #shorts"
                )
                video_tags = ["data", "finance", "shorts", metric_name.lower()]

                video_id, youtube_url = _upload_to_youtube(
                    video_path=final_path,
                    title=video_title,
                    description=video_description,
                    tags=video_tags,
                )
                logger.info("[dataforge] Step 8 complete: video_id=%s, url=%s", video_id, youtube_url)
            except Exception as e:
                logger.error("[dataforge] Step 8 failed: %s", e, exc_info=True)
                _update_story_status(self.db_path, story_id, "UPLOAD_FAILED")
                result["error"] = f"Step 8 upload failed: {e}"
                _send_telegram(f"DataForge UPLOAD FAILED\nStory: {story_id}\nError: {e}")
                return result

            # --- Step 9: Record quota ---
            logger.info("[dataforge] Step 9: Recording quota...")
            try:
                _record_quota(self.db_path, UPLOAD_COST + METADATA_COST)
                logger.info("[dataforge] Step 9 complete")
            except Exception as e:
                logger.error("[dataforge] Step 9 failed: %s", e, exc_info=True)

            # --- Step 10: Update DB ---
            logger.info("[dataforge] Step 10: Updating DB records...")
            try:
                _update_story_status(self.db_path, story_id, "UPLOADED")
                conn = sqlite3.connect(self.db_path)
                try:
                    conn.execute(
                        """INSERT INTO video_log
                           (story_id, youtube_video_id, youtube_title, youtube_url,
                            upload_status, uploaded_at)
                           VALUES (?, ?, ?, ?, 'UPLOADED', ?)""",
                        (
                            story_id, video_id, video_title, youtube_url,
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
                logger.info("[dataforge] Step 10 complete")
            except Exception as e:
                logger.error("[dataforge] Step 10 failed: %s", e, exc_info=True)

            # --- Step 11: GSheet sync ---
            logger.info("[dataforge] Step 11: GSheet sync...")
            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.mark_uploaded(story_id, video_id, youtube_url)
                logger.info("[dataforge] Step 11 complete")
            except Exception as e:
                logger.warning("[dataforge] Step 11 GSheet sync failed (non-fatal): %s", e)

            # --- Step 12: Telegram notification ---
            logger.info("[dataforge] Step 12: Sending Telegram notification...")
            _send_telegram(
                f"DataForge upload complete\n"
                f"Story: {story_id}\n"
                f"Metric: {metric_name}\n"
                f"URL: {youtube_url}"
            )
            logger.info("[dataforge] Step 12 complete")

            # --- Done ---
            result["success"] = True
            result["video_id"] = video_id
            result["youtube_url"] = youtube_url
            logger.info(
                "[dataforge] Production complete: %s -> %s (all 12 steps passed)",
                story_id, youtube_url,
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Pipeline crashed unexpectedly: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge PIPELINE CRASHED\nStory: {story_id}\nError: {e}")

        return result


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_pipeline() -> None:
    """
    Smoke test — checks quota, prints story_id sample and output dirs.
    No actual upload or API calls.
    """
    logging.basicConfig(level=logging.INFO)

    print("\n[dataforge] Testing ProductionPipeline...")

    # Ensure DB and tables exist
    db_path = DB_PATH
    _ensure_tables(db_path)
    print(f"  DB path:    {db_path}")
    print(f"  OUTPUT_DIR: {OUTPUT_DIR}")
    print(f"  RAW_DIR:    {RAW_DIR}")

    # Check quota
    has_budget, used = _check_quota(db_path)
    print(f"\n  Quota check:")
    print(f"    Budget:     {YOUTUBE_DAILY_BUDGET} units/day")
    print(f"    Upload:     {UPLOAD_COST} units")
    print(f"    Metadata:   {METADATA_COST} units")
    print(f"    Used today: {used} units")
    print(f"    Has budget: {has_budget}")

    # Sample story_id
    sample_id = f"df_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    print(f"\n  Sample story_id: {sample_id}")

    # Verify output dirs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Output dirs created/verified.")

    # Verify ffmpeg resolution
    ffmpeg = _resolve_ffmpeg()
    print(f"  ffmpeg binary: {ffmpeg}")

    print("\n[dataforge] ProductionPipeline test complete (no upload performed).")


if __name__ == "__main__":
    test_pipeline()
