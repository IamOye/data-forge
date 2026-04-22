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

# Deploy: 2026-04-04 — force clean redeploy for 1080x1920 merge fix

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
            CREATE TABLE IF NOT EXISTS video_analytics (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id        TEXT NOT NULL,
                youtube_video_id TEXT NOT NULL,
                metric_name     TEXT,
                format          TEXT,
                checkpoint      TEXT NOT NULL,
                views           INTEGER DEFAULT 0,
                watch_time_mins REAL DEFAULT 0,
                likes           INTEGER DEFAULT 0,
                recorded_at     TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS script_quality (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id        TEXT NOT NULL,
                metric_name     TEXT,
                format          TEXT,
                clarity         INTEGER,
                urgency         INTEGER,
                specificity     INTEGER,
                hook_strength   INTEGER,
                avg_score       REAL,
                regenerated     INTEGER DEFAULT 0,
                word_count      INTEGER,
                recorded_at     TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS title_variants (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                story_id        TEXT NOT NULL,
                variant_1       TEXT,
                variant_2       TEXT,
                variant_3       TEXT,
                selected_title  TEXT,
                selected_rank   INTEGER,
                format          TEXT,
                recorded_at     TEXT NOT NULL
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

    merge_cmd = [
        ffmpeg_bin, "-y",
        "-i", str(video_path),
        "-i", str(audio_path),
        "-c:v", "libx264",
        "-preset", "fast",
        "-crf", "18",
        "-pix_fmt", "yuv420p",
        "-vf", "scale=1080:1920:force_original_aspect_ratio=disable",
        "-s", "1080x1920",
        "-c:a", "aac",
        "-b:a", "128k",
        "-shortest",
        str(output_path),
    ]
    logger.info("[dataforge] Merge cmd: %s", " ".join(merge_cmd))
    result = subprocess.run(merge_cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        raise RuntimeError(
            f"ffmpeg merge failed:\n{result.stderr[-500:]}"
        )

    # Probe and verify merged output resolution
    probe = subprocess.run(
        [ffmpeg_bin, "-i", str(output_path)],
        capture_output=True, text=True,
    )
    for line in probe.stderr.split("\n"):
        if "Stream" in line and "Video" in line:
            logger.info("[dataforge] Merged output probe: %s", line.strip())
            break

    logger.info("[dataforge] Merged output: %s", output_path)
    return output_path


def _generate_title_variants(
    metric_name: str,
    hook: str,
    full_script: str,
    pct_change: float,
    api_key: str,
) -> list[str]:
    """
    Generate 3 ranked YouTube title variants for a video using Claude.
    Returns list of 3 title strings, best first. Falls back to hook-based
    title on any failure.

    Title rules:
    - Max 80 characters each
    - Must include the key number or % change
    - Palki Sharma style: punchy, urgent, specific
    - No clickbait words: 'shocking', 'unbelievable', 'you won't believe'
    - Each variant tries a different angle:
        Variant 1: Person-first (human story angle)
        Variant 2: Number-first (data shock angle)
        Variant 3: Question or stakes angle
    """
    try:
        import anthropic as _anthropic
        import json as _json
        import datetime as _dt

        date_suffix = _dt.datetime.now().strftime("%b %d")

        prompt = (
            f"Generate 3 ranked YouTube Shorts title variants for this finance video.\n\n"
            f"Metric: {metric_name}\n"
            f"Change: {pct_change:+.2f}%\n"
            f"Hook: {hook}\n"
            f"Script excerpt: {full_script[:150]}\n\n"
            f"Rules:\n"
            f"- Max 80 characters each (including '| {date_suffix}' suffix which will be appended)\n"
            f"- Must include the specific % change or dollar figure\n"
            f"- Palki Sharma style: punchy, urgent, specific\n"
            f"- No words: shocking, unbelievable, incredible, insane\n"
            f"- Variant 1: person-first human story angle\n"
            f"- Variant 2: number-first data shock angle\n"
            f"- Variant 3: question or stakes angle\n"
            f"- Rank them best to worst for CTR\n\n"
            f"Respond ONLY with JSON, no markdown:\n"
            f'{{ "variants": ["title1", "title2", "title3"] }}'
        )

        client = _anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        data = _json.loads(raw)
        variants = data.get("variants", [])

        if len(variants) >= 3:
            logger.info(
                "[dataforge] Title variants generated: %s | %s | %s",
                variants[0][:40], variants[1][:40], variants[2][:40],
            )
            return variants[:3]

        logger.warning("[dataforge] _generate_title_variants: fewer than 3 variants returned")
        return []

    except Exception as e:
        logger.warning("[dataforge] _generate_title_variants failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# ProductionPipeline
# ---------------------------------------------------------------------------

class ProductionPipeline:
    """
    Full production pipeline for DataForge videos.
    Supports format routing: kinetic (Format 1) and bar_race (Format 2).

    Flow:
      fetch data → script → voiceover → render → merge → upload →
      DB updates → GSheet → Telegram
    """

    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        _ensure_tables(self.db_path)

    def produce(self, format_type: str = "kinetic") -> dict[str, Any]:
        """
        Route to the correct format-specific production method.

        Args:
            format_type: 'kinetic', 'bar_race', 'split', or 'narrative'

        Returns:
            Dict with keys: success, story_id, video_id, youtube_url, error
        """
        if format_type == "kinetic":
            return self._produce_kinetic()
        elif format_type == "narrative":
            return self._produce_narrative()
        elif format_type == "bar_race":
            return self._produce_bar_race()
        elif format_type == "split":
            return self._produce_split()
        elif format_type == "countdown":
            return self._produce_countdown()
        elif format_type == "comparison":
            return self._produce_comparison()
        else:
            return self._produce_kinetic()

    # ------------------------------------------------------------------
    # Shared post-production steps (7-12)
    # ------------------------------------------------------------------

    def _post_production(
        self,
        story_id: str,
        video_path: str,
        audio_path: str,
        metric_name: str,
        data_source: str,
        script_result: Any,
        result: dict[str, Any],
        format_type: str = "kinetic",
    ) -> dict[str, Any]:
        """Steps 7-12: merge, upload, DB, GSheet, Telegram."""

        # --- Step 7: Merge audio + video ---
        logger.info("[dataforge] Step 7: Merging audio + video...")
        try:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            final_path = str(OUTPUT_DIR / f"{story_id}_final.mp4")
            _merge_audio_video(video_path, audio_path, final_path)
            logger.info("[dataforge] Step 7 complete: %s", final_path)
        except Exception as e:
            logger.error("[dataforge] Step 7 failed: %s", e, exc_info=True)
            _update_story_status(self.db_path, story_id, "MERGE_FAILED")
            result["error"] = f"Step 7 merge failed: {e}"
            return result

        # --- Step 8: Upload to YouTube ---
        logger.info("[dataforge] Step 8: Uploading to YouTube...")
        try:
            import datetime as _dt
            _date_suffix = _dt.datetime.now().strftime("%b %d")
            _base_title = getattr(script_result, '_youtube_title', None) or script_result.hook[:90]

            # Generate 3 title variants and pick the best
            _api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            _variants = _generate_title_variants(
                metric_name=metric_name,
                hook=script_result.hook,
                full_script=script_result.full_script,
                pct_change=getattr(script_result, 'pct_change', 0.0),
                api_key=_api_key,
            ) if _api_key else []

            if _variants:
                video_title = f"{_variants[0]} | {_date_suffix}"
            else:
                video_title = f"{_base_title} | {_date_suffix}"

            # Store all variants in DB
            try:
                _conn = sqlite3.connect(self.db_path)
                try:
                    _conn.execute(
                        """INSERT INTO title_variants
                           (story_id, variant_1, variant_2, variant_3,
                            selected_title, selected_rank, format, recorded_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            story_id,
                            _variants[0] if len(_variants) > 0 else None,
                            _variants[1] if len(_variants) > 1 else None,
                            _variants[2] if len(_variants) > 2 else None,
                            video_title,
                            1,
                            format_type,
                            datetime.now(timezone.utc).isoformat(),
                        ),
                    )
                    _conn.commit()
                    logger.info("[dataforge] Step 8: title variants saved for %s", story_id)
                finally:
                    _conn.close()
            except Exception as _e:
                logger.warning("[dataforge] Step 8: title variants save failed (non-fatal): %s", _e)
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

        result["success"] = True
        result["video_id"] = video_id
        result["youtube_url"] = youtube_url
        logger.info(
            "[dataforge] Production complete: %s -> %s (all steps passed)",
            story_id, youtube_url,
        )
        return result

    # ------------------------------------------------------------------
    # Format 1: Kinetic
    # ------------------------------------------------------------------

    def _produce_kinetic(self) -> dict[str, Any]:
        """Full kinetic (Format 1) production flow."""
        story_id = f"df_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        result: dict[str, Any] = {
            "success": False, "story_id": story_id,
            "video_id": None, "youtube_url": None, "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET}"
                logger.warning("[dataforge] %s", result["error"])
                return result
            logger.info("[dataforge] Quota OK: %d/%d units used", units_used, YOUTUBE_DAILY_BUDGET)

            # --- Step 1: Fetch data (FRED -> crypto fallback) ---
            logger.info("[dataforge] [kinetic] Step 1: Fetching data...")
            from src.data.data_fetcher import DataFetcher
            fetcher = DataFetcher()
            story_data = None
            news_context = []

            # Try news-triggered story first (most current and human)
            news_story = fetcher.fetch_news_triggered_story()
            if news_story and news_story.get('data_point'):
                dp = news_story['data_point']
                story_data = dp
                data_source = dp.data_source
                metric_name = dp.metric_name
                news_context = [news_story['headline']]
                logger.info(
                    "[dataforge] [kinetic] Using news-triggered story: %s | %s",
                    news_story['headline'][:60], dp.metric_name,
                )
            else:
                # Fallback: FRED daily rotation
                logger.info("[dataforge] [kinetic] News trigger failed — falling back to FRED rotation")
                daily_story = fetcher.fetch_fred_daily_story()
                if daily_story:
                    story_data = daily_story
                    data_source = daily_story.data_source
                    metric_name = daily_story.metric_name
                    news_context = fetcher.fetch_news_context(metric_name, max_results=2)
                else:
                    # Final fallback: stock movers
                    movers = fetcher.fetch_daily_movers(top_n=5)
                    if not movers:
                        logger.error("[dataforge] [kinetic] All data sources failed")
                        result["error"] = "All data sources failed"
                        return result
                    story_data = movers[0]
                    data_source = story_data.data_source
                    metric_name = story_data.metric_name
                    news_context = fetcher.fetch_news_context(metric_name, max_results=2)

            dp = story_data
            current_value = story_data.current_value
            prev_value = story_data.prev_value
            pct_change = story_data.pct_change
            news_headlines = [h if isinstance(h, str) else str(h) for h in news_context]
            logger.info("[dataforge] [kinetic] Step 1 complete: %s (%+.2f%%)", metric_name, pct_change)

            # --- Step 3: Generate script ---
            logger.info("[dataforge] Step 3: Generating script...")
            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=metric_name, current_value=current_value,
                prev_value=prev_value, pct_change=pct_change,
                data_source=data_source, news_context=news_headlines,
                story_type="kinetic",
            )
            if not script_result.is_valid:
                result["error"] = f"Script invalid: {script_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 3 complete: %d words", script_result.word_count)

            # --- Step 4: Save story to DB ---
            _save_story(
                self.db_path, story_id, "kinetic", data_source, metric_name,
                current_value, prev_value, pct_change,
                script_result.full_script, script_result.hook,
            )

            # Write story to GSheet Data Queue
            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.write_story({
                    "story_id":  story_id,
                    "metric":    metric_name,
                    "format":    "kinetic",
                    "status":    "QUEUED",
                    "hook":      script_result.hook,
                    "source":    data_source,
                })
                logger.info("[dataforge] Step 4: GSheet write_story complete for %s", story_id)
            except Exception as e:
                logger.warning("[dataforge] Step 4: GSheet write_story failed (non-fatal): %s", e)

            # Write script quality scores to DB
            try:
                q = script_result.quality_scores
                if q:
                    conn = sqlite3.connect(self.db_path)
                    try:
                        conn.execute(
                            """INSERT INTO script_quality
                               (story_id, metric_name, format, clarity, urgency,
                                specificity, hook_strength, avg_score, regenerated,
                                word_count, recorded_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                story_id, metric_name, "kinetic",
                                q.get("clarity"), q.get("urgency"),
                                q.get("specificity"), q.get("hook_strength"),
                                q.get("avg_score"), 1 if q.get("regenerated") else 0,
                                script_result.word_count,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        conn.commit()
                        logger.info(
                            "[dataforge] Step 4: quality scores saved for %s avg=%.2f",
                            story_id, q.get("avg_score", 0),
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.warning("[dataforge] Step 4: quality score save failed (non-fatal): %s", e)

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] Step 5: Generating voiceover...")
            from src.media.voiceover import VoiceoverGenerator
            vo_result = VoiceoverGenerator(output_dir=RAW_DIR).generate(
                script_dict=script_result.to_dict(), topic_id=story_id, category="money",
            )
            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover failed: {vo_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 5 complete: %.1fs", vo_result.duration_seconds)

            # --- Step 6: Render kinetic video ---
            logger.info("[dataforge] Step 6: Rendering kinetic video...")
            from src.media.kinetic_renderer import KineticRenderer
            renderer = KineticRenderer(output_dir=RAW_DIR)
            video_path = renderer.render(
                value=current_value, prev_value=prev_value, label=metric_name,
                currency=dp.currency if hasattr(dp, "currency") else "$",
                duration_sec=vo_result.duration_seconds + 2.0,
                story_id=story_id,
                source_credit="Source: CoinGecko" if data_source == "coingecko" else f"Source: {data_source}",
                script_text=script_result.full_script,
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
                format_type="kinetic",
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Kinetic pipeline crashed: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge KINETIC CRASHED\nStory: {story_id}\nError: {e}")
            return result

    # ------------------------------------------------------------------
    # Format 2: Bar Race
    # ------------------------------------------------------------------

    def _produce_bar_race(self) -> dict[str, Any]:
        """Full bar race (Format 2) production flow."""
        story_id = f"df_br_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        result: dict[str, Any] = {
            "success": False, "story_id": story_id,
            "video_id": None, "youtube_url": None, "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET}"
                logger.warning("[dataforge] %s", result["error"])
                return result
            logger.info("[dataforge] Quota OK: %d/%d units used", units_used, YOUTUBE_DAILY_BUDGET)

            # --- Step 1: Fetch crypto market cap history ---
            logger.info("[dataforge] [bar_race] Step 1: Fetching crypto market cap history...")
            from src.data.data_fetcher import DataFetcher
            import pandas as pd
            import numpy as np

            fetcher = DataFetcher()
            metric_name = "Crypto Market Cap Race"
            data_source = "CoinGecko"
            df = None

            try:
                df = fetcher.fetch_crypto_market_cap_history(top_n=10, days=30)
                if df is not None and not df.empty:
                    logger.info(
                        '[dataforge] CoinGecko market cap history: %d coins x %d days',
                        len(df), len(df.columns),
                    )
                else:
                    logger.warning('[dataforge] CoinGecko returned empty DataFrame')
                    df = None
            except Exception as e:
                logger.error("[dataforge] CoinGecko fetch failed: %s", e, exc_info=True)
                df = None

            if df is None:
                logger.warning("[dataforge] Using fallback crypto sample data")
                coins_fb = [
                    'Bitcoin', 'Ethereum', 'Tether', 'BNB', 'Solana',
                    'XRP', 'USDC', 'Cardano', 'Avalanche', 'Dogecoin',
                ]
                import datetime as _dt
                days_fb = [
                    (_dt.datetime.now() - _dt.timedelta(days=i)).strftime('%b %d')
                    for i in range(29, -1, -1)
                ]
                np.random.seed(42)
                base = [1.8e12, 4e11, 1.1e11, 9e10, 7e10, 6e10, 4e10, 2e10, 1.5e10, 1.2e10]
                df = pd.DataFrame(
                    {d: [b * (1 + np.random.uniform(-0.05, 0.05)) for b in base] for d in days_fb},
                    index=coins_fb,
                )

            logger.info("[dataforge] Step 1 complete: %d entities x %d periods", *df.shape)

            # --- Step 2: Fetch news context for crypto ---
            logger.info("[dataforge] [bar_race] Step 2: Fetching crypto news context...")
            try:
                news = fetcher.fetch_news_context('cryptocurrency bitcoin market', max_results=3)
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

            # Build crypto summary for script generation
            try:
                latest_col = df.columns[-1]
                prev_col = df.columns[-2] if len(df.columns) > 1 else df.columns[-1]
                top_coin = df[latest_col].idxmax()
                top_val = df[latest_col].max()
                top_prev = df[prev_col].get(top_coin, top_val)
                top_pct = ((top_val - top_prev) / top_prev * 100) if top_prev else 0

                # Find biggest mover
                df_pct = ((df[latest_col] - df[prev_col]) / df[prev_col].abs() * 100)
                biggest_mover = df_pct.abs().idxmax()
                biggest_pct = df_pct[biggest_mover]

                script_metric = f"Crypto Market Cap — {top_coin} leads"
                script_current = round(top_val / 1e9, 1)
                script_prev = round(top_prev / 1e9, 1)
                script_pct = round(top_pct, 2)
                _sign = "+" if top_pct >= 0 else ""
                news_headlines = [
                    f"{top_coin} market cap: ${script_current}B ({_sign}{top_pct:.1f}% today)",
                    f"Biggest mover: {biggest_mover} ({biggest_pct:+.1f}%)",
                ] + news_headlines[:1]

                logger.info(
                    "[dataforge] Crypto summary: top=%s $%sB (%+.1f%%) mover=%s (%+.1f%%)",
                    top_coin, script_current, top_pct, biggest_mover, biggest_pct,
                )
            except Exception as e:
                logger.warning("[dataforge] Crypto summary failed: %s", e)
                script_metric = "Crypto Market Cap Rankings"
                script_current = 0
                script_prev = 0
                script_pct = 0

            # --- Step 3: Generate script ---
            logger.info("[dataforge] [bar_race] Step 3: Generating script...")
            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=script_metric,
                current_value=script_current,
                prev_value=script_prev,
                pct_change=script_pct,
                data_source=data_source,
                news_context=news_headlines,
                story_type="bar_race",
            )
            if not script_result.is_valid:
                result["error"] = f"Script invalid: {script_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 3 complete: %d words", script_result.word_count)

            # --- Step 4: Save story to DB ---
            _save_story(
                self.db_path, story_id, "bar_race", data_source, metric_name,
                0, 0, 0, script_result.full_script, script_result.hook,
            )

            # Write story to GSheet Data Queue
            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.write_story({
                    "story_id":  story_id,
                    "metric":    metric_name,
                    "format":    "bar_race",
                    "status":    "QUEUED",
                    "hook":      script_result.hook,
                    "source":    data_source,
                })
                logger.info("[dataforge] Step 4: GSheet write_story complete for %s", story_id)
            except Exception as e:
                logger.warning("[dataforge] Step 4: GSheet write_story failed (non-fatal): %s", e)

            # Write script quality scores to DB
            try:
                q = script_result.quality_scores
                if q:
                    conn = sqlite3.connect(self.db_path)
                    try:
                        conn.execute(
                            """INSERT INTO script_quality
                               (story_id, metric_name, format, clarity, urgency,
                                specificity, hook_strength, avg_score, regenerated,
                                word_count, recorded_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                story_id, metric_name, "bar_race",
                                q.get("clarity"), q.get("urgency"),
                                q.get("specificity"), q.get("hook_strength"),
                                q.get("avg_score"), 1 if q.get("regenerated") else 0,
                                script_result.word_count,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        conn.commit()
                        logger.info(
                            "[dataforge] Step 4: quality scores saved for %s avg=%.2f",
                            story_id, q.get("avg_score", 0),
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.warning("[dataforge] Step 4: quality score save failed (non-fatal): %s", e)

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] [bar_race] Step 5: Generating voiceover...")
            from src.media.voiceover import VoiceoverGenerator
            vo_result = VoiceoverGenerator(output_dir=RAW_DIR).generate(
                script_dict=script_result.to_dict(), topic_id=story_id, category="money",
            )
            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover failed: {vo_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 5 complete: %.1fs", vo_result.duration_seconds)

            # --- Step 6: Render bar race video ---
            logger.info("[dataforge] [bar_race] Step 6: Rendering bar race...")
            from src.media.bar_race_renderer import BarRaceRenderer
            renderer = BarRaceRenderer(output_dir=RAW_DIR)
            video_path = renderer.render(
                df=df,
                title='Crypto Market Cap Race — Last 30 Days',
                value_label='Market Cap (USD)',
                duration_sec=vo_result.duration_seconds + 2.0,
                top_n=10,
                story_id=story_id,
                source_credit='Source: CoinGecko',
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            # Override YouTube title for bar race (not from script hook)
            import datetime as _dt
            _today = _dt.datetime.now().strftime("%b %d")
            script_result._youtube_title = f"Crypto Market Cap Race: Last 30 Days | {_today}"

            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
                format_type="bar_race",
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Bar race pipeline crashed: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge BAR_RACE CRASHED\nStory: {story_id}\nError: {e}")
            return result

    # ------------------------------------------------------------------
    # Format 5: Countdown Ranking Reveal
    # ------------------------------------------------------------------

    def _produce_countdown(self) -> dict[str, Any]:
        """Full countdown ranking reveal (Format 5) production flow."""
        story_id = f"df_cd_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        result: dict[str, Any] = {
            "success": False, "story_id": story_id,
            "video_id": None, "youtube_url": None, "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET}"
                logger.warning("[dataforge] %s", result["error"])
                return result
            logger.info("[dataforge] Quota OK: %d/%d units used", units_used, YOUTUBE_DAILY_BUDGET)

            # --- Step 1: Fetch top 10 crypto by market cap (current snapshot) ---
            logger.info("[dataforge] [countdown] Step 1: Fetching top 10 crypto rankings...")
            from src.data.data_fetcher import DataFetcher
            fetcher = DataFetcher()
            data_source = "CoinGecko"
            metric_name = "Top 10 Cryptos by Market Cap"

            items = []
            try:
                import requests
                resp = requests.get(
                    'https://api.coingecko.com/api/v3/coins/markets',
                    params={
                        'vs_currency': 'usd',
                        'order': 'market_cap_desc',
                        'per_page': 10,
                        'page': 1,
                        'price_change_percentage': '24h',
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                coins = resp.json()
                for coin in coins:
                    pct = coin.get('price_change_percentage_24h', 0) or 0
                    mcap = coin.get('market_cap', 0) or 0
                    if mcap >= 1_000_000_000:
                        val_str = f"${mcap / 1_000_000_000:.1f}B"
                    elif mcap >= 1_000_000:
                        val_str = f"${mcap / 1_000_000:.1f}M"
                    else:
                        val_str = f"${mcap:,.0f}"
                    if mcap < 20_000_000_000:
                        continue
                    sign = '+' if pct >= 0 else ''
                    items.append({
                        'name': coin.get('name', ''),
                        'value': val_str,
                        'change': f"{sign}{pct:.1f}%",
                    })
                logger.info("[dataforge] [countdown] Fetched %d coins", len(items))
            except Exception as e:
                logger.error("[dataforge] [countdown] CoinGecko fetch failed: %s", e)
                items = []

            # Fallback sample data
            if not items:
                logger.warning("[dataforge] [countdown] Using fallback sample data")
                items = [
                    {'name': 'Bitcoin',  'value': '$1.37T', 'change': '+2.1%'},
                    {'name': 'Ethereum', 'value': '$253B',  'change': '-1.3%'},
                    {'name': 'Tether',   'value': '$144B',  'change': '+0.0%'},
                    {'name': 'BNB',      'value': '$87B',   'change': '+1.2%'},
                    {'name': 'Solana',   'value': '$71B',   'change': '+3.4%'},
                    {'name': 'XRP',      'value': '$62B',   'change': '-0.8%'},
                    {'name': 'USDC',     'value': '$43B',   'change': '+0.0%'},
                    {'name': 'Cardano',  'value': '$22B',   'change': '+1.1%'},
                    {'name': 'Avalanche','value': '$15B',   'change': '-2.2%'},
                    {'name': 'Dogecoin', 'value': '$12B',   'change': '+0.5%'},
                ]

            logger.info("[dataforge] Step 1 complete: %d ranked items", len(items))

            # --- Step 2: News context ---
            logger.info("[dataforge] [countdown] Step 2: Fetching crypto news context...")
            try:
                news = fetcher.fetch_news_context('crypto market cap ranking', max_results=2)
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

            # Build script summary from #1 coin
            top = items[0] if items else {}
            script_metric = f"Crypto Rankings — {top.get('name', 'Bitcoin')} leads"
            try:
                raw_val = top.get('value', '$0').replace('$', '').replace('B', 'e9').replace('T', 'e12').replace('M', 'e6')
                script_current = float(raw_val) / 1e9
            except Exception:
                script_current = 0.0
            script_pct = float(top.get('change', '0%').replace('%', '').replace('+', '')) if top else 0.0
            news_headlines = [
                f"{top.get('name', 'Bitcoin')} leads crypto market cap rankings today",
            ] + news_headlines[:1]

            # --- Step 3: Generate script ---
            logger.info("[dataforge] [countdown] Step 3: Generating script...")
            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=script_metric,
                current_value=script_current,
                prev_value=script_current * 0.98,
                pct_change=script_pct,
                data_source=data_source,
                news_context=news_headlines,
                story_type="countdown",
            )
            if not script_result.is_valid:
                result["error"] = f"Script invalid: {script_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 3 complete: %d words", script_result.word_count)

            # --- Step 4: Save story to DB ---
            _save_story(
                self.db_path, story_id, "countdown", data_source, metric_name,
                script_current, 0, script_pct,
                script_result.full_script, script_result.hook,
            )

            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.write_story({
                    "story_id": story_id, "metric": metric_name,
                    "format": "countdown", "status": "QUEUED",
                    "hook": script_result.hook, "source": data_source,
                })
                logger.info("[dataforge] Step 4: GSheet write_story complete for %s", story_id)
            except Exception as e:
                logger.warning("[dataforge] Step 4: GSheet write_story failed (non-fatal): %s", e)

            try:
                q = script_result.quality_scores
                if q:
                    conn = sqlite3.connect(self.db_path)
                    try:
                        conn.execute(
                            """INSERT INTO script_quality
                               (story_id, metric_name, format, clarity, urgency,
                                specificity, hook_strength, avg_score, regenerated,
                                word_count, recorded_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                story_id, metric_name, "countdown",
                                q.get("clarity"), q.get("urgency"),
                                q.get("specificity"), q.get("hook_strength"),
                                q.get("avg_score"), 1 if q.get("regenerated") else 0,
                                script_result.word_count,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        conn.commit()
                        logger.info(
                            "[dataforge] Step 4: quality scores saved for %s avg=%.2f",
                            story_id, q.get("avg_score", 0),
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.warning("[dataforge] Step 4: quality score save failed (non-fatal): %s", e)

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] [countdown] Step 5: Generating voiceover...")
            from src.media.voiceover import VoiceoverGenerator
            vo_result = VoiceoverGenerator(output_dir=RAW_DIR).generate(
                script_dict=script_result.to_dict(), topic_id=story_id, category="money",
            )
            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover failed: {vo_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 5 complete: %.1fs", vo_result.duration_seconds)

            # --- Step 6: Render countdown video ---
            logger.info("[dataforge] [countdown] Step 6: Rendering countdown reveal...")
            from src.media.countdown_renderer import CountdownRenderer
            renderer = CountdownRenderer(output_dir=RAW_DIR)
            import datetime as _dt
            _today = _dt.datetime.now().strftime("%b %d")
            video_path = renderer.render(
                items=items,
                title='Top 10 Cryptos by Market Cap',
                subtitle=f'Ranked by Market Cap | {_today}',
                duration_sec=vo_result.duration_seconds + 2.0,
                story_id=story_id,
                source_credit='Source: CoinGecko',
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            script_result._youtube_title = f"Top 10 Cryptos by Market Cap | {_today}"

            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
                format_type="countdown",
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Countdown pipeline crashed: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge COUNTDOWN CRASHED\nStory: {story_id}\nError: {e}")
            return result

    # ------------------------------------------------------------------
    # Format 3: Split Screen
    # ------------------------------------------------------------------

    def _produce_split(self) -> dict[str, Any]:
        """Full split-screen (Format 3) production flow — compares two FRED metrics."""
        story_id = f"df_sp_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        result: dict[str, Any] = {
            "success": False, "story_id": story_id,
            "video_id": None, "youtube_url": None, "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET}"
                logger.warning("[dataforge] %s", result["error"])
                return result
            logger.info("[dataforge] Quota OK: %d/%d units used", units_used, YOUTUBE_DAILY_BUDGET)

            # --- Step 1: Fetch TWO FRED series via day-of-year rotation ---
            logger.info("[dataforge] [split] Step 1: Fetching two FRED series...")
            from src.data.data_fetcher import DataFetcher

            fetcher = DataFetcher()
            data_source = "FRED"

            FRED_SERIES = [
                ('FEDFUNDS',          'Fed Funds Rate',        '%'),
                ('CPIAUCSL',          'US Inflation (CPI)',    '%'),
                ('UNRATE',            'US Unemployment Rate',  '%'),
                ('MORTGAGE30US',      '30-Year Mortgage Rate', '%'),
                ('T10Y2Y',            'Yield Curve Spread',    'pts'),
                ('DCOILWTICO',        'WTI Crude Oil',         '$'),
                ('GOLDAMGBD228NLBM',  'Gold Price',            '$'),
                ('DEXUSEU',           'EUR/USD Exchange Rate',  '$'),
                ('SP500',             'S&P 500 Index',         'pts'),
                ('NASDAQCOM',         'NASDAQ Composite',      'pts'),
                ('VIXCLS',            'VIX Fear Index',        'pts'),
                ('DEXJPUS',           'USD/JPY Exchange Rate',  'Y'),
                ('BAMLH0A0HYM2',     'High Yield Spread',     'pts'),
                ('UMCSENT',           'Consumer Sentiment',    'pts'),
            ]

            day_index = datetime.utcnow().timetuple().tm_yday
            idx_a = day_index % len(FRED_SERIES)
            idx_b = (day_index + 1) % len(FRED_SERIES)
            series_a = FRED_SERIES[idx_a]
            series_b = FRED_SERIES[idx_b]

            logger.info("[dataforge] Split rotation: day %d -> %s vs %s",
                        day_index, series_a[1], series_b[1])

            # Fetch metric A
            metric_a = None
            try:
                df_a = fetcher.fetch_fred_series(series_a[0], periods=2)
                if df_a is not None and len(df_a) >= 2:
                    cur_a = float(df_a.iloc[-1]['value'] if 'value' in df_a.columns else df_a.iloc[-1])
                    prev_a = float(df_a.iloc[-2]['value'] if 'value' in df_a.columns else df_a.iloc[-2])
                    metric_a = {'name': series_a[1], 'current': cur_a, 'prev': prev_a, 'unit': series_a[2]}
                    logger.info("[dataforge] Metric A: %s = %.4f (prev %.4f)", series_a[1], cur_a, prev_a)
            except Exception as e:
                logger.warning("[dataforge] FRED %s failed: %s", series_a[0], e)

            # Fetch metric B
            metric_b = None
            try:
                df_b = fetcher.fetch_fred_series(series_b[0], periods=2)
                if df_b is not None and len(df_b) >= 2:
                    cur_b = float(df_b.iloc[-1]['value'] if 'value' in df_b.columns else df_b.iloc[-1])
                    prev_b = float(df_b.iloc[-2]['value'] if 'value' in df_b.columns else df_b.iloc[-2])
                    metric_b = {'name': series_b[1], 'current': cur_b, 'prev': prev_b, 'unit': series_b[2]}
                    logger.info("[dataforge] Metric B: %s = %.4f (prev %.4f)", series_b[1], cur_b, prev_b)
            except Exception as e:
                logger.warning("[dataforge] FRED %s failed: %s", series_b[0], e)

            # Fallback to crypto if either FRED series failed
            if metric_a is None or metric_b is None:
                logger.warning("[dataforge] FRED incomplete — falling back to crypto for missing slot(s)")
                try:
                    crypto = fetcher.fetch_crypto_movers(top_n=10)
                    if metric_a is None and len(crypto) >= 1:
                        c = crypto[0]
                        metric_a = {'name': c.metric_name, 'current': c.current_value,
                                    'prev': c.prev_value, 'unit': '$'}
                    if metric_b is None and len(crypto) >= 2:
                        c = crypto[1]
                        metric_b = {'name': c.metric_name, 'current': c.current_value,
                                    'prev': c.prev_value, 'unit': '$'}
                        data_source = "CoinGecko"
                except Exception as e:
                    logger.error("[dataforge] Crypto fallback also failed: %s", e)

            if metric_a is None or metric_b is None:
                result["error"] = "Cannot fetch two metrics for split screen (FRED + crypto both failed)"
                logger.error("[dataforge] %s", result["error"])
                return result

            metric_name = f"{metric_a['name']} vs {metric_b['name']}"
            logger.info("[dataforge] Step 1 complete: %s", metric_name)

            # --- Step 2: News context ---
            logger.info("[dataforge] [split] Step 2: Fetching news context...")
            try:
                news = fetcher.fetch_news_context(
                    f"{metric_a['name']} {metric_b['name']}", max_results=2,
                )
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

            # --- Step 3: Generate script ---
            logger.info("[dataforge] [split] Step 3: Generating script...")
            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=metric_name,
                current_value=metric_a['current'],
                prev_value=metric_a['prev'],
                pct_change=0,
                data_source=data_source,
                news_context=news_headlines,
                story_type="split",
            )
            if not script_result.is_valid:
                result["error"] = f"Script invalid: {script_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 3 complete: %d words", script_result.word_count)

            # --- Step 4: Save story to DB ---
            _save_story(
                self.db_path, story_id, "split", data_source, metric_name,
                metric_a['current'], metric_a['prev'], 0,
                script_result.full_script, script_result.hook,
            )

            # Write story to GSheet Data Queue
            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.write_story({
                    "story_id":  story_id,
                    "metric":    metric_name,
                    "format":    "split",
                    "status":    "QUEUED",
                    "hook":      script_result.hook,
                    "source":    data_source,
                })
                logger.info("[dataforge] Step 4: GSheet write_story complete for %s", story_id)
            except Exception as e:
                logger.warning("[dataforge] Step 4: GSheet write_story failed (non-fatal): %s", e)

            # Write script quality scores to DB
            try:
                q = script_result.quality_scores
                if q:
                    conn = sqlite3.connect(self.db_path)
                    try:
                        conn.execute(
                            """INSERT INTO script_quality
                               (story_id, metric_name, format, clarity, urgency,
                                specificity, hook_strength, avg_score, regenerated,
                                word_count, recorded_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                story_id, metric_name, "split",
                                q.get("clarity"), q.get("urgency"),
                                q.get("specificity"), q.get("hook_strength"),
                                q.get("avg_score"), 1 if q.get("regenerated") else 0,
                                script_result.word_count,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        conn.commit()
                        logger.info(
                            "[dataforge] Step 4: quality scores saved for %s avg=%.2f",
                            story_id, q.get("avg_score", 0),
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.warning("[dataforge] Step 4: quality score save failed (non-fatal): %s", e)

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] [split] Step 5: Generating voiceover...")
            from src.media.voiceover import VoiceoverGenerator
            vo_result = VoiceoverGenerator(output_dir=RAW_DIR).generate(
                script_dict=script_result.to_dict(), topic_id=story_id, category="money",
            )
            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover failed: {vo_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 5 complete: %.1fs", vo_result.duration_seconds)

            # --- Step 6: Render split video ---
            logger.info("[dataforge] [split] Step 6: Rendering split screen...")
            from src.media.split_renderer import SplitRenderer
            renderer = SplitRenderer(output_dir=RAW_DIR)
            video_path = renderer.render(
                metric_a=metric_a,
                metric_b=metric_b,
                duration_sec=vo_result.duration_seconds + 2.0,
                story_id=story_id,
                source_credit=f'Source: {data_source}',
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            script_result._youtube_title = f"{metric_a['name']} vs {metric_b['name']} | Today's Data"

            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
                format_type="split",
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Split pipeline crashed: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge SPLIT CRASHED\nStory: {story_id}\nError: {e}")
            return result

    # ------------------------------------------------------------------
    # Format 6: Comparison Ticker
    # ------------------------------------------------------------------

    def _produce_comparison(self) -> dict[str, Any]:
        """Full comparison ticker (Format 6) production flow — two FRED metrics racing."""
        story_id = f"df_cp_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        result: dict[str, Any] = {
            "success": False, "story_id": story_id,
            "video_id": None, "youtube_url": None, "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET}"
                logger.warning("[dataforge] %s", result["error"])
                return result
            logger.info("[dataforge] Quota OK: %d/%d units used", units_used, YOUTUBE_DAILY_BUDGET)

            # --- Step 1: Fetch two FRED series via offset rotation ---
            # Use day+2 offset from split to avoid same pair on same day
            logger.info("[dataforge] [comparison] Step 1: Fetching two FRED series...")
            from src.data.data_fetcher import DataFetcher
            fetcher = DataFetcher()
            data_source = "FRED"

            FRED_PAIRS = [
                ('FEDFUNDS',         'Fed Funds Rate',        '%',
                 'MORTGAGE30US',     '30-Year Mortgage Rate', '%'),
                ('CPIAUCSL',         'US Inflation (CPI)',    '%',
                 'UNRATE',           'US Unemployment Rate',  '%'),
                ('SP500',            'S&P 500 Index',         'pts',
                 'NASDAQCOM',        'NASDAQ Composite',      'pts'),
                ('DCOILWTICO',       'WTI Crude Oil',         '$',
                 'GOLDAMGBD228NLBM', 'Gold Price',            '$'),
                ('T10Y2Y',           'Yield Curve Spread',    'pts',
                 'BAMLH0A0HYM2',     'High Yield Spread',     'pts'),
                ('VIXCLS',           'VIX Fear Index',        'pts',
                 'UMCSENT',          'Consumer Sentiment',    'pts'),
                ('DEXUSEU',          'EUR/USD Rate',          '$',
                 'DEXJPUS',          'USD/JPY Rate',          'Y'),
            ]

            day_index = datetime.utcnow().timetuple().tm_yday
            pair_idx = (day_index + 2) % len(FRED_PAIRS)
            pair = FRED_PAIRS[pair_idx]
            sid_a, label_a, unit_a, sid_b, label_b, unit_b = pair

            logger.info("[dataforge] Comparison pair: %s vs %s", label_a, label_b)

            metric_a = None
            metric_b = None

            try:
                df_a = fetcher.fetch_fred_series(sid_a, periods=2)
                if df_a is not None and len(df_a) >= 2:
                    cur_a  = float(df_a.iloc[-1]['value'])
                    prev_a = float(df_a.iloc[-2]['value'])
                    metric_a = {'name': label_a, 'current': cur_a, 'prev': prev_a, 'unit': unit_a}
                    logger.info("[dataforge] Metric A: %s = %.4f", label_a, cur_a)
            except Exception as e:
                logger.warning("[dataforge] FRED %s failed: %s", sid_a, e)

            try:
                df_b = fetcher.fetch_fred_series(sid_b, periods=2)
                if df_b is not None and len(df_b) >= 2:
                    cur_b  = float(df_b.iloc[-1]['value'])
                    prev_b = float(df_b.iloc[-2]['value'])
                    metric_b = {'name': label_b, 'current': cur_b, 'prev': prev_b, 'unit': unit_b}
                    logger.info("[dataforge] Metric B: %s = %.4f", label_b, cur_b)
            except Exception as e:
                logger.warning("[dataforge] FRED %s failed: %s", sid_b, e)

            # Fallback: crypto pair if FRED fails
            if metric_a is None or metric_b is None:
                logger.warning("[dataforge] FRED incomplete — falling back to crypto pair")
                try:
                    crypto = fetcher.fetch_crypto_movers(top_n=10)
                    if metric_a is None and len(crypto) >= 1:
                        c = crypto[0]
                        metric_a = {'name': c.metric_name, 'current': c.current_value,
                                    'prev': c.prev_value, 'unit': '$'}
                        data_source = "CoinGecko"
                    if metric_b is None and len(crypto) >= 2:
                        c = crypto[1]
                        metric_b = {'name': c.metric_name, 'current': c.current_value,
                                    'prev': c.prev_value, 'unit': '$'}
                        data_source = "CoinGecko"
                except Exception as e:
                    logger.error("[dataforge] Crypto fallback failed: %s", e)

            if metric_a is None or metric_b is None:
                result["error"] = "Cannot fetch two metrics for comparison (FRED + crypto both failed)"
                logger.error("[dataforge] %s", result["error"])
                return result

            metric_name = f"{metric_a['name']} vs {metric_b['name']}"
            logger.info("[dataforge] Step 1 complete: %s", metric_name)

            # --- Step 2: News context ---
            logger.info("[dataforge] [comparison] Step 2: Fetching news context...")
            try:
                news = fetcher.fetch_news_context(
                    f"{metric_a['name']} {metric_b['name']}", max_results=2,
                )
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

            # --- Step 3: Generate script ---
            logger.info("[dataforge] [comparison] Step 3: Generating script...")
            pct_a = ((metric_a['current'] - metric_a['prev']) / abs(metric_a['prev']) * 100) if metric_a['prev'] else 0.0
            pct_b = ((metric_b['current'] - metric_b['prev']) / abs(metric_b['prev']) * 100) if metric_b['prev'] else 0.0
            winner = metric_a if pct_a >= pct_b else metric_b
            loser  = metric_b if pct_a >= pct_b else metric_a

            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=metric_name,
                current_value=winner['current'],
                prev_value=winner['prev'],
                pct_change=max(pct_a, pct_b),
                data_source=data_source,
                news_context=news_headlines,
                story_type="comparison",
            )
            if not script_result.is_valid:
                result["error"] = f"Script invalid: {script_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 3 complete: %d words", script_result.word_count)

            # --- Step 4: Save story to DB ---
            _save_story(
                self.db_path, story_id, "comparison", data_source, metric_name,
                metric_a['current'], metric_a['prev'], pct_a,
                script_result.full_script, script_result.hook,
            )

            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.write_story({
                    "story_id": story_id, "metric": metric_name,
                    "format": "comparison", "status": "QUEUED",
                    "hook": script_result.hook, "source": data_source,
                })
                logger.info("[dataforge] Step 4: GSheet write_story complete for %s", story_id)
            except Exception as e:
                logger.warning("[dataforge] Step 4: GSheet write_story failed (non-fatal): %s", e)

            try:
                q = script_result.quality_scores
                if q:
                    conn = sqlite3.connect(self.db_path)
                    try:
                        conn.execute(
                            """INSERT INTO script_quality
                               (story_id, metric_name, format, clarity, urgency,
                                specificity, hook_strength, avg_score, regenerated,
                                word_count, recorded_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                story_id, metric_name, "comparison",
                                q.get("clarity"), q.get("urgency"),
                                q.get("specificity"), q.get("hook_strength"),
                                q.get("avg_score"), 1 if q.get("regenerated") else 0,
                                script_result.word_count,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        conn.commit()
                        logger.info(
                            "[dataforge] Step 4: quality scores saved for %s avg=%.2f",
                            story_id, q.get("avg_score", 0),
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.warning("[dataforge] Step 4: quality score save failed (non-fatal): %s", e)

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] [comparison] Step 5: Generating voiceover...")
            from src.media.voiceover import VoiceoverGenerator
            vo_result = VoiceoverGenerator(output_dir=RAW_DIR).generate(
                script_dict=script_result.to_dict(), topic_id=story_id, category="money",
            )
            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover failed: {vo_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 5 complete: %.1fs", vo_result.duration_seconds)

            # --- Step 6: Render comparison video ---
            logger.info("[dataforge] [comparison] Step 6: Rendering comparison ticker...")
            from src.media.comparison_renderer import ComparisonRenderer
            renderer = ComparisonRenderer(output_dir=RAW_DIR)
            import datetime as _dt
            _today = _dt.datetime.now().strftime("%b %d")
            video_path = renderer.render(
                metric_a=metric_a,
                metric_b=metric_b,
                duration_sec=vo_result.duration_seconds + 2.0,
                story_id=story_id,
                source_credit=f'Source: {data_source}',
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            script_result._youtube_title = f"{metric_a['name']} vs {metric_b['name']} | {_today}"

            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
                format_type="comparison",
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Comparison pipeline crashed: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge COMPARISON CRASHED\nStory: {story_id}\nError: {e}")
            return result

    # ------------------------------------------------------------------
    # Format 4: Narrative
    # ------------------------------------------------------------------

    def _produce_narrative(self) -> dict[str, Any]:
        """Full narrative (Format 4) production flow — breaking news card."""
        story_id = f"df_nr_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        result: dict[str, Any] = {
            "success": False, "story_id": story_id,
            "video_id": None, "youtube_url": None, "error": None,
        }

        try:
            # --- Quota check ---
            has_budget, units_used = _check_quota(self.db_path)
            if not has_budget:
                result["error"] = f"YouTube quota exhausted: {units_used}/{YOUTUBE_DAILY_BUDGET}"
                logger.warning("[dataforge] %s", result["error"])
                return result
            logger.info("[dataforge] Quota OK: %d/%d units used", units_used, YOUTUBE_DAILY_BUDGET)

            # --- Step 1: Fetch FRED data (same rotation as kinetic) ---
            logger.info("[dataforge] [narrative] Step 1: Fetching FRED data...")
            from src.data.data_fetcher import DataFetcher
            fetcher = DataFetcher()
            dp = None

            try:
                dp = fetcher.fetch_fred_daily_story()
                if dp is None:
                    raise ValueError('FRED returned None')
                logger.info('[dataforge] Using FRED story: %s', dp.metric_name)
            except Exception as e:
                logger.warning('[dataforge] FRED failed (%s), falling back to crypto', e)
                try:
                    movers = fetcher.fetch_crypto_movers(top_n=20)
                    dp = movers[0] if movers else None
                except Exception as e2:
                    logger.error("[dataforge] Crypto also failed: %s", e2, exc_info=True)

            if dp is None:
                result["error"] = "No data from any source (FRED + crypto failed)"
                logger.error("[dataforge] %s", result["error"])
                return result

            metric_name = dp.metric_name
            current_value = dp.current_value
            prev_value = dp.prev_value
            pct_change = dp.pct_change
            data_source = dp.data_source
            unit = dp.currency if hasattr(dp, 'currency') else '$'
            logger.info("[dataforge] Step 1 complete: %s (%+.2f%%)", metric_name, pct_change)

            # --- Step 2: News context ---
            logger.info("[dataforge] [narrative] Step 2: Fetching news context...")
            try:
                news = fetcher.fetch_news_context(metric_name, max_results=3)
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

            # --- Step 3: Generate script ---
            logger.info("[dataforge] [narrative] Step 3: Generating script...")
            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=metric_name, current_value=current_value,
                prev_value=prev_value, pct_change=pct_change,
                data_source=data_source, news_context=news_headlines,
                story_type="narrative",
            )
            if not script_result.is_valid:
                result["error"] = f"Script invalid: {script_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 3 complete: %d words", script_result.word_count)

            # --- Step 4: Save story to DB ---
            _save_story(
                self.db_path, story_id, "narrative", data_source, metric_name,
                current_value, prev_value, pct_change,
                script_result.full_script, script_result.hook,
            )

            # Write story to GSheet Data Queue
            try:
                from src.crawler.gsheet_sync import GSheetSync
                gsheet = GSheetSync()
                gsheet.write_story({
                    "story_id":  story_id,
                    "metric":    metric_name,
                    "format":    "narrative",
                    "status":    "QUEUED",
                    "hook":      script_result.hook,
                    "source":    data_source,
                })
                logger.info("[dataforge] Step 4: GSheet write_story complete for %s", story_id)
            except Exception as e:
                logger.warning("[dataforge] Step 4: GSheet write_story failed (non-fatal): %s", e)

            # Write script quality scores to DB
            try:
                q = script_result.quality_scores
                if q:
                    conn = sqlite3.connect(self.db_path)
                    try:
                        conn.execute(
                            """INSERT INTO script_quality
                               (story_id, metric_name, format, clarity, urgency,
                                specificity, hook_strength, avg_score, regenerated,
                                word_count, recorded_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                story_id, metric_name, "narrative",
                                q.get("clarity"), q.get("urgency"),
                                q.get("specificity"), q.get("hook_strength"),
                                q.get("avg_score"), 1 if q.get("regenerated") else 0,
                                script_result.word_count,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        conn.commit()
                        logger.info(
                            "[dataforge] Step 4: quality scores saved for %s avg=%.2f",
                            story_id, q.get("avg_score", 0),
                        )
                    finally:
                        conn.close()
            except Exception as e:
                logger.warning("[dataforge] Step 4: quality score save failed (non-fatal): %s", e)

            # --- Step 5: Generate voiceover ---
            logger.info("[dataforge] [narrative] Step 5: Generating voiceover...")
            from src.media.voiceover import VoiceoverGenerator
            vo_result = VoiceoverGenerator(output_dir=RAW_DIR).generate(
                script_dict=script_result.to_dict(), topic_id=story_id, category="money",
            )
            if not vo_result.is_valid:
                _update_story_status(self.db_path, story_id, "VO_FAILED")
                result["error"] = f"Voiceover failed: {vo_result.validation_errors}"
                return result
            logger.info("[dataforge] Step 5 complete: %.1fs", vo_result.duration_seconds)

            # --- Step 6: Render narrative video ---
            logger.info("[dataforge] [narrative] Step 6: Rendering narrative card...")
            from src.media.narrative_renderer import NarrativeRenderer
            renderer = NarrativeRenderer(output_dir=RAW_DIR)
            video_path = renderer.render(
                script={
                    'hook': script_result.hook,
                    'context': script_result.context,
                    'cta': script_result.cta,
                },
                metric={
                    'name': metric_name,
                    'current': current_value,
                    'prev': prev_value,
                    'pct_change': pct_change,
                    'unit': unit,
                },
                duration_sec=vo_result.duration_seconds + 2.0,
                story_id=story_id,
                source_credit="Source: CoinGecko" if data_source == "coingecko" else f"Source: {data_source}",
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
                format_type="narrative",
            )

        except Exception as e:
            result["error"] = str(e)
            logger.error("[dataforge] Narrative pipeline crashed: %s", e, exc_info=True)
            try:
                _update_story_status(self.db_path, story_id, "FAILED")
            except Exception:
                pass
            _send_telegram(f"DataForge NARRATIVE CRASHED\nStory: {story_id}\nError: {e}")
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
