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
            _base_title = getattr(script_result, '_youtube_title', None) or script_result.hook[:90]
            _date_suffix = _dt.datetime.now().strftime("%b %d")
            video_title = f"{_base_title} | {_date_suffix}"
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
            logger.info("[dataforge] Step 1 complete: %s (%+.2f%%)", metric_name, pct_change)

            # --- Step 2: News context ---
            logger.info("[dataforge] Step 2: Fetching news context...")
            try:
                news = fetcher.fetch_news_context(metric_name, max_results=3)
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

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
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
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

            # --- Step 1: Fetch World Bank GDP data ---
            logger.info("[dataforge] [bar_race] Step 1: Fetching World Bank GDP data...")
            from src.data.data_fetcher import DataFetcher
            import pandas as pd
            import numpy as np

            fetcher = DataFetcher()
            metric_name = "Global GDP Rankings"
            data_source = "World Bank"
            df = None

            try:
                logger.info('[dataforge] Calling fetch_world_bank(NY.GDP.MKTP.CD)...')
                wb_df = fetcher.fetch_world_bank(
                    indicator='NY.GDP.MKTP.CD',
                    countries=['US', 'CN', 'JP', 'DE', 'GB', 'IN', 'FR', 'BR', 'CA', 'KR'],
                    start_year=2010,
                    end_year=2023,
                )
                if wb_df is not None and not wb_df.empty:
                    logger.info('[dataforge] World Bank raw: %d rows, columns=%s',
                                len(wb_df), list(wb_df.columns))
                    pivot = wb_df.pivot_table(
                        index='country', columns='year', values='value', aggfunc='first',
                    )
                    pivot.columns = [str(c) for c in pivot.columns]
                    pivot = pivot.dropna(axis=1, how='all').dropna(axis=0, how='all')
                    logger.info('[dataforge] World Bank pivot: %d countries x %d years',
                                len(pivot), len(pivot.columns))
                    if len(pivot) >= 3 and len(pivot.columns) >= 3:
                        df = pivot
                    else:
                        logger.warning('[dataforge] World Bank pivot too small: %s', pivot.shape)
                else:
                    logger.warning('[dataforge] World Bank returned empty DataFrame')
            except Exception as e:
                logger.error("[dataforge] World Bank fetch failed: %s", e, exc_info=True)

            if df is None:
                logger.warning("[dataforge] Using fallback GDP sample data")
                countries_fb = [
                    'USA', 'China', 'Japan', 'Germany', 'UK',
                    'India', 'France', 'Brazil', 'Canada', 'S.Korea',
                ]
                years = [str(y) for y in range(2015, 2024)]
                np.random.seed(42)
                df = pd.DataFrame(
                    np.random.uniform(1e12, 25e12, (len(countries_fb), len(years))),
                    index=countries_fb, columns=years,
                )

            logger.info("[dataforge] Step 1 complete: %d entities x %d periods", *df.shape)

            # --- Step 2: News context ---
            logger.info("[dataforge] [bar_race] Step 2: Fetching news context...")
            try:
                news = fetcher.fetch_news_context('global GDP economy', max_results=2)
                news_headlines = [h if isinstance(h, str) else str(h) for h in news]
            except Exception:
                news_headlines = []

            # --- Step 3: Generate script ---
            logger.info("[dataforge] [bar_race] Step 3: Generating script...")
            from src.content.script_adapter import ScriptAdapter
            script_result = ScriptAdapter().generate(
                metric_name=metric_name, current_value=0, prev_value=0,
                pct_change=0, data_source=data_source,
                news_context=news_headlines, story_type="bar_race",
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
                title='Global GDP Rankings',
                value_label='GDP (USD)',
                duration_sec=vo_result.duration_seconds + 2.0,
                top_n=10,
                story_id=story_id,
                source_credit='Source: World Bank',
            )
            logger.info("[dataforge] Step 6 complete: %s", video_path)

            # --- Steps 7-12: post-production ---
            # Override YouTube title for bar race (not from script hook)
            years = list(df.columns)
            script_result._youtube_title = f"Top 10 Economies by GDP: {years[0]}-{years[-1]}"

            return self._post_production(
                story_id, video_path, vo_result.audio_path,
                metric_name, data_source, script_result, result,
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
