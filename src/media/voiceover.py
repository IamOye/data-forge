"""
voiceover.py — VoiceoverGenerator

Generates MP3 voiceovers from script text using the ElevenLabs API.

Usage:
    gen = VoiceoverGenerator()
    result = gen.generate(script_dict, topic_id="stoic_001", category="success")
    print(result.audio_path)
    print(result.duration_seconds)
"""

import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_ELEVENLABS_API_URL = "https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
_ELEVENLABS_API_URL_WITH_TIMESTAMPS = "https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/with-timestamps"
_MODEL_ID = "eleven_turbo_v2"

# ElevenLabs voice IDs for named voices
VOICE_MAP: dict[str, tuple[str, str]] = {
    "money":   ("Adam",   "pNInz6obpgDQGcFmaJgB"),
    "career":  ("Josh",   "TxGEqnHWrfWFTfGW9XjX"),
    "success": ("Josh",   "TxGEqnHWrfWFTfGW9XjX"),
}
DEFAULT_VOICE: tuple[str, str] = ("Adam", "pNInz6obpgDQGcFmaJgB")

VOICE_SETTINGS = {
    "stability":        0.35,   # lower = more expressive delivery
    "similarity_boost": 0.85,
    "style":            0.40,   # higher = more natural style variation
    "use_speaker_boost": True,
}

MIN_DURATION_SECONDS = 10.0

# -14 LUFS is the YouTube recommended integrated loudness level
TARGET_LUFS = -14.0

OUTPUT_DIR = Path("data/raw")

# ---------------------------------------------------------------------------
# Usage tracking constants
# ---------------------------------------------------------------------------

_DEFAULT_DB = Path(os.getenv("DATAFORGE_DB_PATH", "data/processed/data_forge.db"))
_MONTHLY_LIMIT = int(os.getenv("ELEVENLABS_MONTHLY_LIMIT", "30000"))
_RESET_DAY = int(os.getenv("ELEVENLABS_RESET_DAY", "1"))

# Warning thresholds (fractions of monthly limit)
_WARN_67_PCT = 0.67
_WARN_85_PCT = 0.85
_WARN_95_PCT = 0.95


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class VoiceoverResult:
    """Result of a voiceover generation request."""

    topic_id: str
    audio_path: str
    voice_name: str
    voice_id: str
    duration_seconds: float
    is_valid: bool
    validation_errors: list[str] = field(default_factory=list)
    generated_at: str = ""
    words_path: str = ""

    def __post_init__(self) -> None:
        if not self.generated_at:
            self.generated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic_id":          self.topic_id,
            "audio_path":        self.audio_path,
            "voice_name":        self.voice_name,
            "voice_id":          self.voice_id,
            "duration_seconds":  self.duration_seconds,
            "is_valid":          self.is_valid,
            "validation_errors": self.validation_errors,
            "generated_at":      self.generated_at,
            "words_path":        self.words_path,
        }


# ---------------------------------------------------------------------------
# VoiceoverGenerator
# ---------------------------------------------------------------------------

class VoiceoverGenerator:
    """
    Generates voiceover MP3 files via ElevenLabs TTS API.

    Args:
        api_key: ElevenLabs API key. If None, reads ELEVENLABS_API_KEY from env.
        output_dir: Directory to save MP3 files. Defaults to data/raw/.
    """

    def __init__(
        self,
        api_key: str | None = None,
        output_dir: str | Path = OUTPUT_DIR,
        db_path: str | Path | None = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else os.getenv("ELEVENLABS_API_KEY", "")
        self.output_dir = Path(output_dir)
        self.db_path = Path(db_path) if db_path else _DEFAULT_DB

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        script_dict: dict[str, str],
        topic_id: str,
        category: str = "default",
    ) -> VoiceoverResult:
        """
        Generate a voiceover MP3 for the given script.

        Args:
            script_dict: Dict with keys hook, statement, twist, question
                         (or any 'full_script' key). Text is joined in order.
            topic_id: Unique identifier for the topic (used in filename).
            category: Topic category for voice selection (money/career/success).

        Returns:
            VoiceoverResult with path, duration, and validation status.

        Raises:
            ValueError: If ELEVENLABS_API_KEY is not configured.
        """
        if not self.api_key:
            raise ValueError("ELEVENLABS_API_KEY not set")

        voice_name, voice_id = self._select_voice(category)
        text = self._build_text(script_dict)
        output_path = self.output_dir / f"{topic_id}_voice.mp3"

        # Pre-generation budget check: don't waste chars if near monthly limit
        estimated_chars = len(text)
        should_skip, chars_remaining = self._budget_check(estimated_chars)
        if should_skip:
            msg = (
                f"Monthly character limit nearly exhausted. "
                f"Skipping production to preserve remaining {chars_remaining} characters."
            )
            logger.critical("[voiceover] %s", msg)
            return VoiceoverResult(
                topic_id=topic_id,
                audio_path="",
                voice_name=voice_name,
                voice_id=voice_id,
                duration_seconds=0.0,
                is_valid=False,
                validation_errors=[msg],
                words_path="",
            )

        logger.info(
            "Generating voiceover: topic_id=%s, voice=%s, chars=%d",
            topic_id, voice_name, len(text),
        )

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Call ElevenLabs API
        audio_bytes, word_timestamps = self._call_api(voice_id, text)
        output_path.write_bytes(audio_bytes)
        logger.info("Saved voiceover to %s (%d bytes)", output_path, len(audio_bytes))

        # Save word timestamps JSON alongside the audio
        import json as _json
        words_path = self.output_dir / f"{topic_id}_words.json"
        words_path.write_text(_json.dumps(word_timestamps, indent=2), encoding="utf-8")
        logger.debug("Saved %d word timestamps to %s", len(word_timestamps), words_path)

        # Normalize audio loudness with ffmpeg
        self._normalize_audio(output_path)

        # Validate duration
        duration = self._get_duration(output_path)
        errors = self._validate_duration(duration)

        chars_used = len(text)
        logger.info("[voiceover] Used %d chars for topic %s", chars_used, topic_id)
        self._save_usage(topic_id=topic_id, chars_used=chars_used, voice_name=voice_name)
        self._check_monthly_usage()

        result = VoiceoverResult(
            topic_id=topic_id,
            audio_path=str(output_path),
            voice_name=voice_name,
            voice_id=voice_id,
            duration_seconds=duration,
            is_valid=len(errors) == 0,
            validation_errors=errors,
            words_path=str(words_path),
        )
        if errors:
            logger.warning("Voiceover validation errors: %s", errors)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _save_usage(self, topic_id: str, chars_used: int, voice_name: str) -> None:
        """Persist a usage record to the elevenlabs_usage table. Failures are swallowed."""
        try:
            monthly_limit = int(os.getenv("ELEVENLABS_MONTHLY_LIMIT", str(_MONTHLY_LIMIT)))
            reset_day = int(os.getenv("ELEVENLABS_RESET_DAY", str(_RESET_DAY)))
            today = date.today()
            month_start = today.replace(day=reset_day)
            if today.day < reset_day:
                if today.month == 1:
                    month_start = month_start.replace(year=today.year - 1, month=12)
                else:
                    month_start = month_start.replace(month=today.month - 1)

            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS elevenlabs_usage (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        date          TEXT    NOT NULL,
                        topic_id      TEXT    NOT NULL,
                        chars_used    INTEGER NOT NULL,
                        voice_name    TEXT    NOT NULL,
                        monthly_total INTEGER DEFAULT 0,
                        pct_used      REAL    DEFAULT 0,
                        created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
                    )
                """)
                # Migrate: add new columns if they don't exist yet
                for col, coltype in [
                    ("monthly_total", "INTEGER DEFAULT 0"),
                    ("pct_used", "REAL DEFAULT 0"),
                ]:
                    try:
                        conn.execute(f"ALTER TABLE elevenlabs_usage ADD COLUMN {col} {coltype}")
                    except Exception:
                        pass  # column already exists

                # Compute cumulative monthly total including this record
                row = conn.execute(
                    "SELECT SUM(chars_used) FROM elevenlabs_usage WHERE date >= ?",
                    (month_start.isoformat(),),
                ).fetchone()
                prev_total = int(row[0] or 0)
                new_total = prev_total + chars_used
                pct_used = new_total / monthly_limit * 100 if monthly_limit > 0 else 0.0

                conn.execute(
                    "INSERT INTO elevenlabs_usage "
                    "(date, topic_id, chars_used, voice_name, monthly_total, pct_used) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (today.isoformat(), topic_id, chars_used, voice_name,
                     new_total, round(pct_used, 1)),
                )
                conn.commit()
            finally:
                conn.close()
        except Exception as exc:
            logger.warning("[voiceover] Failed to save usage to DB: %s", exc)

    def _check_monthly_usage(self) -> None:
        """Query monthly totals, log status, and emit warnings at 67/85/95% thresholds."""
        try:
            monthly_limit = int(os.getenv("ELEVENLABS_MONTHLY_LIMIT", str(_MONTHLY_LIMIT)))
            reset_day = int(os.getenv("ELEVENLABS_RESET_DAY", str(_RESET_DAY)))

            today = date.today()
            month_start = today.replace(day=reset_day)
            if today.day < reset_day:
                # We're before this month's reset day — look back to last month
                if today.month == 1:
                    month_start = month_start.replace(year=today.year - 1, month=12)
                else:
                    month_start = month_start.replace(month=today.month - 1)

            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS elevenlabs_usage (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        date          TEXT    NOT NULL,
                        topic_id      TEXT    NOT NULL,
                        chars_used    INTEGER NOT NULL,
                        voice_name    TEXT    NOT NULL,
                        monthly_total INTEGER DEFAULT 0,
                        pct_used      REAL    DEFAULT 0,
                        created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
                    )
                """)
                row = conn.execute(
                    "SELECT SUM(chars_used), COUNT(*) FROM elevenlabs_usage WHERE date >= ?",
                    (month_start.isoformat(),),
                ).fetchone()
            finally:
                conn.close()

            monthly_total = int(row[0] or 0)
            videos_produced = int(row[1] or 0)
            chars_remaining = max(0, monthly_limit - monthly_total)
            pct_used = monthly_total / monthly_limit * 100 if monthly_limit > 0 else 0.0

            logger.info(
                "[voiceover] Monthly usage: %d/%d chars (%.1f%%)",
                monthly_total, monthly_limit, pct_used,
            )

            # Calculate videos remaining (avoid division by zero on first video)
            if videos_produced > 0:
                avg_chars = monthly_total / videos_produced
                videos_remaining = int(chars_remaining / avg_chars) if avg_chars > 0 else 0
            else:
                videos_remaining = 0

            # Determine reset date
            if today.day < reset_day:
                reset_date = month_start.replace(month=today.month, day=reset_day)
            else:
                if today.month == 12:
                    reset_date = today.replace(year=today.year + 1, month=1, day=reset_day)
                else:
                    reset_date = today.replace(month=today.month + 1, day=reset_day)

            fraction = monthly_total / monthly_limit if monthly_limit > 0 else 0.0

            reset_date_str = reset_date.strftime("%B %d, %Y")
            if fraction >= _WARN_95_PCT:
                logger.critical(
                    "[voiceover] ElevenLabs at 95%% monthly limit — production will stop at 100%%. "
                    "Reset date: %s",
                    reset_date_str,
                )
                try:
                    from src.notifications.telegram_notifier import TelegramNotifier
                    TelegramNotifier().notify_elevenlabs_critical(
                        chars_remaining=chars_remaining,
                        reset_date=reset_date_str,
                    )
                except Exception:
                    pass
            elif fraction >= _WARN_85_PCT:
                logger.warning(
                    "[voiceover] ElevenLabs at 85%% monthly limit — consider upgrading or pausing "
                    "production until reset."
                )
            elif fraction >= _WARN_67_PCT:
                logger.warning(
                    "[voiceover] ElevenLabs at %.1f%% monthly limit — %d chars remaining. "
                    "Approximately %d videos left this month.",
                    pct_used, chars_remaining, videos_remaining,
                )
                try:
                    from src.notifications.telegram_notifier import TelegramNotifier
                    TelegramNotifier().notify_elevenlabs_warning(
                        chars_used=monthly_total,
                        monthly_limit=monthly_limit,
                        pct=pct_used,
                        videos_left=videos_remaining,
                        reset_date=reset_date_str,
                    )
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("[voiceover] Failed to check monthly usage: %s", exc)

    def _budget_check(self, estimated_chars: int) -> tuple[bool, int]:
        """
        Check if generating ``estimated_chars`` would push monthly total above 95%.

        Returns:
            (should_skip, chars_remaining) — if should_skip is True, caller must
            abort the API call.  Fails safely: returns (False, 0) on any error so
            that a DB outage never blocks production.
        """
        try:
            monthly_limit = int(os.getenv("ELEVENLABS_MONTHLY_LIMIT", str(_MONTHLY_LIMIT)))
            reset_day = int(os.getenv("ELEVENLABS_RESET_DAY", str(_RESET_DAY)))
            today = date.today()
            month_start = today.replace(day=reset_day)
            if today.day < reset_day:
                if today.month == 1:
                    month_start = month_start.replace(year=today.year - 1, month=12)
                else:
                    month_start = month_start.replace(month=today.month - 1)

            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS elevenlabs_usage (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        date          TEXT    NOT NULL,
                        topic_id      TEXT    NOT NULL,
                        chars_used    INTEGER NOT NULL,
                        voice_name    TEXT    NOT NULL,
                        monthly_total INTEGER DEFAULT 0,
                        pct_used      REAL    DEFAULT 0,
                        created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
                    )
                """)
                row = conn.execute(
                    "SELECT SUM(chars_used) FROM elevenlabs_usage WHERE date >= ?",
                    (month_start.isoformat(),),
                ).fetchone()
            finally:
                conn.close()

            monthly_total = int(row[0] or 0)
            chars_remaining = max(0, monthly_limit - monthly_total)
            threshold = monthly_limit * _WARN_95_PCT
            should_skip = (monthly_total + estimated_chars) > threshold
            return should_skip, chars_remaining
        except Exception as exc:
            logger.warning("[voiceover] Budget pre-check failed (proceeding): %s", exc)
            return False, 0

    @staticmethod
    def _select_voice(category: str) -> tuple[str, str]:
        """Return (voice_name, voice_id) for the given category."""
        return VOICE_MAP.get(category.lower(), DEFAULT_VOICE)

    @staticmethod
    def _build_text(script_dict: dict[str, str]) -> str:
        """Concatenate script parts into a single spoken text string."""
        if "full_script" in script_dict:
            return script_dict["full_script"].strip()
        parts = [
            script_dict.get("hook", ""),
            script_dict.get("statement", ""),
            script_dict.get("twist", ""),
            script_dict.get("landing", ""),
            script_dict.get("question", ""),
        ]
        return " ".join(p.strip() for p in parts if p.strip())

    @staticmethod
    def _get_real_usage() -> tuple[int, int]:
        """Query ElevenLabs API directly for real usage figures.

        Returns:
            (characters_used, character_limit) from the subscription endpoint.
        """
        api_key = os.getenv("ELEVENLABS_API_KEY", "")
        if not api_key:
            logger.warning("[elevenlabs] No API key — cannot query live usage")
            return 0, 30000
        resp = httpx.get(
            "https://api.elevenlabs.io/v1/user/subscription",
            headers={"xi-api-key": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        used = data.get("character_count", 0)
        limit = data.get("character_limit", 30000)
        return used, limit

    def _call_api(self, voice_id: str, text: str) -> tuple[bytes, list[dict]]:
        """POST to ElevenLabs TTS with-timestamps endpoint; return (audio_bytes, word_timestamps)."""
        import base64
        url = _ELEVENLABS_API_URL_WITH_TIMESTAMPS.format(voice_id=voice_id)
        payload = {
            "text":           text,
            "model_id":       _MODEL_ID,
            "voice_settings": VOICE_SETTINGS,
        }
        headers = {
            "xi-api-key":   self.api_key,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }
        response = httpx.post(url, json=payload, headers=headers, timeout=30.0)
        response.raise_for_status()
        data = response.json()
        audio_bytes = base64.b64decode(data["audio_base64"])
        alignment = data.get("alignment", {})
        word_timestamps = self._extract_word_timestamps(alignment)
        return audio_bytes, word_timestamps

    @staticmethod
    def _extract_word_timestamps(alignment: dict) -> list[dict]:
        """Extract word-level timestamps from ElevenLabs alignment data.

        Args:
            alignment: Dict with 'characters', 'character_start_times_seconds',
                       'character_end_times_seconds' lists.

        Returns:
            List of dicts: [{text, start_time, end_time}, ...]
        """
        chars = alignment.get("characters", [])
        starts = alignment.get("character_start_times_seconds", [])
        ends = alignment.get("character_end_times_seconds", [])

        if not chars:
            return []

        words: list[dict] = []
        buf_chars: list[str] = []
        buf_starts: list[float] = []
        buf_ends: list[float] = []

        def _flush():
            if buf_chars:
                words.append({
                    "text":       "".join(buf_chars),
                    "start_time": buf_starts[0],
                    "end_time":   buf_ends[-1],
                })
                buf_chars.clear()
                buf_starts.clear()
                buf_ends.clear()

        for i, ch in enumerate(chars):
            s = starts[i] if i < len(starts) else 0.0
            e = ends[i] if i < len(ends) else 0.0
            if ch in (" ", "\n", "\t"):
                _flush()
            else:
                buf_chars.append(ch)
                buf_starts.append(s)
                buf_ends.append(e)

        _flush()
        return words

    def _normalize_audio(self, audio_path: Path) -> None:
        """
        Normalize audio loudness using pydub (pure Python, no temp files).
        Reads and writes in-memory — no subprocess, no renaming, no file locking.
        """
        try:
            from pydub import AudioSegment
            from pydub.effects import normalize

            audio = AudioSegment.from_mp3(str(audio_path))
            normalized = normalize(audio)
            normalized.export(str(audio_path), format="mp3")
            logger.info(
                "[voiceover] Normalized audio to %0.1f LUFS: %s",
                TARGET_LUFS, audio_path,
            )
        except Exception as exc:
            logger.warning(
                "[voiceover] Normalization failed (continuing): %s", exc
            )

    def _get_duration(self, audio_path: Path) -> float:
        """Get MP3 duration in seconds using mutagen."""
        try:
            from mutagen.mp3 import MP3
            audio = MP3(str(audio_path))
            return float(audio.info.length)
        except Exception as exc:
            logger.warning("Could not read audio duration: %s", exc)
            return 0.0

    @staticmethod
    def _validate_duration(duration: float) -> list[str]:
        """Validate duration — only enforces a minimum; no upper bound.

        The video length will extend to match the full voiceover duration.
        """
        if duration < MIN_DURATION_SECONDS:
            return [f"duration {duration:.1f}s is below minimum {MIN_DURATION_SECONDS}s"]
        return []
