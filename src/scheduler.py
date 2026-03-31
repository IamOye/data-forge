"""
scheduler.py -- DataForge APScheduler job runner

Runs DataForge production pipeline 4x/day on WAT schedule.
Entry point for Railway deployment (Procfile: web: python src/scheduler.py).

Jobs (all times WAT = Africa/Lagos = UTC+1):
  06:00 — data_refresh_job:  pre-fetch and cache daily data batch
  07:00 — production_job:    morning / kinetic
  13:00 — production_job:    midday  / narrative
  19:00 — production_job:    evening / bar_race
  01:00 — production_job:    midnight / split
  00:30 — analytics_job:     Phase 3 placeholder
  08:05 — quota_reset_job:   reset YouTube daily quota counter

Telegram bot commands: /queue /stats /quota /refresh /skip

Usage:
    python src/scheduler.py            # start scheduler + telegram bot
    python src/scheduler.py --test     # run startup checks, print schedule, exit
"""

import logging
import os
import sqlite3
import sys
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.environ.get("DATAFORGE_DB_PATH", "data/processed/data_forge.db")
TIMEZONE = "Africa/Lagos"  # WAT = UTC+1

# Module-level data cache populated by data_refresh_job
DATA_CACHE: dict[str, Any] = {}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _db_connect() -> sqlite3.Connection:
    """Connect to the DataForge SQLite database."""
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def _get_setting(key: str) -> str:
    """Read a value from the settings table."""
    conn = _db_connect()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else ""
    finally:
        conn.close()


def _set_setting(key: str, value: str) -> None:
    """Write a value to the settings table."""
    conn = _db_connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
            (key, value),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Scheduled jobs
# ---------------------------------------------------------------------------

def data_refresh_job() -> None:
    """Fetch and cache the daily data batch from all sources."""
    try:
        from src.data.data_fetcher import DataFetcher
        fetcher = DataFetcher()
        total = 0

        # Stock movers
        try:
            movers = fetcher.fetch_daily_movers(top_n=10)
            DATA_CACHE["movers"] = movers
            total += len(movers)
            logger.info("[dataforge] data_refresh: %d stock movers fetched", len(movers))
        except Exception as e:
            logger.error("[dataforge] data_refresh: stock movers failed: %s", e)
            DATA_CACHE.setdefault("movers", [])

        # Crypto movers
        try:
            crypto = fetcher.fetch_crypto_movers(top_n=5)
            DATA_CACHE["crypto"] = crypto
            total += len(crypto)
            logger.info("[dataforge] data_refresh: %d crypto movers fetched", len(crypto))
        except Exception as e:
            logger.error("[dataforge] data_refresh: crypto movers failed: %s", e)
            DATA_CACHE.setdefault("crypto", [])

        # CPI data from FRED
        try:
            cpi = fetcher.fetch_fred_series("CPIAUCSL", periods=6)
            DATA_CACHE["cpi"] = cpi
            cpi_count = len(cpi) if cpi is not None else 0
            total += cpi_count
            logger.info("[dataforge] data_refresh: CPI series fetched (%d points)", cpi_count)
        except Exception as e:
            logger.error("[dataforge] data_refresh: FRED CPI failed: %s", e)
            DATA_CACHE.setdefault("cpi", None)

        DATA_CACHE["refreshed_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("[dataforge] data_refresh_job complete: %d total data points cached", total)

    except Exception as e:
        logger.error("[dataforge] data_refresh_job failed: %s", e)


def production_job(slot: str, format_type: str) -> None:
    """Run the production pipeline for one slot."""
    try:
        # Only kinetic is implemented; fall back for other formats
        actual_format = format_type
        if format_type in ("bar_race", "split", "narrative"):
            logger.warning(
                "[dataforge] production_job: format '%s' not yet implemented "
                "-- falling back to 'kinetic'",
                format_type,
            )
            actual_format = "kinetic"

        logger.info(
            "[dataforge] production_job starting: slot=%s, format=%s (actual=%s)",
            slot, format_type, actual_format,
        )

        from src.pipeline.production_pipeline import ProductionPipeline
        pipeline = ProductionPipeline()
        result = pipeline.produce()

        if result.get("success"):
            logger.info(
                "[dataforge] production_job [%s] SUCCESS: story_id=%s, url=%s",
                slot, result.get("story_id"), result.get("youtube_url"),
            )
        else:
            logger.error(
                "[dataforge] production_job [%s] FAILED: story_id=%s, error=%s",
                slot, result.get("story_id"), result.get("error"),
            )

    except Exception as e:
        logger.error("[dataforge] production_job [%s] crashed: %s", slot, e)


def analytics_job() -> None:
    """Placeholder for Phase 3 analytics harvest."""
    try:
        logger.info("[dataforge] analytics_job: placeholder -- Phase 3")
    except Exception as e:
        logger.error("[dataforge] analytics_job failed: %s", e)


def quota_reset_job() -> None:
    """Reset YouTube daily quota counter."""
    try:
        _set_setting("youtube_units_used_today", "0")
        _set_setting("youtube_units_reset_date", date.today().isoformat())
        logger.info("[dataforge] quota_reset_job: YouTube units reset to 0")
    except Exception as e:
        logger.error("[dataforge] quota_reset_job failed: %s", e)


# ---------------------------------------------------------------------------
# Startup checks
# ---------------------------------------------------------------------------

def run_startup_checks() -> None:
    """Run pre-flight checks. Log warnings but never abort."""
    checks_passed = 0
    checks_total = 5

    # 1. ANTHROPIC_API_KEY
    if os.environ.get("ANTHROPIC_API_KEY"):
        logger.info("[dataforge] Startup: ANTHROPIC_API_KEY is set")
        checks_passed += 1
    else:
        logger.warning("[dataforge] Startup: ANTHROPIC_API_KEY not set -- script generation will fail")

    # 2. ELEVENLABS_API_KEY
    if os.environ.get("ELEVENLABS_API_KEY"):
        logger.info("[dataforge] Startup: ELEVENLABS_API_KEY is set")
        checks_passed += 1
    else:
        logger.warning("[dataforge] Startup: ELEVENLABS_API_KEY not set -- voiceover will fail")

    # 3. Database
    db_path = Path(DB_PATH)
    if db_path.exists():
        logger.info("[dataforge] Startup: Database exists at %s", DB_PATH)
        checks_passed += 1
    else:
        logger.warning("[dataforge] Startup: Database not found at %s -- creating inline", DB_PATH)
        try:
            Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(DB_PATH)
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
                CREATE TABLE IF NOT EXISTS chart_cache (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    cache_key   TEXT UNIQUE NOT NULL,
                    chart_path  TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    expires_at  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT
                );
            """)
            conn.execute("""
                INSERT OR IGNORE INTO settings (key, value)
                VALUES
                    ('youtube_units_used_today', '0'),
                    ('youtube_units_reset_date', ''),
                    ('dataforge_version', '0.1.0')
            """)
            conn.commit()
            conn.close()
            checks_passed += 1
            logger.info("[dataforge] Startup: Database created at %s", DB_PATH)
        except Exception as e:
            logger.error("[dataforge] Startup: DB creation failed: %s", e)

    # 4. yfinance smoke test
    try:
        import yfinance as yf
        ticker = yf.Ticker("AAPL")
        price = ticker.fast_info.get("lastPrice", None)
        if price:
            logger.info("[dataforge] Startup: yfinance OK -- AAPL = $%.2f", price)
            checks_passed += 1
        else:
            logger.warning("[dataforge] Startup: yfinance returned no price for AAPL")
    except Exception as e:
        logger.warning("[dataforge] Startup: yfinance check failed: %s", e)

    # 5. FRED_API_KEY
    if os.environ.get("FRED_API_KEY"):
        logger.info("[dataforge] Startup: FRED_API_KEY is set")
        checks_passed += 1
    else:
        logger.warning("[dataforge] Startup: FRED_API_KEY not set -- FRED fetches will fail")

    logger.info(
        "[dataforge] Startup checks: %d/%d passed. Scheduler starting.",
        checks_passed, checks_total,
    )


# ---------------------------------------------------------------------------
# Telegram bot
# ---------------------------------------------------------------------------

def _setup_telegram_bot() -> threading.Thread | None:
    """Set up and return a thread running the Telegram bot. Returns None if token not set."""
    bot_token = os.environ.get("DATAFORGE_TELEGRAM_BOT_TOKEN", "")
    if not bot_token:
        logger.warning("[dataforge] DATAFORGE_TELEGRAM_BOT_TOKEN not set -- Telegram bot disabled")
        return None

    def _run_bot() -> None:
        """Run the Telegram bot with a manual async loop (no signal handlers)."""
        import asyncio

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            from telegram import Update
            from telegram.ext import (
                Application,
                CommandHandler,
                ContextTypes,
            )
        except ImportError:
            logger.error("[dataforge] python-telegram-bot not installed -- Telegram bot disabled")
            return

        async def cmd_queue(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            """Show today's story queue."""
            try:
                conn = _db_connect()
                rows = conn.execute(
                    "SELECT story_id, status, metric_name, story_type "
                    "FROM data_stories WHERE date(created_at) = date('now') "
                    "ORDER BY created_at DESC LIMIT 10"
                ).fetchall()
                conn.close()

                if not rows:
                    await update.message.reply_text("No stories queued today.")
                    return

                lines = ["Today's queue:"]
                for sid, status, metric, stype in rows:
                    lines.append(f"  {status:>10} | {stype:<10} | {metric} ({sid})")
                await update.message.reply_text("\n".join(lines))
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")

        async def cmd_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            """Show total uploads and views."""
            try:
                conn = _db_connect()
                row = conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(views_24h), 0) FROM video_log"
                ).fetchone()
                conn.close()
                total, views = row
                await update.message.reply_text(
                    f"Total uploads: {total}\nTotal views (24h): {views:,}"
                )
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")

        async def cmd_quota(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            """Show YouTube quota usage."""
            try:
                used = _get_setting("youtube_units_used_today") or "0"
                await update.message.reply_text(
                    f"YouTube quota used today: {used}/8000 units"
                )
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")

        async def cmd_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            """Trigger data_refresh_job manually."""
            try:
                await update.message.reply_text("Refreshing data...")
                data_refresh_job()
                count = len(DATA_CACHE.get("movers", []))
                await update.message.reply_text(
                    f"Data refresh complete. {count} movers cached."
                )
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")

        async def cmd_skip(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            """Mark next QUEUED story as SKIPPED."""
            try:
                conn = _db_connect()
                row = conn.execute(
                    "SELECT story_id FROM data_stories "
                    "WHERE status = 'QUEUED' ORDER BY created_at ASC LIMIT 1"
                ).fetchone()
                if not row:
                    conn.close()
                    await update.message.reply_text("No QUEUED stories to skip.")
                    return
                story_id = row[0]
                conn.execute(
                    "UPDATE data_stories SET status = 'SKIPPED' WHERE story_id = ?",
                    (story_id,),
                )
                conn.commit()
                conn.close()
                await update.message.reply_text(f"Skipped: {story_id}")
            except Exception as e:
                await update.message.reply_text(f"Error: {e}")

        async def run() -> None:
            """Initialize, start, and poll without signal handlers."""
            app = Application.builder().token(bot_token).build()
            app.add_handler(CommandHandler("queue", cmd_queue))
            app.add_handler(CommandHandler("stats", cmd_stats))
            app.add_handler(CommandHandler("quota", cmd_quota))
            app.add_handler(CommandHandler("refresh", cmd_refresh))
            app.add_handler(CommandHandler("skip", cmd_skip))

            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)

            logger.info("[dataforge] Telegram bot started (manual async polling)")

            # Keep running until stopped
            while True:
                await asyncio.sleep(3600)

        try:
            loop.run_until_complete(run())
        except Exception as e:
            logger.error("[dataforge] Telegram bot error: %s", e)
        finally:
            loop.close()

    thread = threading.Thread(target=_run_bot, daemon=True, name="telegram-bot")
    return thread


# ---------------------------------------------------------------------------
# Scheduler builder
# ---------------------------------------------------------------------------

def build_scheduler():
    """Build and return the configured APScheduler BackgroundScheduler."""
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger

    scheduler = BackgroundScheduler(timezone=TIMEZONE)

    # 06:00 WAT — data refresh
    scheduler.add_job(
        data_refresh_job,
        CronTrigger(hour=6, minute=0, timezone=TIMEZONE),
        id="data_refresh",
        name="Data Refresh (06:00 WAT)",
        replace_existing=True,
    )

    # 07:00 WAT — morning / kinetic
    scheduler.add_job(
        production_job,
        CronTrigger(hour=7, minute=0, timezone=TIMEZONE),
        args=["morning", "kinetic"],
        id="production_morning",
        name="Production: morning/kinetic (07:00 WAT)",
        replace_existing=True,
    )

    # 13:00 WAT — midday / narrative
    scheduler.add_job(
        production_job,
        CronTrigger(hour=13, minute=0, timezone=TIMEZONE),
        args=["midday", "narrative"],
        id="production_midday",
        name="Production: midday/narrative (13:00 WAT)",
        replace_existing=True,
    )

    # 19:00 WAT — evening / bar_race
    scheduler.add_job(
        production_job,
        CronTrigger(hour=19, minute=0, timezone=TIMEZONE),
        args=["evening", "bar_race"],
        id="production_evening",
        name="Production: evening/bar_race (19:00 WAT)",
        replace_existing=True,
    )

    # 01:00 WAT — midnight / split
    scheduler.add_job(
        production_job,
        CronTrigger(hour=1, minute=0, timezone=TIMEZONE),
        args=["midnight", "split"],
        id="production_midnight",
        name="Production: midnight/split (01:00 WAT)",
        replace_existing=True,
    )

    # 00:30 WAT — analytics placeholder
    scheduler.add_job(
        analytics_job,
        CronTrigger(hour=0, minute=30, timezone=TIMEZONE),
        id="analytics",
        name="Analytics Harvest (00:30 WAT)",
        replace_existing=True,
    )

    # 08:05 WAT — quota reset
    scheduler.add_job(
        quota_reset_job,
        CronTrigger(hour=8, minute=5, timezone=TIMEZONE),
        id="quota_reset",
        name="Quota Reset (08:05 WAT)",
        replace_existing=True,
    )

    return scheduler


# ---------------------------------------------------------------------------
# Exported helpers (used by main.py)
# ---------------------------------------------------------------------------

def run_startup_tasks() -> None:
    """Run startup checks and initial data refresh."""
    run_startup_checks()
    logger.info("[dataforge] Running initial data refresh...")
    data_refresh_job()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point: parse --test flag, run checks, start scheduler + bot."""
    test_mode = "--test" in sys.argv

    # --- Startup checks ---
    run_startup_checks()

    # --- Build scheduler ---
    scheduler = build_scheduler()

    if test_mode:
        print("\n[dataforge] --test mode: printing schedule and exiting.\n")
        print("Registered jobs:")
        print(f"  {'ID':<25} {'Name':<45} {'Next run (WAT)'}")
        print(f"  {'-'*25} {'-'*45} {'-'*20}")
        for job in sorted(scheduler.get_jobs(), key=lambda j: j.id):
            # Compute next run time for display
            trigger = job.trigger
            next_fire = "N/A"
            try:
                from apscheduler.triggers.cron import CronTrigger
                if isinstance(trigger, CronTrigger):
                    fields = {f.name: str(f) for f in trigger.fields}
                    next_fire = f"{fields.get('hour', '?')}:{fields.get('minute', '?')} daily"
            except Exception:
                pass
            print(f"  {job.id:<25} {job.name:<45} {next_fire}")

        print(f"\n  Timezone: {TIMEZONE}")
        print(f"  DB path:  {DB_PATH}")
        print(f"\n  Telegram bot: {'enabled' if os.environ.get('DATAFORGE_TELEGRAM_BOT_TOKEN') else 'disabled (no token)'}")
        print("\n[dataforge] Test complete. Exiting.\n")
        return

    # --- Start scheduler ---
    scheduler.start()
    logger.info("[dataforge] Scheduler started with %d jobs", len(scheduler.get_jobs()))

    # --- Start Telegram bot ---
    bot_thread = _setup_telegram_bot()
    if bot_thread:
        bot_thread.start()
        logger.info("[dataforge] Telegram bot thread started")

    # --- Block forever ---
    logger.info("[dataforge] Scheduler running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("[dataforge] Shutting down scheduler...")
        scheduler.shutdown(wait=False)
        logger.info("[dataforge] Shutdown complete.")


if __name__ == "__main__":
    main()
