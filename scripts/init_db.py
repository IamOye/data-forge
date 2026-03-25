"""
DataForge — SQLite schema initialiser.
Run once before first deployment: python scripts/init_db.py
"""

import sqlite3
import os

DB_PATH = os.environ.get('DATAFORGE_DB_PATH', 'data/processed/data_forge.db')

def init_db(db_path: str = DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.executescript("""
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

    cur.execute("""
        INSERT OR IGNORE INTO settings (key, value)
        VALUES
            ('youtube_units_used_today', '0'),
            ('youtube_units_reset_date', ''),
            ('dataforge_version', '0.1.0')
    """)

    conn.commit()
    conn.close()
    print(f'[dataforge] Database initialised at: {db_path}')

if __name__ == '__main__':
    init_db()
    print('[dataforge] Schema complete. Tables: data_stories, video_log, chart_cache, settings.')
