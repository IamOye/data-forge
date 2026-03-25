# ChannelForge 🎬

**Fully automated YouTube Shorts production system.**  
Research → Script → Voiceover → B-Roll → Captions → Upload — zero daily intervention required.

[![Tests](https://img.shields.io/badge/tests-1190%20passing-brightgreen)](https://github.com/IamOye/channel-forge)
[![Python](https://img.shields.io/badge/python-3.11-blue)](https://python.org)
[![Railway](https://img.shields.io/badge/deployed-Railway-blueviolet)](https://railway.app)

---

## What it does

ChannelForge runs 24/7 on Railway cloud and produces 4 YouTube Shorts per day for the [@moneyheresy](https://youtube.com/@moneyheresy) channel — a finance/career channel targeting Western audiences.

**Daily pipeline (fully automatic):**

```
07:30 WAT  Competitor research (YouTube autocomplete + trends)
08:05 WAT  Quota recovery — retry any failed uploads
09:00 WAT  Production run 1
13:00 WAT  Production run 2
19:00 WAT  Production run 3
01:00 WAT  Production run 4
Hourly     Comment monitoring + lead magnet delivery
Monday     Google Sheet topic queue sync (28 topics/week)
```

**Each production run:**
1. Picks next topic from manual queue (Google Sheet) or AI fallback
2. Generates hook, script, and voiceover (ElevenLabs)
3. Fetches b-roll clips from Pixabay (aerial-first strategy)
4. Renders VIZIONTIA-style captions (Impact font, gold highlight, black stroke)
5. Runs quality gate — rejects poor videos before upload
6. Uploads to YouTube with metadata and thumbnail
7. Sends Telegram notification with video URL

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    RAILWAY (Cloud)                  │
│                                                     │
│  APScheduler → Orchestrator → Production Pipeline  │
│                    ↓                               │
│  Topic Queue  →  Script Gen  →  Voiceover          │
│  (Google Sheet)  (Claude)       (ElevenLabs)       │
│                    ↓                               │
│  B-Roll Fetch →  Caption Render →  Video Build     │
│  (Pixabay)        (Pillow/FFmpeg)  (FFmpeg)        │
│                    ↓                               │
│  Quality Gate →  YouTube Upload →  Telegram Alert  │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│                LOCAL (Laptop)                       │
│                                                     │
│  tools/research.py  — Reddit scraping + scoring    │
│  tools/quick_add.py — Single topic fast add        │
│  tools/local_telegram_listener.py — Phone control  │
└─────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Runtime | Python 3.11 |
| Deployment | Railway (asia-southeast1) |
| Scheduler | APScheduler |
| AI — Scripts | Anthropic Claude Sonnet |
| AI — Scoring/Rewriting | Anthropic Claude Haiku |
| Voiceover | ElevenLabs (Adam voice) |
| B-Roll | Pixabay API |
| Captions | Pillow + FFmpeg |
| Video Assembly | FFmpeg + MoviePy |
| Topic Queue | Google Sheets API (gspread) |
| Notifications | Telegram Bot API |
| Database | SQLite (Railway persistent volume) |
| Tests | pytest (1,190 passing) |

---

## Repository Structure

```
channel-forge/
├── src/
│   ├── content/
│   │   ├── hook_generator.py      # Hook selection + scoring
│   │   ├── script_generator.py    # Script + CTA generation
│   │   └── metadata_generator.py  # YouTube title/desc/tags
│   ├── crawler/
│   │   ├── reddit_scraper.py      # Reddit finance subreddits
│   │   ├── competitor_scraper.py  # YouTube autocomplete
│   │   ├── trend_scraper.py       # Google Trends
│   │   └── gsheet_topic_sync.py   # Google Sheet read/write
│   ├── media/
│   │   ├── voiceover.py           # ElevenLabs TTS
│   │   ├── pixabay_fetcher.py     # B-roll fetch + scoring
│   │   ├── caption_renderer.py    # VIZIONTIA-style captions
│   │   └── video_builder.py       # FFmpeg video assembly
│   ├── pipeline/
│   │   ├── production_pipeline.py # Main pipeline orchestration
│   │   ├── topic_queue.py         # Topic priority logic
│   │   └── multi_channel_orchestrator.py
│   ├── publisher/
│   │   ├── youtube_uploader.py    # YouTube Data API v3
│   │   ├── comment_responder.py   # Comment monitoring + DM
│   │   └── telegram_reply_handler.py # Telegram commands
│   ├── research/
│   │   └── research_engine.py     # 4-phase research pipeline
│   └── scheduler.py               # APScheduler job definitions
├── tools/
│   ├── research.py                # Local Reddit research CLI
│   ├── quick_add.py               # Single topic fast-add
│   └── local_telegram_listener.py # Laptop → Telegram bridge
├── tests/                         # 1,190 pytest tests
├── data/
│   ├── processed/                 # SQLite databases (Railway volume)
│   └── output/                    # Produced videos (temp)
├── nixpacks.toml                  # Railway build config
├── requirements.txt
└── main.py                        # Entry point
```

---

## Environment Variables

Set these in Railway (production) and `.env` (local development):

### Required — Core
| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Claude API key |
| `ELEVENLABS_API_KEY` | ElevenLabs TTS key |
| `PIXABAY_API_KEY` | Pixabay video/image search |
| `YOUTUBE_CLIENT_SECRET_B64` | YouTube OAuth client secret (base64) |
| `YOUTUBE_TOKEN_B64` | YouTube OAuth token (base64) |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Your Telegram chat ID |

### Required — Google Sheets
| Variable | Description |
|----------|-------------|
| `GOOGLE_SHEET_ID` | Topic queue Google Sheet ID |
| `GOOGLE_CREDENTIALS_B64` | Service account JSON key (base64) |

### Required — Lead Magnets
| Variable | Description |
|----------|-------------|
| `GUMROAD_URL_MONEY` | Gumroad link for SYSTEM trigger |
| `GUMROAD_URL_CAREER` | Gumroad link for AUTOMATE trigger |
| `GUMROAD_URL_SUCCESS` | Gumroad link for BLUEPRINT trigger |

### Optional
| Variable | Description |
|----------|-------------|
| `PEXELS_API_KEY` | Pexels video API (higher quality, pending) |
| `YOUTUBE_API_KEY` | YouTube Data API key for search |

---

## Local Development Setup

### Prerequisites
- Python 3.11+
- Node.js (for docx generation scripts)
- FFmpeg installed and on PATH

### Install

```bash
git clone https://github.com/IamOye/channel-forge.git
cd channel-forge
pip install -r requirements.txt
```

### Configure

```bash
cp .env.example .env
# Fill in your API keys in .env
```

### Run tests

```bash
pytest tests/ -v
```

### Run local research tool

```bash
# Reddit research (must run locally — Railway IPs blocked)
python tools/research.py --source reddit

# All sources
python tools/research.py

# Fast scan without rewriting
python tools/research.py --no-rewrite

# After session — sync to Google Sheet
python tools/research.py --sync
```

### Add a single topic

```bash
python tools/quick_add.py "Why your pension will not be enough" money
```

---

## Topic Queue Workflow

ChannelForge uses a Google Sheet as the editorial queue:

1. **Research** — Run `tools/research.py` locally (Reddit) or `/research` on Telegram (other sources)
2. **Approve** — Select topics interactively, they go to Google Sheet as READY
3. **Sync** — Every Monday 06:00 WAT, ChannelForge pulls next 28 READY topics
4. **Produce** — Production runs consume topics in SEQ order
5. **Mark used** — After upload, row updated to USED with video ID and date

**Priority order in production:**
1. `manual_topics` table (from Google Sheet, QUEUED status, SEQ ascending)
2. `scored_topics` table (AI-scraped from competitor/autocomplete/trends)
3. Claude-generated fallback topic

---

## Telegram Commands

Control ChannelForge entirely from your phone:

```
Research:
  /research              All non-Reddit sources
  /research competitor   Competitor channels only
  /research trends       Google Trends only
  /researchstatus        Check active session

Review (during research session):
  /next                  Next 5 topics
  /add 1,3,5-8           Add to Google Sheet
  /skip 2,4              Exclude permanently
  /edit 3                Edit a title
  /done                  End session

Queue management:
  /synctopics            Manual Monday sync
  /listtopics            Next 7 queued topics
  /weeklystatus          Production summary
  /addtopic money [title] Add single topic

System:
  /held                  Quality gate holds
  /status                Quota + next run
```

> **Note:** Reddit research (`/research reddit`) must run locally on a laptop. Railway datacenter IPs are blocked by Reddit.

---

## Caption Style (VIZIONTIA)

Captions render directly on video frame — no background box:

- **Font:** Impact / DejaVu Sans Bold (truetype, never bitmap)
- **Size:** `round(canvas_width × 0.155)` — scales with resolution
  - 360px canvas → 56px
  - 1080px canvas → 167px
- **Style:** ALL CAPS, white text, 2–3px black stroke
- **Highlight:** Current word in `#FFD700` gold (text colour only)
- **Position:** 75–80% from top, centred horizontally

---

## B-Roll Strategy

Aerial-first approach for consistent cinematic quality:

```
7 queries  →  Aerial/drone footage (city skylines, highways, 
               coastlines, luxury areas)
3 queries  →  Human footage (office workers, professionals —
               only when script references people directly)
2 queries  →  Financial imagery (money, charts, buildings)
```

Relevance scoring via Claude Haiku before download:
- Aerial/drone: 8–10
- Finance/professional: 6–7  
- Irrelevant (animals, food, sports): 1–3
- Threshold: score ≥ 6 to pass

---

## Quality Gate

Every video passes a pre-upload check:

| Check | Threshold | Action on fail |
|-------|-----------|----------------|
| Clip diversity | < 1 unique clip per 8s | Hold + Telegram alert |
| Clip dominance | Any clip > 15s | Hold + Telegram alert |
| Caption font size | < 40px | Hold + Telegram alert |

Failed videos saved to `quality_holds` table. Use `/held` on Telegram to review.

---

## CTA Strategy

Every video ends with a combined subscribe + lead magnet CTA:

| Category | CTA | Trigger Keyword |
|----------|-----|----------------|
| money | "Subscribe — we expose this stuff daily. Comment SYSTEM for the 5-day money reset free." | SYSTEM |
| career | "Subscribe if nobody told you this before. Comment AUTOMATE for the salary playbook free." | AUTOMATE |
| success | "Subscribe — we drop uncomfortable truths daily. Comment BLUEPRINT for the AI advantage guide free." | BLUEPRINT |

When a viewer comments the trigger keyword, ChannelForge sends a Telegram alert. You approve, and the bot replies with the Gumroad link.

---

## Deployment (Railway)

```bash
# Railway auto-deploys on git push to main
git push origin main
```

Railway build installs system fonts via `nixpacks.toml`:
```toml
[phases.setup]
nixPkgs = ["dejavu_fonts", "liberation_ttf", "ffmpeg"]
```

Persistent volume mounted at `/app/data/processed/` for SQLite databases.

---

## Running Tests

```bash
# All tests
pytest tests/ -v

# Specific module
pytest tests/test_caption_renderer.py -v
pytest tests/test_research_engine.py -v

# With coverage
pytest tests/ --cov=src --cov-report=term-missing
```

---

## Roadmap

- [ ] Pexels API integration (higher quality vertical stock)
- [ ] Title deduplication check (cosine similarity > 0.85)
- [ ] Telegram offset persistence across redeploys
- [ ] Multi-niche channel support (AI/Tech, Legal, Self-Improvement)
- [ ] Web dashboard (Phase 2 productization)
- [ ] Analytics reporting (views, CTR, subscriber growth)

---

## Lessons Learnt

A running register of 31 lessons from production is maintained in `docs/ChannelForge_Lessons_Learnt_v3.xlsx`. Key findings:

- `ImageFont.load_default()` in Pillow ignores the size parameter entirely — always use `truetype()`
- Railway datacenter IPs are blocked by Reddit — Reddit scraping must run locally
- `gspread` 6.x `get_all_records(expected_headers=...)` is unreliable — use `get_all_values()` + manual dict building
- YouTube API quota (10,000 units/day) requires careful scheduling — competitor research at 07:30 WAT preserves quota for production runs

---

## License

Private repository. All rights reserved.  
© 2026 Olusegun Ogunbiyi / ProjectShield

---

*Built with Claude Opus 4.6 · Deployed on Railway · Producing @moneyheresy*
