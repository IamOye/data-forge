## Project: ChannelForge
Automated pipeline that crawls websites, extracts and normalizes 
titles as market signals, then generates a fully automated 
faceless YouTube channel content system.

## Architecture
Phase 1 — Data Harvesting:
- Crawl a given URL and all sub-pages under the same domain
- Extract all page titles, headings (h1-h3), resource/file titles
- Normalize, deduplicate, and score titles by frequency/relevance
- Output clean dataset to data/processed/ as CSV and SQLite

Phase 2 — YouTube Channel Automation:
- Analyze title dataset as market/demand signals
- Generate video ideas, titles, scripts, descriptions, tags
- Output structured channel content to data/output/
- Templates for thumbnails, voiceover scripts, upload metadata

## Tech Stack
- Python 3.11+
- scrapy + playwright (crawling)
- beautifulsoup4 (HTML parsing)
- pandas + nltk (data cleaning)
- sqlite3 (storage)
- anthropic SDK (content generation)
- APScheduler (scheduling)
- jinja2 (output templates)

## Directory Structure
src/
├── crawler/      # Scrapy spider + Playwright integration
├── extractor/    # Title and resource extraction logic
├── normalizer/   # Text cleaning and deduplication
├── pipeline/     # Orchestrates full Phase 1 flow
└── youtube/      # Content generation and output formatting
data/
├── raw/          # Raw scraped HTML and metadata
├── processed/    # Cleaned title datasets (CSV + SQLite)
└── output/       # Final YouTube channel content packages
templates/        # Jinja2 templates for scripts, metadata
logs/             # Crawl and pipeline logs

## Dev Commands
- `python main.py crawl <url>` — Run full crawl pipeline
- `python main.py generate` — Run YouTube content generator
- `python main.py full <url>` — Run both phases end to end
- `pytest tests/` — Run all tests
- `pip install -r requirements.txt` — Install dependencies

## Coding Standards
- Python 3.11+ type hints on all functions
- Docstrings on all classes and public methods
- Error handling on all network calls (retries + timeouts)
- Log everything to logs/ — never use bare print()
- All config via .env — never hardcode URLs or API keys
- Tests required for all extraction and normalization logic

## Autonomous Operations
- Reading and writing files in src/, data/, templates/, logs/
- Running pytest
- Running pip install
- Git status, diff, add, commit

## Always Requires My Confirmation
- Deleting any data/ files
- Making live API calls to Anthropic (cost implications)
- Pushing to remote repository
- Modifying .env
- Any scheduling/automation that runs unattended
