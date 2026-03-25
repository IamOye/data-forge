#!/usr/bin/env python3
"""
research.py — ChannelForge Local Research Tool

Thin CLI wrapper around ResearchEngine. Scrapes, scores, rewrites, and
presents an interactive ranked list for manual curation.

Usage:
    python tools/research.py                        # full scrape, top 50
    python tools/research.py --source reddit        # Reddit only
    python tools/research.py --source competitor    # competitor channels only
    python tools/research.py --category money       # filter to money
    python tools/research.py --count 100            # show top 100
    python tools/research.py --no-score             # skip Claude scoring
    python tools/research.py --no-rewrite           # skip auto-rewrite
    python tools/research.py --sync                 # push reviewed log to Sheet

Requires: pip install rich gspread google-auth anthropic httpx
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(_PROJECT_ROOT / ".env")


# ---------------------------------------------------------------------------
# Display with Rich
# ---------------------------------------------------------------------------

def display_topics(scored: list, count: int = 50, offset: int = 0) -> None:
    """Print ranked table of scored topics using rich."""
    from rich.console import Console
    from src.research.research_engine import _safe_str, _safe_int, _safe_float

    console = Console()
    today = datetime.now().strftime("%Y-%m-%d")
    total = len(scored)
    showing = scored[offset:offset + count]

    if not showing:
        console.print("[yellow]No topics to display.[/]")
        return

    # Summary stats
    all_scores = [t.score for t in scored if t.score > 0]
    top_hook = max((t.hook_strength for t in scored), default=0)
    avg_score = sum(all_scores) / max(len(all_scores), 1)
    n_rewritten = sum(1 for t in scored if t.original_title)

    console.print()
    console.print("─" * 70, style="bold blue")
    console.print(f"CHANNELFORGE TOPIC RESEARCH — {today}", style="bold white")
    console.print(
        f"Scored: {total} | Top hook: {top_hook:.1f} | Avg: {avg_score:.1f} | "
        f"{n_rewritten} rewritten | Showing {offset + 1}–{offset + len(showing)}",
        style="dim",
    )
    console.print("─" * 70, style="bold blue")
    console.print()

    for i, t in enumerate(showing, start=offset + 1):
        if t.score >= 8:
            score_style = "bold green"
        elif t.score >= 6:
            score_style = "yellow"
        else:
            score_style = "dim"

        cat = _safe_str(t.category) or "money"
        cat_colors = {"money": "green", "career": "cyan", "success": "magenta"}
        cat_style = cat_colors.get(cat, "white")
        title_display = _safe_str(t.title) or "(untitled)"

        # Score display with hook sub-score
        hook_str = f" (H:{t.hook_strength:.1f})" if t.hook_strength > 0 else ""
        rewrite_tag = "[cyan]✏️ [/]" if t.original_title else "  "

        console.print(
            f" [{score_style}]{i:>3}[/]  "
            f"[{score_style}]{t.score:.1f}{hook_str}[/]  "
            f"[{cat_style}]{cat:<8}[/] "
            f"{rewrite_tag}[bold white]{title_display}[/]"
        )

        # Show original if rewritten
        if t.original_title and t.original_title != t.title:
            console.print(f"              [dim]Original: {t.original_title}[/]")

        hook = _safe_str(t.hook_angle)
        if hook:
            console.print(f"              [dim italic]Hook: {hook}[/]")

        # Improvement reason from rewrite
        if t.original_title and t.reason:
            console.print(f"              [dim]Improvement: {t.reason}[/]")

        # Source
        source_str = _safe_str(t.source) or "unknown"
        detail = _safe_str(t.source_detail)
        if detail:
            source_str += f"/{detail}"
        if t.score_hint > 0:
            if t.source == "reddit":
                source_str += f" ({_safe_int(t.score_hint)} upvotes)"
            elif t.source == "competitor":
                source_str += f" ({_safe_int(t.score_hint):,} views)"
        console.print(f"              [dim]From: {source_str}[/]")
        console.print()

    console.print("─" * 70, style="bold blue")


# ---------------------------------------------------------------------------
# Google Sheets integration (delegates to engine)
# ---------------------------------------------------------------------------

def add_to_sheet(topics: list) -> tuple[int, int]:
    """Append scored topics to Google Sheet. Returns (first_seq, last_seq)."""
    import json
    import os
    import base64
    from datetime import datetime as dt

    import gspread
    from google.oauth2.service_account import Credentials

    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in .env")

    creds_b64 = os.getenv("GOOGLE_CREDENTIALS_B64", "")
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "")
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    if creds_b64:
        _b64 = creds_b64.strip()
        _missing = len(_b64) % 4
        if _missing:
            _b64 += "=" * (4 - _missing)
        creds = Credentials.from_service_account_info(json.loads(base64.b64decode(_b64)), scopes=scopes)
    elif creds_file:
        creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    else:
        raise ValueError("Set GOOGLE_CREDENTIALS_B64 or GOOGLE_CREDENTIALS_FILE in .env")

    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(sheet_id)

    try:
        ws = spreadsheet.worksheet("Topic Queue")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet("Topic Queue", rows=500, cols=11)
        ws.update("A1:K1", [["#", "SEQ", "Title / Topic", "Category", "Hook Angle (optional)",
                              "Status", "Priority", "Date Added", "Date Used", "Video ID", "Notes"]])

    col_b = ws.col_values(2)  # SEQ column
    nums = []
    for v in col_b[1:]:
        try:
            nums.append(int(v))
        except (ValueError, TypeError):
            pass
    seq = max(nums, default=0) + 1
    first_seq = seq
    today = dt.now().strftime("%d-%b-%y")
    row_num = len(ws.col_values(1)) + 1

    rows = []
    for t in topics:
        from src.research.research_engine import _safe_str, _safe_float
        notes = f"Source: {_safe_str(t.source)}"
        if t.source_detail:
            notes += f"/{_safe_str(t.source_detail)}"
        notes += f" | Score: {t.score:.1f}"
        if t.reason:
            notes += f" | {_safe_str(t.reason)}"

        rows.append([row_num, seq, t.title, t.category, t.hook_angle,
                      "READY", "MEDIUM", today, "", "", notes])
        seq += 1
        row_num += 1

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

    return first_seq, seq - 1


# ---------------------------------------------------------------------------
# Interactive Review
# ---------------------------------------------------------------------------

def interactive_review(engine, scored: list, count: int = 50, session_id: str = "") -> None:
    """Interactive loop: display topics, accept commands."""
    from rich.console import Console
    console = Console()
    offset = 0

    while True:
        showing = scored[offset:offset + count]
        if not showing:
            console.print("[yellow]No more topics to show.[/]")
            break

        display_topics(scored, count=count, offset=offset)

        console.print(
            "[bold]Commands:[/] Enter topic #s (e.g. 1,3,5-8) then "
            "[green][A]dd[/]  [yellow][S]kip[/]  "
            "[cyan][E]dit #[/]  [blue][R]efresh[/]  [red][Q]uit[/]"
        )
        console.print()

        selected_indices: list[int] = []

        while True:
            try:
                cmd = input("→ ").strip()
            except (EOFError, KeyboardInterrupt):
                # Mark all displayed-but-unacted as skipped
                _mark_unacted_skipped(engine, scored, offset, count, session_id)
                console.print("\n[dim]Goodbye.[/]")
                return

            if not cmd:
                continue
            upper = cmd.upper()

            if upper == "Q":
                _mark_unacted_skipped(engine, scored, offset, count, session_id)
                console.print("[dim]Goodbye.[/]")
                return

            if upper == "R":
                offset += count
                break

            if upper.startswith("E"):
                parts = cmd.split(None, 1)
                num_str = parts[1] if len(parts) > 1 else parts[0][1:]
                try:
                    from src.research.research_engine import ScoredTopic
                    idx = int(num_str) - 1
                    if 0 <= idx < len(scored):
                        new_title = input(f"  New title [{scored[idx].title}]: ").strip()
                        if new_title:
                            old = scored[idx]
                            scored[idx] = ScoredTopic(
                                title=new_title, score=old.score, category=old.category,
                                hook_angle=old.hook_angle, reason=old.reason,
                                source=old.source, source_detail=old.source_detail,
                                score_hint=old.score_hint,
                                hook_strength=old.hook_strength, contrarian=old.contrarian,
                                specificity=old.specificity, brand_fit=old.brand_fit,
                                search_demand=old.search_demand,
                                original_title=old.original_title or old.title,
                                rewritten_score=old.rewritten_score,
                            )
                            console.print(f"  [green]Updated #{idx + 1}[/]")
                    else:
                        console.print(f"  [red]#{idx + 1} out of range[/]")
                except ValueError:
                    console.print("  [red]Usage: E <number>[/]")
                continue

            if upper == "A":
                if not selected_indices:
                    console.print("[yellow]No topics selected. Enter numbers first.[/]")
                    continue
                to_add = [scored[i] for i in selected_indices if 0 <= i < len(scored)]
                if not to_add:
                    console.print("[yellow]No valid topics selected.[/]")
                    continue
                try:
                    console.print(f"[dim]Adding {len(to_add)} topics to Google Sheet...[/]")
                    first, last = add_to_sheet(to_add)
                    for t in to_add:
                        console.print(f"  SEQ #{first}: {t.title}")
                        engine.mark_reviewed(
                            title=t.title, action="added", session_id=session_id,
                            score=t.score, category=t.category, source=t.source,
                            original_title=t.original_title,
                        )
                        first += 1
                    console.print(f"[bold green]✅ Done. {len(to_add)} topics added.[/]")
                except Exception as exc:
                    console.print(f"[bold red]Google Sheet error: {exc}[/]")
                selected_indices = []
                continue

            if upper == "S":
                if selected_indices:
                    to_skip = [scored[i] for i in selected_indices if 0 <= i < len(scored)]
                    engine.mark_batch_reviewed(to_skip, "skipped", session_id)
                    console.print(f"[dim]Skipped {len(to_skip)} — won't appear again.[/]")
                selected_indices = []
                continue

            # Parse topic numbers
            try:
                for part in cmd.split(","):
                    part = part.strip()
                    if "-" in part:
                        lo, hi = part.split("-", 1)
                        for n in range(int(lo), int(hi) + 1):
                            selected_indices.append(n - 1)
                    else:
                        selected_indices.append(int(part) - 1)
                selected_indices = sorted(set(selected_indices))
                titles = [scored[i].title for i in selected_indices if 0 <= i < len(scored)]
                console.print(
                    f"[cyan]Selected {len(titles)} topic(s).[/] "
                    f"Press [green]A[/] to add, [yellow]S[/] to skip."
                )
            except ValueError:
                console.print("[red]Invalid input. Enter numbers, A, S, E #, R, or Q.[/]")


def _mark_unacted_skipped(engine, scored, offset, count, session_id):
    """Mark displayed-but-unacted topics as skipped on quit."""
    showing = scored[offset:offset + count]
    if showing:
        engine.mark_batch_reviewed(showing, "skipped", session_id)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="ChannelForge Topic Research Tool")
    parser.add_argument("--source", type=str, default=None,
                        choices=["reddit", "autocomplete", "trends", "competitor"])
    parser.add_argument("--category", type=str, default=None,
                        choices=["money", "career", "success"])
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--no-score", action="store_true", help="Skip Claude scoring")
    parser.add_argument("--no-rewrite", action="store_true", help="Skip auto-rewrite")
    parser.add_argument("--sync", action="store_true", help="Push reviewed log to Google Sheet")
    args = parser.parse_args()

    from rich.console import Console
    from src.research.research_engine import ResearchEngine, ScoredTopic, _safe_str, _safe_float

    console = Console()

    # --sync mode: push reviewed topics and exit
    if args.sync:
        console.print("[cyan]Syncing reviewed topics to Google Sheet...[/]")
        engine = ResearchEngine()
        count = engine.sync_reviewed_to_sheet()
        console.print(f"[green]✅ Synced {count} reviewed topics.[/]")
        return

    engine = ResearchEngine(
        enable_rewrite=not args.no_rewrite and not args.no_score,
    )

    def progress(msg: str) -> None:
        console.print(f"  {msg}")

    if args.no_score:
        # Quick mode: scrape + dedup only, no Claude calls
        console.print("\n[bold cyan]Scraping (no-score mode)...[/]\n")
        raw = engine.scrape([args.source] if args.source else None)
        if not raw:
            console.print("[bold red]No topics found.[/]")
            return
        progress(f"Scraped {len(raw)} raw topics")

        clean = engine.deduplicate(raw)
        progress(f"{len(clean)} after dedup")

        scored = [
            ScoredTopic(
                title=_safe_str(t.title) or "untitled", score=0.0,
                category="unknown", hook_angle="", reason="",
                source=_safe_str(t.source), source_detail=_safe_str(t.source_detail),
                score_hint=_safe_float(t.score_hint),
            )
            for t in clean
        ]
        scored.sort(key=lambda s: s.score_hint, reverse=True)
    else:
        console.print("\n[bold cyan]Running full research pipeline...[/]\n")
        scored = engine.run(source=args.source, category=args.category, progress_callback=progress)

    if args.category and not args.no_score:
        scored = [s for s in scored if s.category == args.category]

    if not scored:
        console.print("[bold red]No topics to display.[/]")
        return

    interactive_review(engine, scored, count=args.count, session_id="local")


if __name__ == "__main__":
    main()
