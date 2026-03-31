"""
story_selector.py — StorySelector

Uses Claude API to select the 4 most compelling stories from a daily data batch,
one per posting slot, each a different format type.

Slots and default format assignments:
  morning  → kinetic
  midday   → narrative
  evening  → bar_race
  midnight → split

Usage:
    from src.data.data_fetcher import DataFetcher
    fetcher = DataFetcher()
    batch = fetcher.fetch_daily_movers(top_n=10)
    selector = StorySelector()
    stories = selector.select_daily_stories(batch)
    for s in stories:
        print(s.slot, s.story_type, batch[s.data_point_index].metric_name)
"""

import sys
import os
# Ensure project root is in path so "from src.x import" works on Railway
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import logging
import os
import re
from collections import namedtuple

import anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

StoryConfig = namedtuple('StoryConfig', [
    'story_type',        # 'kinetic'|'bar_race'|'split'|'narrative'
    'data_point_index',  # index into the data_batch list
    'hook_angle',        # e.g. 'loss framing' | 'record high' | 'comparison'
    'visual_emphasis',   # e.g. 'number animation' | 'bar race' | 'split screen'
    'slot',              # 'morning'|'midday'|'evening'|'midnight'
])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MODEL = "claude-sonnet-4-5"
_MAX_TOKENS = 600

VALID_STORY_TYPES = {'kinetic', 'bar_race', 'split', 'narrative'}
VALID_SLOTS = {'morning', 'midday', 'evening', 'midnight'}

_SYSTEM_PROMPT = (
    "You are a financial data editor for a YouTube Shorts channel. "
    "Given today's economic data points, select the 4 most compelling stories "
    "for short-form video. Each story must use a different format type. "
    "Assign one story per slot: morning=kinetic, midday=narrative, "
    "evening=bar_race, midnight=split. "
    "Return ONLY valid JSON, no markdown:\n"
    "[{\"story_type\": \"...\", \"data_point_index\": 0, \"hook_angle\": \"...\", "
    "\"visual_emphasis\": \"...\", \"slot\": \"...\"}]"
)

# Default slot → story_type mapping for fallback
_DEFAULT_SLOT_MAP = [
    ('morning',  'kinetic',   'number animation'),
    ('midday',   'narrative', 'text overlay'),
    ('evening',  'bar_race',  'bar race'),
    ('midnight', 'split',     'split screen'),
]


# ---------------------------------------------------------------------------
# StorySelector
# ---------------------------------------------------------------------------

class StorySelector:
    """
    Selects 4 daily stories from a data batch using Claude API.

    Falls back to a deterministic default selection if the API call fails
    or returns invalid results.

    Args:
        api_key: Anthropic API key. Reads ANTHROPIC_API_KEY from env if None.
        model:   Claude model to use.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = _MODEL,
    ) -> None:
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self.model = model

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def select_daily_stories(
        self,
        data_batch: list,
        already_used_types: list[str] | None = None,
    ) -> list[StoryConfig]:
        """
        Select 4 stories from the data batch, one per slot.

        Args:
            data_batch:         List of DataPoint namedtuples from data_fetcher.
            already_used_types: Story types already used today (will be excluded).

        Returns:
            List of 4 StoryConfig namedtuples.
        """
        if not data_batch:
            logger.warning("[dataforge] Empty data batch — using default selection")
            return self._default_selection(data_batch)

        if not self.api_key:
            logger.warning("[dataforge] ANTHROPIC_API_KEY not set — using default selection")
            return self._default_selection(data_batch)

        already_used = set(already_used_types or [])
        prompt = self._build_prompt(data_batch, already_used)

        try:
            client = anthropic.Anthropic(api_key=self.api_key)
            message = client.messages.create(
                model=self.model,
                max_tokens=_MAX_TOKENS,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()
            logger.info("[dataforge] StorySelector raw response: %s", raw[:300])

            configs = self._parse_response(raw, data_batch)

            if self._validate(configs, data_batch):
                logger.info("[dataforge] StorySelector: %d valid stories selected", len(configs))
                return configs
            else:
                logger.warning("[dataforge] Validation failed — using default selection")
                return self._default_selection(data_batch)

        except Exception as e:
            logger.error("[dataforge] StorySelector API call failed: %s", e)
            return self._default_selection(data_batch)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_prompt(data_batch: list, already_used: set[str]) -> str:
        """Serialize data_batch as a numbered list for Claude."""
        lines = ["Today's data points:\n"]
        for i, dp in enumerate(data_batch):
            metric = getattr(dp, 'metric_name', str(dp))
            pct = getattr(dp, 'pct_change', 0.0)
            source = getattr(dp, 'data_source', 'unknown')
            lines.append(f"  {i}. {metric} | {pct:+.2f}% | Source: {source}")

        if already_used:
            lines.append(f"\nAlready used today (exclude): {', '.join(already_used)}")

        lines.append(
            "\nSelect the 4 most compelling stories. "
            "Each must use a different format type and slot."
        )
        return "\n".join(lines)

    @staticmethod
    def _parse_response(raw: str, data_batch: list) -> list[StoryConfig]:
        """Parse Claude's JSON response into StoryConfig objects."""
        # Strip markdown fences if present
        clean = re.sub(r"```(?:json)?|```", "", raw).strip()
        items = json.loads(clean)

        configs: list[StoryConfig] = []
        for item in items:
            idx = int(item.get("data_point_index", 0))
            # Clamp index to valid range
            idx = max(0, min(idx, len(data_batch) - 1)) if data_batch else 0
            configs.append(StoryConfig(
                story_type=str(item.get("story_type", "kinetic")),
                data_point_index=idx,
                hook_angle=str(item.get("hook_angle", "")),
                visual_emphasis=str(item.get("visual_emphasis", "")),
                slot=str(item.get("slot", "")),
            ))
        return configs

    @staticmethod
    def _validate(configs: list[StoryConfig], data_batch: list) -> bool:
        """Validate: exactly 4 stories, no duplicate types, no duplicate slots."""
        if len(configs) != 4:
            logger.warning("[dataforge] Expected 4 stories, got %d", len(configs))
            return False

        types = [c.story_type for c in configs]
        slots = [c.slot for c in configs]

        if len(set(types)) != 4:
            logger.warning("[dataforge] Duplicate story_types: %s", types)
            return False

        if len(set(slots)) != 4:
            logger.warning("[dataforge] Duplicate slots: %s", slots)
            return False

        for c in configs:
            if c.story_type not in VALID_STORY_TYPES:
                logger.warning("[dataforge] Invalid story_type: %s", c.story_type)
                return False
            if c.slot not in VALID_SLOTS:
                logger.warning("[dataforge] Invalid slot: %s", c.slot)
                return False
            if data_batch and c.data_point_index >= len(data_batch):
                logger.warning(
                    "[dataforge] data_point_index %d out of range (batch size %d)",
                    c.data_point_index, len(data_batch),
                )
                return False

        return True

    @staticmethod
    def _default_selection(data_batch: list) -> list[StoryConfig]:
        """
        Deterministic fallback: assign first available data points to slots.

        Always returns exactly 4 StoryConfig objects.
        """
        batch_size = len(data_batch) if data_batch else 0
        configs: list[StoryConfig] = []

        for i, (slot, story_type, visual) in enumerate(_DEFAULT_SLOT_MAP):
            idx = i if i < batch_size else 0
            metric = ""
            if data_batch:
                metric = getattr(data_batch[idx], 'metric_name', '')
            configs.append(StoryConfig(
                story_type=story_type,
                data_point_index=idx,
                hook_angle='default',
                visual_emphasis=visual,
                slot=slot,
            ))

        logger.info("[dataforge] Default selection: %d stories assigned", len(configs))
        return configs


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_story_selector() -> None:
    """
    Test StorySelector with mock DataPoint objects.
    If ANTHROPIC_API_KEY not set, tests the fallback path only.
    """
    logging.basicConfig(level=logging.INFO)
    from collections import namedtuple

    print("\n[dataforge] Testing StorySelector...")

    # Create mock DataPoint objects
    DataPoint = namedtuple('DataPoint', [
        'metric_name', 'current_value', 'prev_value',
        'pct_change', 'data_source', 'date', 'currency', 'extra_meta',
    ])

    mock_batch = [
        DataPoint('Apple Market Cap', 2_800_000_000_000, 3_000_000_000_000,
                  -6.67, 'Yahoo Finance', '2026-03-30', 'USD', {}),
        DataPoint('US Unemployment Rate', 4.1, 3.8,
                  7.89, 'BLS', '2026-03-30', '%', {}),
        DataPoint('Bitcoin Price', 68_500, 62_000,
                  10.48, 'CoinGecko', '2026-03-30', 'USD', {}),
        DataPoint('EUR/USD Exchange Rate', 1.0821, 1.0950,
                  -1.18, 'Alpha Vantage', '2026-03-30', 'USD', {}),
        DataPoint('Nigeria GDP Growth', 3.46, 2.98,
                  16.11, 'World Bank', '2026-03-30', 'NGN', {}),
    ]

    selector = StorySelector()
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if not api_key:
        print("  ANTHROPIC_API_KEY not set — testing fallback path only\n")

    stories = selector.select_daily_stories(mock_batch)

    print(f"  Stories selected: {len(stories)}\n")
    for s in stories:
        metric = mock_batch[s.data_point_index].metric_name if mock_batch else 'N/A'
        print(f"  [{s.slot:>8}]  {s.story_type:<10}  {metric:<25}  angle: {s.hook_angle}")

    # Validate result
    types = {s.story_type for s in stories}
    slots = {s.slot for s in stories}
    print(f"\n  Unique types: {len(types)}/4  Unique slots: {len(slots)}/4")
    assert len(stories) == 4, f"Expected 4 stories, got {len(stories)}"
    assert len(types) == 4, f"Expected 4 unique types, got {len(types)}"
    assert len(slots) == 4, f"Expected 4 unique slots, got {len(slots)}"
    print("  [OK] All assertions passed")

    print("\n[dataforge] StorySelector test complete.")


if __name__ == "__main__":
    test_story_selector()
