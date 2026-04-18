"""
script_adapter.py — ScriptAdapter

Generates 40-second YouTube Shorts scripts for DataForge finance data videos.
Adapted from ChannelForge script_generator.py.

Key differences from ChannelForge:
  - Data-driven narrative (the number IS the story)
  - 4-part structure instead of 5 (no landing beat)
  - 50-55 word hard max (shorter = faster pacing for data videos)
  - Script must make sense WITHOUT seeing the chart
  - System prompt: financial data narrator, not opinion/debate host

Usage:
    adapter = ScriptAdapter()
    result = adapter.generate(
        metric_name="Apple Market Cap",
        current_value=2_800_000_000_000,
        prev_value=3_000_000_000_000,
        pct_change=-6.67,
        data_source="Yahoo Finance",
        news_context=["Apple drops on iPhone demand fears"],
    )
    print(result.full_script)
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import anthropic
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

_MODEL = "claude-sonnet-4-5"
MAX_WORDS = 60
RETRY_WORD_LIMIT = 58

_SYSTEM_PROMPT = (
    "You are a financial news storyteller writing scripts for 35-40 second "
    "YouTube Shorts videos. Your style is Palki Sharma Upadhyay — urgent, "
    "journalistic, punchy. Short sentences. Dramatic cause-and-effect. "
    "You name the reason like an accusation. You connect the macro to one "
    "person's real life, then zoom back out to the bigger picture.\n\n"
    "Structure (follow this exactly, 50-60 words total):\n\n"
    "HOOK (6-10 words): Drop the viewer into one person's moment. "
    "Ultra short. No fluff. Example: 'Marcus pulls into his usual station. "
    "$4.20 per gallon.'\n\n"
    "HUMAN IMPACT (10-15 words): Two or three short punchy sentences. "
    "Show exactly what this costs the person in dollars, decisions, or pain. "
    "Then pivot hard — 'Here is why.' or 'Here is what happened.' "
    "Name the cause like a verdict: one country, one decision, one number.\n\n"
    "DATA PROOF (10-14 words): The hard number. The exact change. "
    "The source. Short sentence. No softening.\n\n"
    "KICKER (8-10 words): One line that stings. Zoom out from the person "
    "to the universal truth. Make the viewer feel it. "
    "Example: 'When geopolitics sneezes, your gas tank bleeds.'\n\n"
    "CTA (6-8 words): Verbatim — "
    "'Subscribe - your wallet deserves better intel.'\n\n"
    "Rules:\n"
    "- Maximum sentence length: 10 words. Break longer thoughts into two.\n"
    "- Never use: 'In conclusion', 'This means that', 'It is important', "
    "'As we can see', 'Additionally'\n"
    "- Never soften the number — say it flat and hard\n"
    "- The cause must be named specifically: a country, a policy, a person, "
    "an event — not vague words like 'market forces' or 'global tensions'\n"
    "- The person in HOOK must have a name and a specific situation\n"
    "- Numbers must match the data provided exactly\n"
    "- MINIMUM 50 words total. If under 50, expand HUMAN IMPACT.\n"
    "- Active voice, present tense throughout\n\n"
    "Respond ONLY with a JSON object, no markdown, no preamble:\n"
    "{\"hook\": \"...\", \"context\": \"...\", \"insight\": \"...\", "
    "\"cta\": \"Subscribe - your wallet deserves better intel.\"}"
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ScriptResult:
    """Result of a ScriptAdapter.generate() call."""

    metric_name: str
    hook: str
    context: str
    insight: str
    cta: str
    full_script: str
    word_count: int
    is_valid: bool
    validation_errors: list[str] = field(default_factory=list)
    raw_response: str = ""
    generated_at: str = ""

    def __post_init__(self) -> None:
        if not self.generated_at:
            self.generated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return {
            "metric_name":       self.metric_name,
            "hook":              self.hook,
            "context":           self.context,
            "insight":           self.insight,
            "cta":               self.cta,
            "full_script":       self.full_script,
            "word_count":        self.word_count,
            "is_valid":          self.is_valid,
            "validation_errors": self.validation_errors,
            "generated_at":      self.generated_at,
        }


REQUIRED_PARTS = ["hook", "context", "insight", "cta"]
FIXED_CTA = "Subscribe - your wallet deserves better intel."


# ---------------------------------------------------------------------------
# ScriptAdapter
# ---------------------------------------------------------------------------

class ScriptAdapter:
    """
    Generates data narrator scripts via Claude API.

    Args:
        api_key:    Anthropic API key. Reads ANTHROPIC_API_KEY from env if None.
        model:      Claude model to use.
        max_tokens: Max tokens for API response.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str = _MODEL,
        max_tokens: int = 400,
    ) -> None:
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        self.model = model
        self.max_tokens = max_tokens

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        metric_name: str,
        current_value: float,
        prev_value: float,
        pct_change: float,
        data_source: str = "Yahoo Finance",
        news_context: list[str] | None = None,
        story_type: str = "kinetic",
    ) -> ScriptResult:
        """
        Generate a data narrator script for a DataForge video.

        Args:
            metric_name:   Human-readable metric (e.g. 'Apple Market Cap')
            current_value: Current value of the metric
            prev_value:    Previous value
            pct_change:    Percentage change (positive = up, negative = down)
            data_source:   Source attribution (e.g. 'FRED', 'Yahoo Finance')
            news_context:  Optional list of headlines explaining the move
            story_type:    Format type hint for prompt context

        Returns:
            ScriptResult with full_script and validation status.
        """
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not set")

        client = anthropic.Anthropic(api_key=self.api_key)
        prompt = self._build_prompt(
            metric_name, current_value, prev_value,
            pct_change, data_source, news_context or [], story_type,
        )

        try:
            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = message.content[0].text.strip()
            logger.info("[dataforge] ScriptAdapter raw response: %s", raw[:200])

        except Exception as e:
            logger.error("[dataforge] ScriptAdapter API call failed: %s", e)
            return self._error_result(metric_name, str(e))

        parts = self._parse_parts(raw)

        # Retry if too long
        if self._parts_too_long(parts):
            parts, raw = self._retry_for_brevity(
                metric_name, current_value, pct_change, parts, raw, client
            )

        # Force CTA if drifted
        if parts.get("cta", "").strip() != FIXED_CTA:
            logger.warning(
                "[dataforge] CTA drift detected. Got: %r — force replacing",
                parts.get("cta", ""),
            )
            parts["cta"] = FIXED_CTA

        result = self._build_result(metric_name, parts, raw)

        # Word count validation logging
        if result.word_count < 45:
            logger.warning(
                '[dataforge] Script too short: %d words. Story: %s',
                result.word_count, metric_name,
            )
        logger.info('[dataforge] Script word count: %d words', result.word_count)

        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_prompt(
        metric_name: str,
        current_value: float,
        prev_value: float,
        pct_change: float,
        data_source: str,
        news_context: list[str],
        story_type: str,
    ) -> str:
        direction = "UP" if pct_change >= 0 else "DOWN"
        sign = "+" if pct_change >= 0 else ""
        if news_context:
            headlines = "\n".join(f"  - {h}" for h in news_context[:3])
            news_block = (
                f"\nToday's headline driving this story:\n{headlines}\n"
                f"Use this headline to choose the person in your story and "
                f"make the human impact specific to what's happening TODAY."
            )
        else:
            news_block = "\nNo headline available — use the data movement to find the human story."

        return (
            f"TODAY'S STORY DATA:\n"
            f"Metric: {metric_name}\n"
            f"Current value: {current_value:,.2f}\n"
            f"Previous value: {prev_value:,.2f}\n"
            f"Change: {direction} {sign}{pct_change:.2f}%\n"
            f"Data source: {data_source}\n"
            f"Video format: {story_type}"
            f"{news_block}\n\n"
            f"Write the Palki Sharma-style 4-part script now. MUST be 50-60 words total.\n"
            f"HOOK = person dropped into the moment (ultra short). "
            f"CONTEXT = human cost + 'Here is why' + name the cause. "
            f"INSIGHT = flat hard number. KICKER = universal sting. "
            f"CTA verbatim: \"{FIXED_CTA}\""
        )

    @staticmethod
    def _parse_parts(raw: str) -> dict[str, str]:
        """Parse JSON response into parts dict. Falls back to empty strings."""
        try:
            # Strip markdown fences if present
            clean = re.sub(r"```(?:json)?|```", "", raw).strip()
            data = json.loads(clean)
            return {
                "hook":    str(data.get("hook", "")),
                "context": str(data.get("context", "")),
                "insight": str(data.get("insight", "")),
                "cta":     str(data.get("cta", "")),
            }
        except Exception as e:
            logger.error("[dataforge] Failed to parse script JSON: %s\nRaw: %s", e, raw)
            return {p: "" for p in REQUIRED_PARTS}

    def _parts_too_long(self, parts: dict[str, str]) -> bool:
        all_text = " ".join(parts.get(p, "") for p in REQUIRED_PARTS)
        word_count = len(all_text.split()) if all_text.strip() else 0
        has_content = all(parts.get(p, "").strip() for p in REQUIRED_PARTS)
        return has_content and word_count > RETRY_WORD_LIMIT

    def _retry_for_brevity(
        self,
        metric_name: str,
        current_value: float,
        pct_change: float,
        parts: dict[str, str],
        raw: str,
        client: "anthropic.Anthropic",
    ) -> tuple[dict[str, str], str]:
        word_count = len(
            " ".join(parts.get(p, "") for p in REQUIRED_PARTS).split()
        )
        logger.warning(
            "[dataforge] Script too long (%d words) — retrying with 55-word cap",
            word_count,
        )
        retry_prompt = (
            f"Metric: {metric_name}, value: {current_value:,.2f}, "
            f"change: {pct_change:+.2f}%\n"
            f"Your previous script was {word_count} words — over the 60-word limit. "
            f"Rewrite in EXACTLY 50 words or fewer. Cut every non-essential word."
        )
        try:
            message = client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": retry_prompt}],
            )
            raw2 = message.content[0].text.strip()
            parts2 = self._parse_parts(raw2)
            has_content = all(parts2.get(p, "").strip() for p in REQUIRED_PARTS)
            if has_content:
                new_count = len(
                    " ".join(parts2.get(p, "") for p in REQUIRED_PARTS).split()
                )
                logger.info(
                    "[dataforge] Word count retry: %d → %d words",
                    word_count, new_count,
                )
                if new_count < MAX_WORDS:
                    return parts2, raw2
        except Exception as e:
            logger.error("[dataforge] Brevity retry failed: %s", e)
        return parts, raw

    def _build_result(
        self, metric_name: str, parts: dict[str, str], raw: str
    ) -> ScriptResult:
        hook    = parts.get("hook", "")
        context = parts.get("context", "")
        insight = parts.get("insight", "")
        cta     = parts.get("cta", "")

        full_script = " ".join(filter(None, [hook, context, insight, cta]))
        word_count  = len(full_script.split()) if full_script.strip() else 0
        errors      = self._validate(hook, context, insight, cta, word_count)

        return ScriptResult(
            metric_name=metric_name,
            hook=hook,
            context=context,
            insight=insight,
            cta=cta,
            full_script=full_script,
            word_count=word_count,
            is_valid=len(errors) == 0,
            validation_errors=errors,
            raw_response=raw,
        )

    @staticmethod
    def _validate(
        hook: str,
        context: str,
        insight: str,
        cta: str,
        word_count: int,
    ) -> list[str]:
        errors: list[str] = []
        for name, text in [
            ("hook", hook), ("context", context),
            ("insight", insight), ("cta", cta),
        ]:
            if not text.strip():
                errors.append(f"{name} is empty")
        if word_count >= MAX_WORDS:
            errors.append(f"word_count={word_count} exceeds limit of {MAX_WORDS - 1}")
        if cta.strip() and cta.strip() != FIXED_CTA:
            errors.append(f"CTA does not match fixed text. Got: {cta!r}")
        return errors

    @staticmethod
    def _error_result(metric_name: str, error: str) -> ScriptResult:
        return ScriptResult(
            metric_name=metric_name,
            hook="", context="", insight="", cta="",
            full_script="",
            word_count=0,
            is_valid=False,
            validation_errors=[error],
        )


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_script_adapter():
    logging.basicConfig(level=logging.INFO)
    adapter = ScriptAdapter()

    print("\n[dataforge] Testing ScriptAdapter...\n")

    result = adapter.generate(
        metric_name="Apple Market Cap",
        current_value=2_800_000_000_000,
        prev_value=3_000_000_000_000,
        pct_change=-6.67,
        data_source="Yahoo Finance",
        news_context=["Apple drops on weak iPhone demand outlook"],
        story_type="kinetic",
    )

    print(f"  Valid:      {result.is_valid}")
    print(f"  Word count: {result.word_count}")
    print(f"  Errors:     {result.validation_errors}")
    print(f"\n  HOOK:    {result.hook}")
    print(f"  CONTEXT: {result.context}")
    print(f"  INSIGHT: {result.insight}")
    print(f"  CTA:     {result.cta}")
    print(f"\n  FULL SCRIPT:\n  {result.full_script}")
    print("\n[dataforge] ScriptAdapter test complete.")


if __name__ == "__main__":
    test_script_adapter()
