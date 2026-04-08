"""
narrative_renderer.py — NarrativeRenderer

"Breaking News" card — text-forward, word-by-word animation.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)

Layout:
  Top:    BREAKING pill badge + category label
  Middle: Headline word-by-word reveal + context fade-in
  Bottom: ChartDrop Daily Brief + date + watermark

Frame size:   1080 x 1920 px
Frame rate:   15fps render, 30fps output

Usage:
    renderer = NarrativeRenderer()
    mp4_path = renderer.render(
        script={'hook': '...', 'context': '...', 'cta': '...'},
        metric={'name': '...', 'current': 3.64, 'prev': 3.50,
                'pct_change': 4.0, 'unit': '%'},
        duration_sec=30.0,
    )
"""

import logging
import math
import os
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from textwrap import wrap

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FRAME_W = 1080
FRAME_H = 1920
RENDER_FPS = 15
OUTPUT_FPS = 30
BG_COLOR = (13, 17, 23)           # #0D1117
PANEL_COLOR = (17, 24, 39)        # #111827
TEXT_COLOR = (255, 255, 255)       # #FFFFFF
SECONDARY_COLOR = (139, 148, 158)  # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA
RED_COLOR = (231, 76, 60)         # #E74C3C

FONT_CANDIDATES = [
    os.environ.get('FONT_PATH', ''),
    '/app/assets/fonts/Roboto-Bold.ttf',
    '/app/data/fonts/Roboto-Bold.ttf',
    'assets/fonts/Roboto-Bold.ttf',
    os.path.join(os.path.dirname(__file__), '..', '..',
                 'assets', 'fonts', 'Roboto-Bold.ttf'),
]

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))

# Word reveal timing
FRAMES_PER_WORD = 4      # ~0.27s per word at 15fps
HOLD_AFTER_HEADLINE = 10  # frames to hold after headline is fully revealed
CONTEXT_FADE_FRAMES = 20  # frames to fade in context paragraph

# Number pattern for teal highlighting
_NUMBER_RE = re.compile(r'[\d,.]+\s*(?:percent|%|trillion|billion|million|thousand)?', re.IGNORECASE)


# ---------------------------------------------------------------------------
# NarrativeRenderer
# ---------------------------------------------------------------------------

class NarrativeRenderer:
    """
    Renders a "Breaking News" card animation as an MP4.
    Headline reveals word-by-word. Context fades in after.
    Frames streamed to disk immediately — no RAM accumulation.
    """

    def __init__(self, output_dir: str | Path = OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.font_path = self._resolve_font()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(
        self,
        script: dict,
        metric: dict,
        duration_sec: float = 30.0,
        story_id: str = 'narrative_000',
        source_credit: str = 'Source: FRED',
    ) -> str:
        """
        Render a narrative "Breaking News" card animation to MP4.

        Args:
            script: dict with keys: hook, context, cta
            metric: dict with keys: name, current, prev, pct_change, unit
            duration_sec: Total video duration in seconds.
            story_id: Used for output filename.
            source_credit: Attribution text.

        Returns:
            str: Path to the rendered MP4 file.
        """
        from PIL import Image, ImageDraw

        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f'{story_id}_video.mp4'
        total_frames = int(duration_sec * RENDER_FPS)

        logger.info(
            '[dataforge] NarrativeRenderer: rendering %d frames (%dx%d) for %s',
            total_frames, FRAME_W, FRAME_H, story_id,
        )

        # Fonts
        font_headline = self._load_font(60)
        font_context = self._load_font(40)
        font_breaking = self._load_font(36)
        font_category = self._load_font(32)
        font_brief = self._load_font(36)
        font_date = self._load_font(30)
        font_watermark = self._load_font(32)
        font_source = self._load_font(26)

        # Pre-compute headline words and their positions
        hook = script.get('hook', '')
        context_text = script.get('context', '')
        headline_words = hook.split()
        category = metric.get('name', 'MARKETS').upper()
        date_str = datetime.now(timezone.utc).strftime('%b %d, %Y').upper()

        # Compute animation phases
        headline_end = len(headline_words) * FRAMES_PER_WORD
        hold_end = headline_end + HOLD_AFTER_HEADLINE
        context_end = hold_end + CONTEXT_FADE_FRAMES

        volume_tmp = Path(os.environ.get(
            'DATAFORGE_RAW_DIR', 'data/raw'
        )) / 'tmp_frames_narrative'

        try:
            volume_tmp.mkdir(parents=True, exist_ok=True)

            for i in range(total_frames):
                img = Image.new('RGBA', (FRAME_W, FRAME_H), (*BG_COLOR, 255))
                draw = ImageDraw.Draw(img)

                # ===== TOP SECTION (y: 0-200) =====

                # Teal top bar
                draw.rectangle([(0, 0), (FRAME_W, 6)], fill=ACCENT_COLOR)

                # BREAKING pill badge
                pill_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                pill_draw = ImageDraw.Draw(pill_layer)

                # Pulse the dot
                dot_visible = (i // 8) % 2 == 0
                dot_char = '\u25cf ' if dot_visible else '  '
                breaking_text = f'{dot_char}BREAKING'

                # Measure pill size
                bbox = draw.textbbox((0, 0), breaking_text, font=font_breaking)
                pill_w = bbox[2] - bbox[0] + 40
                pill_h = bbox[3] - bbox[1] + 20

                pill_draw.rounded_rectangle(
                    [(50, 35), (50 + pill_w, 35 + pill_h)],
                    radius=20,
                    fill=(*RED_COLOR, 230),
                )
                img = Image.alpha_composite(img, pill_layer)
                draw = ImageDraw.Draw(img)

                draw.text(
                    (70, 45),
                    breaking_text,
                    font=font_breaking,
                    fill=TEXT_COLOR,
                )

                # Category label (right-aligned)
                draw.text(
                    (FRAME_W - 60, 50),
                    category,
                    font=font_category,
                    fill=ACCENT_COLOR,
                    anchor='ra',
                )

                # Thin separator
                draw.line([(60, 120), (FRAME_W - 60, 120)],
                          fill=(*SECONDARY_COLOR, 80), width=1)

                # ===== MIDDLE SECTION — HEADLINE (y: 200-1400) =====

                # How many words to show on this frame
                words_visible = min(
                    (i // FRAMES_PER_WORD) + 1 if i < headline_end else len(headline_words),
                    len(headline_words),
                )

                # Build visible text and wrap it
                visible_text = ' '.join(headline_words[:words_visible])

                # Draw headline word by word with teal numbers
                self._draw_wrapped_text(
                    draw, visible_text, font_headline,
                    x=60, y=200, max_width=FRAME_W - 120,
                    default_color=TEXT_COLOR,
                    highlight_numbers=True,
                )

                # ===== CONTEXT FADE-IN =====
                if i >= hold_end and context_text:
                    if i < context_end:
                        alpha = int(255 * (i - hold_end) / CONTEXT_FADE_FRAMES)
                    else:
                        alpha = 255

                    ctx_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                    ctx_draw = ImageDraw.Draw(ctx_layer)

                    # Estimate headline height for context placement
                    headline_lines = self._wrap_text(hook, font_headline, FRAME_W - 120)
                    line_h = font_headline.getbbox('Ay')[3] + 10
                    ctx_y = 200 + len(headline_lines) * line_h + 40

                    self._draw_wrapped_text(
                        ctx_draw, context_text, font_context,
                        x=60, y=ctx_y, max_width=FRAME_W - 120,
                        default_color=(*SECONDARY_COLOR, alpha),
                        highlight_numbers=False,
                    )
                    img = Image.alpha_composite(img, ctx_layer)
                    draw = ImageDraw.Draw(img)

                # ===== BOTTOM SECTION (y: 1600-1920) =====

                # Teal divider
                draw.line([(60, 1600), (FRAME_W - 60, 1600)],
                          fill=ACCENT_COLOR, width=2)

                # "ChartDrop Daily Brief"
                draw.text(
                    (60, 1640),
                    'ChartDrop Daily Brief',
                    font=font_brief,
                    fill=TEXT_COLOR,
                )

                # Date (right-aligned)
                draw.text(
                    (FRAME_W - 60, 1645),
                    date_str,
                    font=font_date,
                    fill=SECONDARY_COLOR,
                    anchor='ra',
                )

                # Bottom band
                draw.line([(0, 1912), (FRAME_W, 1912)],
                          fill=ACCENT_COLOR, width=2)
                draw.rectangle([(0, 1914), (FRAME_W, FRAME_H)],
                               fill=(*PANEL_COLOR, 255))

                # Watermark + source
                wm_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                wm_draw = ImageDraw.Draw(wm_layer)
                wm_draw.text(
                    (40, 1870),
                    '@ChartDrop',
                    font=font_watermark,
                    fill=(255, 255, 255, 100),
                    anchor='lm',
                )
                wm_draw.text(
                    (FRAME_W - 40, 1870),
                    source_credit,
                    font=font_source,
                    fill=(*SECONDARY_COLOR, 100),
                    anchor='rm',
                )
                img = Image.alpha_composite(img, wm_layer)

                # Save frame to disk
                frame_path = volume_tmp / f'frame_{i:05d}.jpg'
                img.convert('RGB').save(frame_path, 'JPEG', quality=85)
                del img

            # Assemble MP4
            self._frames_to_mp4(volume_tmp, output_path, RENDER_FPS, OUTPUT_FPS)

        finally:
            if volume_tmp.exists():
                shutil.rmtree(volume_tmp, ignore_errors=True)
                logger.info('[dataforge] Temp frames cleaned up')

        logger.info('[dataforge] NarrativeRenderer: output -> %s', output_path)
        return str(output_path)

    # ------------------------------------------------------------------
    # Text drawing helpers
    # ------------------------------------------------------------------

    def _wrap_text(self, text: str, font, max_width: int) -> list[str]:
        """Wrap text to fit within max_width pixels. Returns list of lines."""
        from PIL import ImageDraw, Image
        tmp = ImageDraw.Draw(Image.new('RGB', (1, 1)))
        words = text.split()
        lines = []
        current_line = []
        for word in words:
            test = ' '.join(current_line + [word])
            bbox = tmp.textbbox((0, 0), test, font=font)
            if bbox[2] - bbox[0] > max_width and current_line:
                lines.append(' '.join(current_line))
                current_line = [word]
            else:
                current_line.append(word)
        if current_line:
            lines.append(' '.join(current_line))
        return lines

    def _draw_wrapped_text(
        self, draw, text: str, font,
        x: int, y: int, max_width: int,
        default_color: tuple,
        highlight_numbers: bool = False,
    ) -> int:
        """Draw wrapped text with optional teal number highlighting. Returns final y."""
        lines = self._wrap_text(text, font, max_width)
        line_h = font.getbbox('Ay')[3] + 12
        cur_y = y

        for line in lines:
            if highlight_numbers:
                # Draw word by word, highlighting numbers in teal
                cur_x = x
                for word in line.split():
                    is_number = bool(re.match(r'^[\d,.$%]+$', word.strip('.,!?')))
                    color = ACCENT_COLOR if is_number else default_color
                    draw.text((cur_x, cur_y), word + ' ', font=font, fill=color)
                    bbox = draw.textbbox((cur_x, cur_y), word + ' ', font=font)
                    cur_x = bbox[2]
            else:
                draw.text((x, cur_y), line, font=font, fill=default_color)
            cur_y += line_h

        return cur_y

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_font(self) -> str:
        """Resolve font path from candidate list. Raises if not found."""
        font_path = None
        for candidate in FONT_CANDIDATES:
            if candidate and os.path.exists(candidate):
                font_path = os.path.abspath(candidate)
                break
        if font_path:
            from PIL import ImageFont
            ImageFont.truetype(font_path, 48)
            logger.info('[dataforge] Font resolved: %s', font_path)
            return font_path
        else:
            raise RuntimeError(
                '[dataforge] Roboto-Bold.ttf not found. '
                'Upload font to /app/assets/fonts/ on Railway volume.'
            )

    def _load_font(self, size: int):
        """Load Roboto-Bold at given size."""
        from PIL import ImageFont
        return ImageFont.truetype(self.font_path, size)

    @staticmethod
    def _frames_to_mp4(
        frames_dir: Path, output_path: Path,
        input_fps: int, output_fps: int = 30,
    ) -> None:
        """Assemble pre-written JPEG frames into MP4."""
        ffmpeg_bin = os.environ.get('FFMPEG_BINARY', '')
        if not ffmpeg_bin or not Path(ffmpeg_bin).exists():
            try:
                import imageio_ffmpeg
                ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
            except Exception:
                ffmpeg_bin = 'ffmpeg'

        from PIL import Image as _Image
        check = _Image.open(frames_dir / 'frame_00000.jpg')
        check_size = check.size
        check.close()
        assert check_size == (FRAME_W, FRAME_H), \
            f'[dataforge] Saved frame wrong size: {check_size}'
        logger.info('[dataforge] Frame 0 on disk verified: %dx%d', *check_size)

        cmd = [
            ffmpeg_bin, '-y',
            '-framerate', str(input_fps),
            '-i', str(frames_dir / 'frame_%05d.jpg'),
            '-vf', f'scale={FRAME_W}:{FRAME_H}:force_original_aspect_ratio=disable',
            '-s', f'{FRAME_W}x{FRAME_H}',
            '-c:v', 'libx264',
            '-preset', 'fast',
            '-crf', '18',
            '-pix_fmt', 'yuv420p',
            '-r', str(output_fps),
            str(output_path),
        ]
        logger.info('[dataforge] ffmpeg cmd: %s', ' '.join(cmd))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error('[dataforge] ffmpeg stderr:\n%s', result.stderr[-1000:])
            raise RuntimeError(
                f'ffmpeg failed (code {result.returncode}): {result.stderr[-200:]}'
            )

        probe = subprocess.run(
            [ffmpeg_bin, '-i', str(output_path)],
            capture_output=True, text=True,
        )
        for line in probe.stderr.split('\n'):
            if 'Stream' in line and 'Video' in line:
                logger.info('[dataforge] Output probe: %s', line.strip())
                break

        logger.info('[dataforge] ffmpeg assembled %s', output_path)


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_render():
    """Smoke test — renders a 15-second narrative card video."""
    logging.basicConfig(level=logging.INFO)

    print('\n[dataforge] Testing NarrativeRenderer...')

    renderer = NarrativeRenderer()
    print(f'  Font path: {renderer.font_path}')

    script = {
        'hook': 'Fed Funds Rate holds at 3.64 percent unchanged today.',
        'context': 'Markets await Fed signals as inflation stays sticky above the 2 percent target.',
        'cta': 'Follow ChartDrop for daily data that moves markets.',
    }
    metric = {
        'name': 'Fed Funds Rate',
        'current': 3.64,
        'prev': 3.64,
        'pct_change': 0.0,
        'unit': '%',
    }

    print(f'\n  Hook: {script["hook"]}')
    print(f'  Context: {script["context"]}')

    print('\n  Rendering 15-second narrative card...')
    try:
        path = renderer.render(
            script=script,
            metric=metric,
            duration_sec=15.0,
            story_id='test_narrative',
            source_credit='Source: FRED',
        )
        size_mb = Path(path).stat().st_size / 1_048_576
        print(f'  [OK] {path} ({size_mb:.1f} MB)')
    except Exception as e:
        print(f'  [FAIL] {e}')
        import traceback
        traceback.print_exc()

    print('\n[dataforge] NarrativeRenderer test complete.')


if __name__ == '__main__':
    test_render()
