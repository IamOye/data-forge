"""
split_renderer.py — SplitRenderer

Renders a split-screen comparison animation showing two metrics animating
simultaneously side by side.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)

Visual spec (DataForge identity):
  Background:   #0D1117
  Left panel:   accent bar #E74C3C (red/loss side)
  Right panel:  accent bar #26A65B (green/gain side)
  Divider:      vertical white line at center, 2px, 40% opacity
  Text color:   #FFFFFF
  Secondary:    #8B949E
  Accent teal:  #00D4AA (title and gap label)
  Font:         Roboto-Bold.ttf
  Frame size:   1080 x 1920 px
  Frame rate:   30 fps

Usage:
    renderer = SplitRenderer()
    mp4_path = renderer.render(
        value_a=12_500, value_b=67_000,
        label_a='Savings Account', label_b='S&P 500 Index',
        start_value=10_000, currency='$', duration_sec=45.0,
    )
"""

import logging
import math
import os
import subprocess
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FRAME_W = 1080
FRAME_H = 1920
PANEL_W = FRAME_W // 2  # 540
FPS = 30
BG_COLOR = (13, 17, 23)           # #0D1117
TEXT_COLOR = (255, 255, 255)       # #FFFFFF
SECONDARY_COLOR = (139, 148, 158)  # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA
LEFT_ACCENT = (231, 76, 60)       # #E74C3C (red)
RIGHT_ACCENT = (38, 166, 91)      # #26A65B (green)

FONT_PATH = os.environ.get(
    'FONT_PATH',
    '/app/assets/fonts/Roboto-Bold.ttf',
)
LOCAL_FONT_FALLBACK = str(
    Path(__file__).resolve().parent.parent.parent / 'assets' / 'fonts' / 'Roboto-Bold.ttf'
)

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))


# ---------------------------------------------------------------------------
# SplitRenderer
# ---------------------------------------------------------------------------

class SplitRenderer:
    """
    Renders a split-screen comparison animation as an MP4.

    Two panels animate from a shared start_value to their respective
    final values, showing the divergence visually.
    """

    def __init__(self, output_dir: str | Path = OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.font_path = self._resolve_font()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(
        self,
        value_a: float,
        value_b: float,
        label_a: str,
        label_b: str,
        start_value: float,
        currency: str = '$',
        duration_sec: float = 45.0,
        story_id: str = 'split_000',
        source_credit: str = 'Source: FRED / Yahoo Finance',
        show_gap: bool = True,
    ) -> str:
        """
        Render a split-screen comparison animation to MP4.

        Args:
            value_a:       Final value for left panel.
            value_b:       Final value for right panel.
            label_a:       Left panel label (e.g. 'Savings Account').
            label_b:       Right panel label (e.g. 'S&P 500 Index').
            start_value:   Shared starting value for both panels.
            currency:      Currency symbol prefix.
            duration_sec:  Total video duration in seconds.
            story_id:      Used for output filename.
            source_credit: Small attribution text bottom-right.
            show_gap:      Show divergence gap label in hold phase.

        Returns:
            str: Path to the rendered MP4 file.
        """
        try:
            from PIL import Image, ImageDraw
        except ImportError:
            raise RuntimeError('Pillow not installed. Run: pip install Pillow')

        self.output_dir.mkdir(parents=True, exist_ok=True)

        total_frames = int(duration_sec * FPS)
        animate_frames = int(total_frames * 0.70)
        hold_frames = total_frames - animate_frames

        pct_a = ((value_a - start_value) / start_value * 100) if start_value else 0.0
        pct_b = ((value_b - start_value) / start_value * 100) if start_value else 0.0
        gap = abs(value_b - value_a)

        font_large = self._load_font(120)
        font_medium = self._load_font(56)
        font_small = self._load_font(36)
        font_tiny = self._load_font(28)
        font_gap = self._load_font(72)

        logger.info(
            '[dataforge] SplitRenderer: rendering %d frames for %s',
            total_frames, story_id,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            for i in range(total_frames):
                # --- Compute animated values ---
                if i < animate_frames:
                    t = i / animate_frames
                    eased = self._ease_out_cubic(t)
                    anim_a = start_value + (value_a - start_value) * eased
                    anim_b = start_value + (value_b - start_value) * eased
                    in_hold = False
                else:
                    anim_a = value_a
                    anim_b = value_b
                    in_hold = True

                # --- Draw frame ---
                img = Image.new('RGB', (FRAME_W, FRAME_H), BG_COLOR)
                draw = ImageDraw.Draw(img)

                # Left panel accent bar (left edge, vertical)
                draw.rectangle([(0, 0), (6, FRAME_H)], fill=LEFT_ACCENT)

                # Right panel accent bar (right edge, vertical)
                draw.rectangle([(FRAME_W - 6, 0), (FRAME_W, FRAME_H)], fill=RIGHT_ACCENT)

                # Center divider — white, 2px, 40% opacity
                # PIL RGB mode doesn't support alpha directly, so use secondary color
                divider_color = (255, 255, 255)
                for dx in range(2):
                    draw.line(
                        [(PANEL_W + dx, 100), (PANEL_W + dx, FRAME_H - 100)],
                        fill=(*divider_color, 102),  # ~40% opacity
                        width=1,
                    )

                # Accent top bar
                draw.rectangle([(0, 0), (FRAME_W, 6)], fill=ACCENT_COLOR)

                # Title — top center
                title_text = f'{label_a} vs {label_b}'
                draw.text(
                    (FRAME_W // 2, 120),
                    title_text,
                    font=font_medium,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # --- Left panel (x: 0-540) ---
                left_cx = PANEL_W // 2  # 270

                # Panel label
                draw.text(
                    (left_cx, 300),
                    label_a.upper(),
                    font=font_small,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # Animated number
                num_a_str = self._format_value(anim_a, currency)
                draw.text(
                    (left_cx, 800),
                    num_a_str,
                    font=font_large,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # % change
                arrow_a = '+' if pct_a >= 0 else ''
                pct_a_color = RIGHT_ACCENT if pct_a >= 0 else LEFT_ACCENT
                draw.text(
                    (left_cx, 1000),
                    f'{arrow_a}{pct_a:.1f}%',
                    font=font_small,
                    fill=pct_a_color,
                    anchor='mm',
                )

                # Start value note
                start_str = self._format_value(start_value, currency)
                draw.text(
                    (left_cx, 1100),
                    f'Started at {start_str}',
                    font=font_tiny,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # --- Right panel (x: 540-1080) ---
                right_cx = PANEL_W + PANEL_W // 2  # 810

                # Panel label
                draw.text(
                    (right_cx, 300),
                    label_b.upper(),
                    font=font_small,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # Animated number
                num_b_str = self._format_value(anim_b, currency)
                draw.text(
                    (right_cx, 800),
                    num_b_str,
                    font=font_large,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # % change
                arrow_b = '+' if pct_b >= 0 else ''
                pct_b_color = RIGHT_ACCENT if pct_b >= 0 else LEFT_ACCENT
                draw.text(
                    (right_cx, 1000),
                    f'{arrow_b}{pct_b:.1f}%',
                    font=font_small,
                    fill=pct_b_color,
                    anchor='mm',
                )

                # Start value note
                draw.text(
                    (right_cx, 1100),
                    f'Started at {start_str}',
                    font=font_tiny,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # --- Gap label (hold phase only) ---
                if show_gap and in_hold:
                    gap_str = f'Gap: {self._format_value(gap, currency)}'
                    draw.text(
                        (FRAME_W // 2, 1300),
                        gap_str,
                        font=font_gap,
                        fill=ACCENT_COLOR,
                        anchor='mm',
                    )

                # Accent bottom bar
                draw.rectangle(
                    [(0, FRAME_H - 6), (FRAME_W, FRAME_H)],
                    fill=ACCENT_COLOR,
                )

                # Watermark bottom-left
                draw.text(
                    (40, FRAME_H - 60),
                    '@ChartDrop',
                    font=font_tiny,
                    fill=(*SECONDARY_COLOR, 102),
                )

                # Source credit bottom-right
                draw.text(
                    (FRAME_W - 40, FRAME_H - 60),
                    source_credit,
                    font=font_tiny,
                    fill=(*SECONDARY_COLOR, 102),
                    anchor='ra',
                )

                # Save frame
                frame_path = tmp_path / f'frame_{i:05d}.png'
                img.save(frame_path, 'PNG')

            # --- Assemble frames into MP4 via ffmpeg ---
            output_path = self.output_dir / f'{story_id}_video.mp4'
            self._frames_to_mp4(tmp_path, output_path, FPS)

        logger.info('[dataforge] SplitRenderer: output -> %s', output_path)
        return str(output_path)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_font(self) -> str:
        """Return font path — Railway volume path or local fallback."""
        if Path(FONT_PATH).exists():
            return FONT_PATH
        if Path(LOCAL_FONT_FALLBACK).exists():
            logger.info('[dataforge] Using local font fallback: %s', LOCAL_FONT_FALLBACK)
            return LOCAL_FONT_FALLBACK
        logger.warning(
            '[dataforge] Roboto-Bold.ttf not found at %s or %s -- PIL will use default font',
            FONT_PATH, LOCAL_FONT_FALLBACK,
        )
        return ''

    def _load_font(self, size: int):
        """Load Roboto-Bold at given size, fall back to PIL default."""
        from PIL import ImageFont
        if self.font_path and Path(self.font_path).exists():
            try:
                return ImageFont.truetype(self.font_path, size)
            except Exception as e:
                logger.warning('[dataforge] Font load error: %s', e)
        return ImageFont.load_default()

    @staticmethod
    def _ease_out_cubic(t: float) -> float:
        """Ease-out cubic: fast start, decelerates to final value."""
        return 1 - (1 - t) ** 3

    @staticmethod
    def _format_value(value: float, currency: str = '$') -> str:
        """Format a number for display. Auto-abbreviates large numbers: T, B, M, K."""
        abs_val = abs(value)
        prefix = '-' if value < 0 else ''
        if abs_val >= 1_000_000_000_000:
            return f'{prefix}{currency}{abs_val / 1_000_000_000_000:.2f}T'
        if abs_val >= 1_000_000_000:
            return f'{prefix}{currency}{abs_val / 1_000_000_000:.2f}B'
        if abs_val >= 1_000_000:
            return f'{prefix}{currency}{abs_val / 1_000_000:.2f}M'
        if abs_val >= 1_000:
            return f'{prefix}{currency}{abs_val / 1_000:.1f}K'
        if abs_val < 1:
            return f'{prefix}{currency}{value:.4f}'
        return f'{prefix}{currency}{value:,.2f}'

    @staticmethod
    def _frames_to_mp4(frames_dir: Path, output_path: Path, fps: int) -> None:
        """
        Assemble PNG frames into MP4 using ffmpeg.
        Resolves binary via: FFMPEG_BINARY env var -> imageio_ffmpeg -> system ffmpeg.
        """
        ffmpeg_bin = os.environ.get('FFMPEG_BINARY', '')
        if not ffmpeg_bin or not Path(ffmpeg_bin).exists():
            try:
                import imageio_ffmpeg
                ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
            except Exception:
                ffmpeg_bin = 'ffmpeg'
        logger.info('[dataforge] Using ffmpeg binary: %s', ffmpeg_bin)
        cmd = [
            ffmpeg_bin, '-y',
            '-framerate', str(fps),
            '-i', str(frames_dir / 'frame_%05d.png'),
            '-c:v', 'libx264',
            '-pix_fmt', 'yuv420p',
            '-crf', '18',
            '-preset', 'fast',
            str(output_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f'ffmpeg failed:\n{result.stderr[-500:]}'
            )
        logger.info('[dataforge] ffmpeg assembled %s', output_path)


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_render():
    """
    Smoke test — renders a short 5-second split-screen video locally.
    Run: python src/media/split_renderer.py
    Output: data/raw/test_split_video.mp4
    """
    logging.basicConfig(level=logging.INFO)
    renderer = SplitRenderer()

    print('\n[dataforge] Testing SplitRenderer...')
    print(f'  Font path resolved: {renderer.font_path or "PIL default (no TTF found)"}')

    # Test _format_value (same tests as kinetic_renderer.py)
    tests = [
        (2_800_000_000_000, '$', '$2.80T'),
        (180_000_000_000, '$', '$180.00B'),
        (4_500_000, '$', '$4.50M'),
        (1375.34, '$', '$1.4K'),
        (0.000423, '$', '$0.0004'),
        (67_000, '$', '$67.0K'),
        (12_500, '$', '$12.5K'),
        (10_000, '$', '$10.0K'),
    ]
    print('\n  Format tests:')
    all_passed = True
    for val, sym, expected in tests:
        result = SplitRenderer._format_value(val, sym)
        if result == expected:
            status = '[OK]'
        else:
            status = f'[FAIL] (got {result})'
            all_passed = False
        print(f'    {val:>20,.0f}  ->  {result:>12}  {status}')

    # Render a short test video (5 seconds for fast local test)
    print('\n  Rendering 5-second test split-screen video...')
    try:
        path = renderer.render(
            value_a=12_500,
            value_b=67_000,
            label_a='Savings Account',
            label_b='S&P 500 Index',
            start_value=10_000,
            currency='$',
            duration_sec=5.0,
            story_id='test_split',
            source_credit='Source: FRED / Yahoo Finance',
            show_gap=True,
        )
        size_mb = Path(path).stat().st_size / 1_048_576
        print(f'  [OK] Video rendered: {path} ({size_mb:.1f} MB)')
    except RuntimeError as e:
        print(f'  [FAIL] Render failed: {e}')
        print('    (ffmpeg may not be installed locally -- this will work on Railway)')
    except Exception as e:
        print(f'  [FAIL] Unexpected error: {e}')

    print('\n[dataforge] SplitRenderer test complete.')


if __name__ == '__main__':
    test_render()
