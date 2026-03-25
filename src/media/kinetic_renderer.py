"""
kinetic_renderer.py — KineticRenderer

Renders a Pillow frame-by-frame animation of a number counting up or down.
Output: 1080x1920 MP4 (video only — audio merged by production_pipeline.py)

Visual spec (from master build doc Section 9):
  Background:   #0D1117
  Primary text: #FFFFFF
  Accent teal:  #00D4AA
  Up green:     #26A65B
  Down red:     #E74C3C
  Font:         Roboto-Bold.ttf
  Frame size:   1080 x 1920 px
  Frame rate:   30 fps

Usage:
    renderer = KineticRenderer()
    mp4_path = renderer.render(
        value=180_000_000_000,
        prev_value=200_000_000_000,
        label="Apple Market Cap",
        currency='$',
        duration_sec=40.0,
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
FPS = 30
BG_COLOR = (13, 17, 23)          # #0D1117
TEXT_COLOR = (255, 255, 255)      # #FFFFFF
SECONDARY_COLOR = (139, 148, 158) # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA
UP_COLOR = (38, 166, 91)          # #26A65B
DOWN_COLOR = (231, 76, 60)        # #E74C3C

FONT_PATH = os.environ.get(
    'FONT_PATH',
    '/app/assets/fonts/Roboto-Bold.ttf'
)
# Local fallback for development
LOCAL_FONT_FALLBACK = str(
    Path(__file__).resolve().parent.parent.parent / 'assets' / 'fonts' / 'Roboto-Bold.ttf'
)

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))


# ---------------------------------------------------------------------------
# KineticRenderer
# ---------------------------------------------------------------------------

class KineticRenderer:
    """
    Renders a kinetic number counting animation as an MP4.

    The number animates from prev_value to current_value over the first
    60% of frames using an ease-out curve. The remaining 40% holds on
    the final value with a subtle pulse effect.
    """

    def __init__(self, output_dir: str | Path = OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.font_path = self._resolve_font()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(
        self,
        value: float,
        prev_value: float,
        label: str,
        currency: str = '$',
        duration_sec: float = 40.0,
        accent_color: str = '#00D4AA',
        story_id: str = 'kinetic_000',
        source_credit: str = 'Source: Yahoo Finance',
    ) -> str:
        """
        Render a kinetic number animation to MP4.

        Args:
            value:        Final (current) value to animate to.
            prev_value:   Starting value.
            label:        Label text shown below the number (e.g. 'Apple Market Cap').
            currency:     Currency symbol prefix (e.g. '$', '€', '₦', '').
            duration_sec: Total video duration in seconds.
            accent_color: Hex colour for the accent bar (default teal).
            story_id:     Used for output filename.
            source_credit: Small attribution text bottom-right.

        Returns:
            str: Path to the rendered MP4 file.
        """
        try:
            from PIL import Image, ImageDraw, ImageFont
        except ImportError:
            raise RuntimeError('Pillow not installed. Run: pip install Pillow')

        self.output_dir.mkdir(parents=True, exist_ok=True)

        total_frames = int(duration_sec * FPS)
        animate_frames = int(total_frames * 0.60)  # count-up phase
        hold_frames = total_frames - animate_frames  # hold phase

        is_up = value >= prev_value
        change_color = UP_COLOR if is_up else DOWN_COLOR
        pct_change = ((value - prev_value) / prev_value * 100) if prev_value else 0.0
        arrow = '▲' if is_up else '▼'

        font_large = self._load_font(200)
        font_medium = self._load_font(72)
        font_small = self._load_font(44)
        font_tiny = self._load_font(32)

        logger.info(
            '[dataforge] KineticRenderer: rendering %d frames for %s',
            total_frames, story_id,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            for i in range(total_frames):
                # --- Compute animated value ---
                if i < animate_frames:
                    t = i / animate_frames
                    eased = self._ease_out_cubic(t)
                    animated_value = prev_value + (value - prev_value) * eased
                else:
                    animated_value = value
                    # Subtle pulse on hold: scale font slightly
                    hold_progress = (i - animate_frames) / max(hold_frames, 1)
                    pulse = 1.0 + 0.015 * math.sin(hold_progress * math.pi * 4)
                    font_large = self._load_font(int(200 * pulse))

                # --- Draw frame ---
                img = Image.new('RGB', (FRAME_W, FRAME_H), BG_COLOR)
                draw = ImageDraw.Draw(img)

                # Accent top bar
                draw.rectangle([(0, 0), (FRAME_W, 8)], fill=ACCENT_COLOR)

                # Label text (upper area)
                label_y = 340
                draw.text(
                    (FRAME_W // 2, label_y),
                    label.upper(),
                    font=font_medium,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # Main number
                number_str = self._format_value(animated_value, currency)
                number_y = FRAME_H // 2 - 60
                draw.text(
                    (FRAME_W // 2, number_y),
                    number_str,
                    font=font_large,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # % change badge
                pct_str = f'{arrow} {abs(pct_change):.2f}%'
                badge_y = number_y + 160
                draw.text(
                    (FRAME_W // 2, badge_y),
                    pct_str,
                    font=font_medium,
                    fill=change_color,
                    anchor='mm',
                )

                # Divider line
                line_y = badge_y + 90
                draw.line(
                    [(FRAME_W // 2 - 200, line_y), (FRAME_W // 2 + 200, line_y)],
                    fill=SECONDARY_COLOR,
                    width=2,
                )

                # Accent bottom bar
                draw.rectangle(
                    [(0, FRAME_H - 8), (FRAME_W, FRAME_H)],
                    fill=ACCENT_COLOR,
                )

                # Watermark bottom-left
                draw.text(
                    (40, FRAME_H - 60),
                    '@ChartDrop',
                    font=font_tiny,
                    fill=(*SECONDARY_COLOR, 100),
                )

                # Source credit bottom-right
                draw.text(
                    (FRAME_W - 40, FRAME_H - 60),
                    source_credit,
                    font=font_tiny,
                    fill=(*SECONDARY_COLOR, 100),
                    anchor='ra',
                )

                # Save frame
                frame_path = tmp_path / f'frame_{i:05d}.png'
                img.save(frame_path, 'PNG')

            # --- Assemble frames into MP4 via ffmpeg ---
            output_path = self.output_dir / f'{story_id}_video.mp4'
            self._frames_to_mp4(tmp_path, output_path, FPS)

        logger.info('[dataforge] KineticRenderer: output → %s', output_path)
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
            '[dataforge] Roboto-Bold.ttf not found at %s or %s — PIL will use default font',
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
        """
        Format a number for display.
        Auto-abbreviates large numbers: T, B, M, K.
        """
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
        Resolves binary via: FFMPEG_BINARY env var → imageio_ffmpeg → system ffmpeg.
        Raises RuntimeError if ffmpeg is not available.
        """
        # Resolve ffmpeg binary
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
    Smoke test — renders a short 5-second kinetic video locally.
    Run: python src/media/kinetic_renderer.py
    Output: data/raw/test_kinetic_video.mp4
    """
    logging.basicConfig(level=logging.INFO)
    renderer = KineticRenderer()

    print('\n[dataforge] Testing KineticRenderer...')
    print(f'  Font path resolved: {renderer.font_path or "PIL default (no TTF found)"}')

    # Test _format_value
    tests = [
        (2_800_000_000_000, '$', '$2.80T'),
        (180_000_000_000, '$', '$180.00B'),
        (4_500_000, '$', '$4.50M'),
        (1375.34, '₦', '₦1.4K'),
        (0.000423, '$', '$0.0004'),
    ]
    print('\n  Format tests:')
    for val, sym, expected in tests:
        result = KineticRenderer._format_value(val, sym)
        status = '✓' if result == expected else f'✗ (got {result})'
        print(f'    {val:>20,.0f} → {result:>12}  {status}')

    # Render a short test video (5 seconds to keep it fast locally)
    print('\n  Rendering 5-second test video...')
    try:
        path = renderer.render(
            value=180_000_000_000,
            prev_value=200_000_000_000,
            label='Apple Market Cap',
            currency='$',
            duration_sec=5.0,
            story_id='test_kinetic',
            source_credit='Source: Yahoo Finance',
        )
        size_mb = Path(path).stat().st_size / 1_048_576
        print(f'  ✓ Video rendered: {path} ({size_mb:.1f} MB)')
    except RuntimeError as e:
        print(f'  ✗ Render failed: {e}')
        print('    (ffmpeg may not be installed locally — this will work on Railway)')

    print('\n[dataforge] KineticRenderer test complete.')


if __name__ == '__main__':
    test_render()

# ---------------------------------------------------------------------------
# ffmpeg path resolver (Windows + Railway compatible)
# ---------------------------------------------------------------------------

def _resolve_ffmpeg() -> str:
    """
    Resolve the ffmpeg binary path.
    Order:
      1. FFMPEG_BINARY env var (explicit override)
      2. imageio_ffmpeg (bundled binary — works on Windows dev machines)
      3. System 'ffmpeg' (Railway / Linux)
    """
    # 1. Explicit override
    explicit = os.environ.get('FFMPEG_BINARY', '')
    if explicit and Path(explicit).exists():
        return explicit

    # 2. imageio_ffmpeg bundled binary
    try:
        import imageio_ffmpeg
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path and Path(path).exists():
            return path
    except Exception:
        pass

    # 3. System ffmpeg
    return 'ffmpeg'
