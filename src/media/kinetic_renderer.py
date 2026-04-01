"""
kinetic_renderer.py — KineticRenderer

Renders a Pillow frame-by-frame animation of a number counting up or down.
Output: 1080x1920 MP4 (video only — audio merged by production_pipeline.py)

Visual spec:
  Background:   #0D1117
  Primary text: #FFFFFF
  Accent teal:  #00D4AA
  Up green:     #26A65B
  Down red:     #E74C3C
  Font:         Roboto-Bold.ttf
  Frame size:   1080 x 1920 px (strict — asserted every frame)
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
BG_COLOR = (13, 17, 23)           # #0D1117
TEXT_COLOR = (255, 255, 255)       # #FFFFFF
SECONDARY_COLOR = (139, 148, 158)  # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA
UP_COLOR = (38, 166, 91)          # #26A65B
DOWN_COLOR = (231, 76, 60)        # #E74C3C

FONT_PATH = os.environ.get(
    'FONT_PATH',
    '/app/assets/fonts/Roboto-Bold.ttf',
)
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
            from PIL import Image, ImageDraw
        except ImportError:
            raise RuntimeError('Pillow not installed. Run: pip install Pillow')

        self.output_dir.mkdir(parents=True, exist_ok=True)

        total_frames = int(duration_sec * FPS)
        animate_frames = int(total_frames * 0.60)
        hold_frames = total_frames - animate_frames

        is_up = value >= prev_value
        change_color = UP_COLOR if is_up else DOWN_COLOR
        pct_change = ((value - prev_value) / prev_value * 100) if prev_value else 0.0
        arrow = '\u25b2' if is_up else '\u25bc'

        # Font sizes tuned for 1920px tall frame
        font_number = self._load_font(260)
        font_label = self._load_font(80)
        font_change = self._load_font(72)
        font_small = self._load_font(44)
        font_subtitle = self._load_font(40)

        logger.info(
            '[dataforge] KineticRenderer: rendering %d frames (%dx%d) for %s',
            total_frames, FRAME_W, FRAME_H, story_id,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            for i in range(total_frames):
                # --- Compute animated value ---
                if i < animate_frames:
                    # Frame 0 shows prev_value (no blank opening frame)
                    t = i / max(animate_frames - 1, 1)
                    eased = self._ease_out_cubic(t)
                    animated_value = prev_value + (value - prev_value) * eased
                    current_font_number = font_number
                else:
                    animated_value = value
                    # Subtle pulse on hold
                    hold_progress = (i - animate_frames) / max(hold_frames, 1)
                    pulse = 1.0 + 0.015 * math.sin(hold_progress * math.pi * 4)
                    current_font_number = self._load_font(int(260 * pulse))

                # --- Draw frame ---
                img = Image.new('RGB', (FRAME_W, FRAME_H), BG_COLOR)
                assert img.size == (FRAME_W, FRAME_H), f"Wrong frame size: {img.size}"
                draw = ImageDraw.Draw(img)

                # Accent top bar — y=0 to y=12
                draw.rectangle([(0, 0), (FRAME_W, 12)], fill=ACCENT_COLOR)

                # Label text — y=280
                draw.text(
                    (FRAME_W // 2, 280),
                    label.upper(),
                    font=font_label,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # Subtitle — y=360
                draw.text(
                    (FRAME_W // 2, 360),
                    "Today's Move",
                    font=font_subtitle,
                    fill=SECONDARY_COLOR,
                    anchor='mm',
                )

                # --- Central number with glow effect — y=880 ---
                number_str = self._format_value(animated_value, currency)
                number_y = 880

                # Glow: draw at 3 offsets in change_color at ~20% opacity
                glow_color = (*change_color, 50)
                # Use RGBA overlay for glow
                glow_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                glow_draw = ImageDraw.Draw(glow_layer)
                for dx, dy in [(3, 3), (-3, -3), (3, -3)]:
                    glow_draw.text(
                        (FRAME_W // 2 + dx, number_y + dy),
                        number_str,
                        font=current_font_number,
                        fill=glow_color,
                        anchor='mm',
                    )
                img = Image.composite(
                    Image.alpha_composite(img.convert('RGBA'), glow_layer),
                    img.convert('RGBA'),
                    glow_layer,
                ).convert('RGB')
                draw = ImageDraw.Draw(img)

                # Main number on top
                draw.text(
                    (FRAME_W // 2, number_y),
                    number_str,
                    font=current_font_number,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # --- % change badge with rounded rectangle chip — y=1060 ---
                pct_str = f'{arrow} {abs(pct_change):.2f}%'
                badge_y = 1060

                # Measure text for badge background
                bbox = draw.textbbox((0, 0), pct_str, font=font_change)
                text_w = bbox[2] - bbox[0]
                text_h = bbox[3] - bbox[1]
                pad_x, pad_y = 24, 12
                badge_x1 = FRAME_W // 2 - text_w // 2 - pad_x
                badge_y1 = badge_y - text_h // 2 - pad_y
                badge_x2 = FRAME_W // 2 + text_w // 2 + pad_x
                badge_y2 = badge_y + text_h // 2 + pad_y

                # Rounded rectangle chip background (20% opacity via overlay)
                chip_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                chip_draw = ImageDraw.Draw(chip_layer)
                chip_draw.rounded_rectangle(
                    [(badge_x1, badge_y1), (badge_x2, badge_y2)],
                    radius=16,
                    fill=(*change_color, 50),
                )
                img = Image.alpha_composite(img.convert('RGBA'), chip_layer).convert('RGB')
                draw = ImageDraw.Draw(img)

                # Badge text
                draw.text(
                    (FRAME_W // 2, badge_y),
                    pct_str,
                    font=font_change,
                    fill=change_color,
                    anchor='mm',
                )

                # Divider line — y=1160, 400px wide
                draw.line(
                    [(FRAME_W // 2 - 200, 1160), (FRAME_W // 2 + 200, 1160)],
                    fill=SECONDARY_COLOR,
                    width=2,
                )

                # Accent bottom bar — y=1908 to y=1920
                draw.rectangle(
                    [(0, 1908), (FRAME_W, FRAME_H)],
                    fill=ACCENT_COLOR,
                )

                # Watermark bottom-left — y=1820
                draw.text(
                    (40, 1820),
                    '@ChartDrop',
                    font=font_small,
                    fill=(*SECONDARY_COLOR, 102),
                )

                # Source credit bottom-right — y=1820
                draw.text(
                    (FRAME_W - 40, 1820),
                    source_credit,
                    font=font_small,
                    fill=(*SECONDARY_COLOR, 102),
                    anchor='ra',
                )

                # Save frame
                frame_path = tmp_path / f'frame_{i:05d}.png'
                img.save(frame_path, 'PNG')

            # --- Assemble frames into MP4 via ffmpeg ---
            output_path = self.output_dir / f'{story_id}_video.mp4'
            self._frames_to_mp4(tmp_path, output_path, FPS)

        logger.info('[dataforge] KineticRenderer: output -> %s', output_path)
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
        Forces output to exactly 1080x1920.
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
            '-vf', 'scale=1080:1920:force_original_aspect_ratio=disable',
            '-s', '1080x1920',
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
# ffmpeg path resolver (Windows + Railway compatible)
# ---------------------------------------------------------------------------

def _resolve_ffmpeg() -> str:
    """
    Resolve the ffmpeg binary path.
    Order:
      1. FFMPEG_BINARY env var (explicit override)
      2. imageio_ffmpeg (bundled binary)
      3. System 'ffmpeg' (Railway / Linux)
    """
    explicit = os.environ.get('FFMPEG_BINARY', '')
    if explicit and Path(explicit).exists():
        return explicit
    try:
        import imageio_ffmpeg
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path and Path(path).exists():
            return path
    except Exception:
        pass
    return 'ffmpeg'


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
        (1375.34, '$', '$1.4K'),
        (0.000423, '$', '$0.0004'),
    ]
    print('\n  Format tests:')
    for val, sym, expected in tests:
        result = KineticRenderer._format_value(val, sym)
        if result == expected:
            status = '[OK]'
        else:
            status = f'[FAIL] (got {result})'
        print(f'    {val:>20,.0f}  ->  {result:>12}  {status}')

    # Render a short test video (5 seconds to keep it fast locally)
    print('\n  Rendering 5-second test video (1080x1920)...')
    try:
        path = renderer.render(
            value=2_850_000_000_000,
            prev_value=3_050_000_000_000,
            label='Apple Market Cap',
            currency='$',
            duration_sec=5.0,
            story_id='test_kinetic',
            source_credit='Source: Yahoo Finance',
        )
        size_mb = Path(path).stat().st_size / 1_048_576
        print(f'  [OK] Video rendered: {path} ({size_mb:.1f} MB)')
    except RuntimeError as e:
        print(f'  [FAIL] Render failed: {e}')
        print('    (ffmpeg may not be installed locally -- this will work on Railway)')
    except Exception as e:
        print(f'  [FAIL] Unexpected error: {e}')

    print('\n[dataforge] KineticRenderer test complete.')


if __name__ == '__main__':
    test_render()
