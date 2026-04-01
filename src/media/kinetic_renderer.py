"""
kinetic_renderer.py — KineticRenderer

Bloomberg Terminal aesthetic kinetic number animation.
Output: 1080x1920 MP4 (video only — audio merged by production_pipeline.py)

Visual identity:
  Bloomberg terminal meets mobile finance app.
  Multi-layer background (base + vignette + dot grid).
  Header/footer panel bands. Stats row. Animated underline.
  Glow effect on hero number. Pill-shaped P&L badge.

Frame size:   1080 x 1920 px (strict)
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
PANEL_COLOR = (17, 24, 39)        # #111827
TEXT_COLOR = (255, 255, 255)       # #FFFFFF
SECONDARY_COLOR = (139, 148, 158)  # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA
UP_COLOR = (38, 166, 91)          # #26A65B
DOWN_COLOR = (231, 76, 60)        # #E74C3C
RULE_COLOR = (45, 55, 72)         # #2D3748

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
    Renders a Bloomberg-terminal-style kinetic number animation as an MP4.

    Background is built once (base + vignette + dot grid) and copied per frame.
    60% animate / 40% hold with ease-out cubic.
    """

    def __init__(self, output_dir: str | Path = OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.font_path = self._resolve_font()
        self._bg_frame = self._build_background()

    # ------------------------------------------------------------------
    # Background builder (called once)
    # ------------------------------------------------------------------

    def _build_background(self):
        """Build the 3-layer background: base + vignette + dot grid."""
        from PIL import Image, ImageDraw

        # Layer 1 — base fill
        bg = Image.new('RGBA', (FRAME_W, FRAME_H), (*BG_COLOR, 255))

        # Layer 2 — radial vignette (concentric ellipses, dark edges)
        vignette = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
        vdraw = ImageDraw.Draw(vignette)
        cx, cy = FRAME_W // 2, FRAME_H // 2
        steps = 12
        max_rx, max_ry = FRAME_W // 2 + 100, FRAME_H // 2 + 100
        for s in range(steps):
            # Outer to inner
            frac = s / steps
            rx = int(max_rx - frac * 80 * steps)
            ry = int(max_ry - frac * 80 * steps)
            if rx <= 0 or ry <= 0:
                break
            alpha = int(200 * (1 - frac))
            vdraw.ellipse(
                [(cx - rx, cy - ry), (cx + rx, cy + ry)],
                fill=(0, 0, 0, alpha),
            )
        bg = Image.alpha_composite(bg, vignette)

        # Layer 3 — dot grid (1px dots, #FFFFFF at 6% opacity, every 48px)
        grid = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
        gdraw = ImageDraw.Draw(grid)
        dot_color = (255, 255, 255, 15)
        for gx in range(0, FRAME_W, 48):
            for gy in range(0, FRAME_H, 48):
                gdraw.point((gx, gy), fill=dot_color)
        bg = Image.alpha_composite(bg, grid)

        logger.info('[dataforge] Background built: %dx%d with vignette + dot grid', FRAME_W, FRAME_H)
        return bg

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

        Returns:
            str: Path to the rendered MP4 file.
        """
        from PIL import Image, ImageDraw

        self.output_dir.mkdir(parents=True, exist_ok=True)

        total_frames = int(duration_sec * FPS)
        animate_frames = int(total_frames * 0.60)
        hold_frames = total_frames - animate_frames
        underline_end = int(total_frames * 0.40)
        prev_fade_start = int(total_frames * 0.20)
        prev_fade_end = int(total_frames * 0.30)

        is_up = value >= prev_value
        change_color = UP_COLOR if is_up else DOWN_COLOR
        pct_change = ((value - prev_value) / prev_value * 100) if prev_value else 0.0
        abs_change = abs(value - prev_value)
        arrow = '\u25b2' if is_up else '\u25bc'
        sign = '+' if is_up else '-'

        # Fonts tuned for 1920px frame
        font_number = self._load_font(240)
        font_currency = self._load_font(120)
        font_label = self._load_font(52)
        font_live = self._load_font(32)
        font_pct_arrow = self._load_font(56)
        font_pct_text = self._load_font(64)
        font_stat_label = self._load_font(32)
        font_stat_value = self._load_font(52)
        font_prev_label = self._load_font(32)
        font_prev_value = self._load_font(48)
        font_watermark = self._load_font(36)
        font_source = self._load_font(30)

        logger.info(
            '[dataforge] KineticRenderer: rendering %d frames (%dx%d) for %s',
            total_frames, FRAME_W, FRAME_H, story_id,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)

            for i in range(total_frames):
                # --- Compute animated value ---
                if i < animate_frames:
                    t = i / max(animate_frames - 1, 1)
                    eased = self._ease_out_cubic(t)
                    animated_value = prev_value + (value - prev_value) * eased
                    current_font_number = font_number
                else:
                    animated_value = value
                    hold_progress = (i - animate_frames) / max(hold_frames, 1)
                    pulse = 1.0 + 0.012 * math.sin(hold_progress * math.pi * 4)
                    current_font_number = self._load_font(int(240 * pulse))

                # --- Start from background copy ---
                img = self._bg_frame.copy()
                assert img.size == (FRAME_W, FRAME_H), f"Wrong frame size: {img.size}"
                draw = ImageDraw.Draw(img)

                # ===== ZONE 1 — TOP HEADER BAND (y: 0-180) =====
                draw.rectangle([(0, 0), (FRAME_W, 180)], fill=(*PANEL_COLOR, 255))

                # Label — left-aligned
                draw.text(
                    (60, 90),
                    label.upper(),
                    font=font_label,
                    fill=TEXT_COLOR,
                    anchor='lm',
                )

                # LIVE dot + text (top-right, pulsing)
                live_visible = (i // 15) % 2 == 0
                dot_alpha = 255 if live_visible else 80
                dot_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                dot_draw = ImageDraw.Draw(dot_layer)
                dot_draw.ellipse(
                    [(968, 78), (988, 98)],
                    fill=(*change_color, dot_alpha),
                )
                img = Image.alpha_composite(img, dot_layer)
                draw = ImageDraw.Draw(img)
                draw.text(
                    (1000, 90),
                    'LIVE',
                    font=font_live,
                    fill=(*change_color, dot_alpha),
                    anchor='lm',
                )

                # ===== ZONE 2 — NUMBER STAGE (y: 180-1100) =====

                # Previous value ghost (fade out between 20-30% of frames)
                if i < prev_fade_end:
                    if i < prev_fade_start:
                        prev_alpha = 255
                    else:
                        fade_frac = (i - prev_fade_start) / max(prev_fade_end - prev_fade_start, 1)
                        prev_alpha = int(255 * (1 - fade_frac))

                    prev_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                    prev_draw = ImageDraw.Draw(prev_layer)
                    prev_draw.text(
                        (FRAME_W // 2, 260),
                        'PREVIOUS',
                        font=font_prev_label,
                        fill=(*SECONDARY_COLOR, prev_alpha),
                        anchor='mm',
                    )
                    prev_str = self._format_value(prev_value, currency)
                    prev_draw.text(
                        (FRAME_W // 2, 310),
                        prev_str,
                        font=font_prev_value,
                        fill=(*SECONDARY_COLOR, prev_alpha),
                        anchor='mm',
                    )
                    img = Image.alpha_composite(img, prev_layer)
                    draw = ImageDraw.Draw(img)

                # Main number — glow + draw
                number_str_no_curr = self._format_value(animated_value, '')
                number_str_full = self._format_value(animated_value, currency)
                number_y = 680

                # Glow effect (4 offsets in change_color)
                glow_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                glow_draw = ImageDraw.Draw(glow_layer)
                for dx, dy, ga in [(6, 6, 25), (-6, -6, 25), (6, -6, 20), (-6, 6, 20)]:
                    glow_draw.text(
                        (FRAME_W // 2 + dx, number_y + dy),
                        number_str_no_curr,
                        font=current_font_number,
                        fill=(*change_color, ga),
                        anchor='mm',
                    )
                img = Image.alpha_composite(img, glow_layer)
                draw = ImageDraw.Draw(img)

                # Currency symbol — smaller, secondary, to the left of the number
                num_bbox = draw.textbbox((FRAME_W // 2, number_y), number_str_no_curr,
                                        font=current_font_number, anchor='mm')
                curr_x = num_bbox[0] - 10
                curr_y = num_bbox[1] + 20
                draw.text(
                    (curr_x, curr_y),
                    currency,
                    font=font_currency,
                    fill=SECONDARY_COLOR,
                    anchor='ra',
                )

                # Main number in white
                draw.text(
                    (FRAME_W // 2, number_y),
                    number_str_no_curr,
                    font=current_font_number,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # Animated underline (grows over first 40% of frames)
                if i < underline_end:
                    line_w = int((i / max(underline_end - 1, 1)) * 600)
                else:
                    line_w = 600
                if line_w > 0:
                    underline_y = num_bbox[3] + 20
                    draw.line(
                        [(FRAME_W // 2 - line_w // 2, underline_y),
                         (FRAME_W // 2 + line_w // 2, underline_y)],
                        fill=change_color,
                        width=4,
                    )

                # ===== ZONE 3 — CHANGE BADGE (y: 1100-1280) =====
                badge_cx = FRAME_W // 2
                badge_cy = 1190

                # Diffuse glow behind pill
                glow_pill = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                glow_pill_draw = ImageDraw.Draw(glow_pill)
                glow_pill_draw.rounded_rectangle(
                    [(badge_cx - 200, badge_cy - 60), (badge_cx + 200, badge_cy + 60)],
                    radius=60,
                    fill=(*change_color, 15),
                )
                img = Image.alpha_composite(img, glow_pill)
                draw = ImageDraw.Draw(img)

                # Pill background
                pill_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                pill_draw = ImageDraw.Draw(pill_layer)
                pill_x1 = badge_cx - 160
                pill_y1 = badge_cy - 48
                pill_x2 = badge_cx + 160
                pill_y2 = badge_cy + 48
                # Fill
                pill_draw.rounded_rectangle(
                    [(pill_x1, pill_y1), (pill_x2, pill_y2)],
                    radius=48,
                    fill=(*change_color, 40),
                )
                # Border
                pill_draw.rounded_rectangle(
                    [(pill_x1, pill_y1), (pill_x2, pill_y2)],
                    radius=48,
                    outline=(*change_color, 200),
                    width=2,
                )
                img = Image.alpha_composite(img, pill_layer)
                draw = ImageDraw.Draw(img)

                # Arrow inside pill (left side)
                draw.text(
                    (pill_x1 + 40, badge_cy),
                    arrow,
                    font=font_pct_arrow,
                    fill=change_color,
                    anchor='mm',
                )

                # Pct text inside pill (right of arrow)
                pct_text = f'{sign}{abs(pct_change):.2f}%'
                draw.text(
                    (badge_cx + 20, badge_cy),
                    pct_text,
                    font=font_pct_text,
                    fill=TEXT_COLOR,
                    anchor='mm',
                )

                # ===== ZONE 4 — STATS ROW (y: 1300-1480) =====

                # Horizontal rule above
                draw.line([(60, 1298), (1020, 1298)], fill=RULE_COLOR, width=1)

                # Left cell: PREV CLOSE
                draw.text(
                    (60, 1340),
                    'PREV CLOSE',
                    font=font_stat_label,
                    fill=SECONDARY_COLOR,
                    anchor='la',
                )
                draw.text(
                    (60, 1400),
                    self._format_value(prev_value, currency),
                    font=font_stat_value,
                    fill=TEXT_COLOR,
                    anchor='la',
                )

                # Vertical divider
                draw.line([(540, 1310), (540, 1470)], fill=RULE_COLOR, width=2)

                # Right cell: CHANGE
                draw.text(
                    (600, 1340),
                    'CHANGE',
                    font=font_stat_label,
                    fill=SECONDARY_COLOR,
                    anchor='la',
                )
                change_str = f'{sign}{self._format_value(abs_change, currency)}'
                draw.text(
                    (600, 1400),
                    change_str,
                    font=font_stat_value,
                    fill=change_color,
                    anchor='la',
                )

                # Horizontal rule below
                draw.line([(60, 1482), (1020, 1482)], fill=RULE_COLOR, width=1)

                # ===== ZONE 5 — BOTTOM INFO BAND (y: 1820-1920) =====

                # Teal top border
                draw.line([(0, 1820), (FRAME_W, 1820)], fill=ACCENT_COLOR, width=2)

                # Panel background
                draw.rectangle([(0, 1822), (FRAME_W, FRAME_H)], fill=(*PANEL_COLOR, 255))

                # Watermark
                wm_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                wm_draw = ImageDraw.Draw(wm_layer)
                wm_draw.text(
                    (60, 1870),
                    '@ChartDrop',
                    font=font_watermark,
                    fill=(255, 255, 255, 100),
                    anchor='lm',
                )
                # Source credit
                wm_draw.text(
                    (1020, 1870),
                    source_credit,
                    font=font_source,
                    fill=(*SECONDARY_COLOR, 100),
                    anchor='rm',
                )
                img = Image.alpha_composite(img, wm_layer)

                # Save frame as JPEG for speed
                frame_path = tmp_path / f'frame_{i:05d}.jpg'
                img.convert('RGB').save(frame_path, 'JPEG', quality=92)

            # --- Assemble frames into MP4 ---
            output_path = self.output_dir / f'{story_id}_video.mp4'
            self._frames_to_mp4(tmp_path, output_path, FPS)

        logger.info('[dataforge] KineticRenderer: output -> %s', output_path)
        return str(output_path)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_font(self) -> str:
        """Return font path -- Railway volume path or local fallback."""
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
        """Assemble JPEG frames into MP4. Forces 1080x1920."""
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
            '-i', str(frames_dir / 'frame_%05d.jpg'),
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
    """Resolve ffmpeg binary path."""
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
    Smoke test -- renders two 5-second kinetic videos locally.
    Test 1: DOWN move (red)   Test 2: UP move (green)
    """
    logging.basicConfig(level=logging.INFO)
    renderer = KineticRenderer()

    print('\n[dataforge] Testing KineticRenderer (Bloomberg terminal style)...')
    print(f'  Font path resolved: {renderer.font_path or "PIL default (no TTF found)"}')

    # Format tests
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
        status = '[OK]' if result == expected else f'[FAIL] (got {result})'
        print(f'    {val:>20,.0f}  ->  {result:>12}  {status}')

    # Test 1 -- DOWN move (red)
    print('\n  Test 1: Rendering DOWN move (Apple, red)...')
    try:
        path1 = renderer.render(
            value=2_850_000_000_000,
            prev_value=3_050_000_000_000,
            label='Apple Market Cap',
            currency='$',
            duration_sec=5.0,
            story_id='test_kinetic_down',
            source_credit='Source: Yahoo Finance',
        )
        size1 = Path(path1).stat().st_size / 1_048_576
        print(f'  [OK] {path1} ({size1:.1f} MB)')
    except Exception as e:
        print(f'  [FAIL] {e}')

    # Test 2 -- UP move (green)
    print('\n  Test 2: Rendering UP move (Bitcoin, green)...')
    try:
        path2 = renderer.render(
            value=94_500,
            prev_value=81_200,
            label='Bitcoin Price',
            currency='$',
            duration_sec=5.0,
            story_id='test_kinetic_up',
            source_credit='Source: CoinGecko',
        )
        size2 = Path(path2).stat().st_size / 1_048_576
        print(f'  [OK] {path2} ({size2:.1f} MB)')
    except Exception as e:
        print(f'  [FAIL] {e}')

    print('\n[dataforge] KineticRenderer test complete.')


if __name__ == '__main__':
    test_render()
