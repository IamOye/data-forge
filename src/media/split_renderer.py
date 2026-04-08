"""
split_renderer.py — SplitRenderer

Renders a split-screen comparison of TWO metrics animating simultaneously.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)

Layout:
  Top half  (y: 0-960):     Metric A panel
  Bottom half (y: 960-1920): Metric B panel
  Divider: 4px teal line at y=960

Each panel: dark background, metric name, hero number, P&L badge, prev close.
Both panels animate simultaneously with ease-out cubic.

Frame size:   1080 x 1920 px
Frame rate:   15fps render, 30fps output

Usage:
    renderer = SplitRenderer()
    mp4_path = renderer.render(
        metric_a={'name': 'Fed Funds Rate', 'current': 5.33, 'prev': 5.50, 'unit': '%'},
        metric_b={'name': 'US Inflation', 'current': 3.2, 'prev': 3.4, 'unit': '%'},
        duration_sec=30.0,
    )
"""

import logging
import math
import os
import shutil
import subprocess
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FRAME_W = 1080
FRAME_H = 1920
PANEL_H = 960  # each panel is half the frame
RENDER_FPS = 15
OUTPUT_FPS = 30
BG_COLOR = (13, 17, 23)           # #0D1117
PANEL_COLOR = (17, 24, 39)        # #111827
TEXT_COLOR = (255, 255, 255)       # #FFFFFF
SECONDARY_COLOR = (139, 148, 158)  # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA
UP_COLOR = (38, 166, 91)          # #26A65B
DOWN_COLOR = (231, 76, 60)        # #E74C3C

FONT_CANDIDATES = [
    os.environ.get('FONT_PATH', ''),
    '/app/assets/fonts/Roboto-Bold.ttf',
    '/app/data/fonts/Roboto-Bold.ttf',
    'assets/fonts/Roboto-Bold.ttf',
    os.path.join(os.path.dirname(__file__), '..', '..',
                 'assets', 'fonts', 'Roboto-Bold.ttf'),
]

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))


# ---------------------------------------------------------------------------
# SplitRenderer
# ---------------------------------------------------------------------------

class SplitRenderer:
    """
    Renders a split-screen comparison of two metrics as an MP4.
    Both panels animate simultaneously with kinetic number counting.
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
        metric_a: dict,
        metric_b: dict,
        duration_sec: float = 30.0,
        story_id: str = 'split_000',
        source_credit: str = 'Source: FRED',
    ) -> str:
        """
        Render a split-screen comparison animation to MP4.

        Args:
            metric_a: dict with keys: name, current, prev, unit
            metric_b: dict with keys: name, current, prev, unit
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
            '[dataforge] SplitRenderer: rendering %d frames (%dx%d) for %s',
            total_frames, FRAME_W, FRAME_H, story_id,
        )

        animate_frames = int(total_frames * 0.60)
        hold_frames = total_frames - animate_frames

        # Fonts
        font_title = self._load_font(44)
        font_number = self._load_font(120)
        font_label = self._load_font(40)
        font_pct = self._load_font(48)
        font_prev_label = self._load_font(28)
        font_prev_val = self._load_font(36)
        font_watermark = self._load_font(32)
        font_source = self._load_font(26)
        font_unit = self._load_font(60)

        # Pre-compute metric data
        metrics = []
        for m in [metric_a, metric_b]:
            current = float(m['current'])
            prev = float(m['prev'])
            pct = ((current - prev) / abs(prev) * 100) if prev else 0.0
            is_up = current >= prev
            metrics.append({
                'name': m['name'],
                'current': current,
                'prev': prev,
                'pct': pct,
                'unit': m.get('unit', '$'),
                'is_up': is_up,
                'color': UP_COLOR if is_up else DOWN_COLOR,
                'arrow': '\u25b2' if is_up else '\u25bc',
                'sign': '+' if is_up else '-',
            })

        volume_tmp = Path(os.environ.get(
            'DATAFORGE_RAW_DIR', 'data/raw'
        )) / 'tmp_frames_split'

        try:
            volume_tmp.mkdir(parents=True, exist_ok=True)

            for i in range(total_frames):
                # Compute eased progress
                if i < animate_frames:
                    t = i / max(animate_frames - 1, 1)
                    eased = self._ease_out_cubic(t)
                    current_font_number = font_number
                else:
                    eased = 1.0
                    hold_progress = (i - animate_frames) / max(hold_frames, 1)
                    pulse = 1.0 + 0.010 * math.sin(hold_progress * math.pi * 4)
                    current_font_number = self._load_font(int(120 * pulse))

                img = Image.new('RGBA', (FRAME_W, FRAME_H), (*BG_COLOR, 255))
                draw = ImageDraw.Draw(img)

                # === TITLE BAR (top centre) ===
                draw.rectangle([(0, 0), (FRAME_W, 70)], fill=(*PANEL_COLOR, 255))
                draw.text(
                    (FRAME_W // 2, 35),
                    'CHART DROP',
                    font=font_title,
                    fill=ACCENT_COLOR,
                    anchor='mm',
                )

                # === Draw both panels ===
                for panel_idx, m in enumerate(metrics):
                    y_offset = 0 if panel_idx == 0 else PANEL_H

                    # Panel background (subtle header band)
                    header_y = y_offset + 80
                    draw.rectangle(
                        [(0, header_y), (FRAME_W, header_y + 60)],
                        fill=(*PANEL_COLOR, 255),
                    )

                    # Metric name
                    draw.text(
                        (FRAME_W // 2, header_y + 30),
                        m['name'].upper(),
                        font=font_label,
                        fill=SECONDARY_COLOR,
                        anchor='mm',
                    )

                    # Animated value
                    animated = m['prev'] + (m['current'] - m['prev']) * eased
                    display = self._format_number(animated)
                    number_y = y_offset + 380

                    # Glow effect
                    glow_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                    glow_draw = ImageDraw.Draw(glow_layer)
                    for dx, dy, ga in [(4, 4, 20), (-4, -4, 20), (4, -4, 15), (-4, 4, 15)]:
                        glow_draw.text(
                            (FRAME_W // 2 + dx, number_y + dy),
                            display,
                            font=current_font_number,
                            fill=(*m['color'], ga),
                            anchor='mm',
                        )
                    img = Image.alpha_composite(img, glow_layer)
                    draw = ImageDraw.Draw(img)

                    # Unit symbol (suffix or prefix)
                    num_bbox = draw.textbbox(
                        (FRAME_W // 2, number_y), display,
                        font=current_font_number, anchor='mm',
                    )
                    if m['unit'] in ('%', 'pts'):
                        draw.text(
                            (num_bbox[2] + 8, num_bbox[1] + 10),
                            m['unit'],
                            font=font_unit,
                            fill=SECONDARY_COLOR,
                            anchor='la',
                        )
                    else:
                        draw.text(
                            (num_bbox[0] - 8, num_bbox[1] + 10),
                            m['unit'],
                            font=font_unit,
                            fill=SECONDARY_COLOR,
                            anchor='ra',
                        )

                    # Hero number
                    draw.text(
                        (FRAME_W // 2, number_y),
                        display,
                        font=current_font_number,
                        fill=(255, 255, 255),
                        anchor='mm',
                    )

                    # P&L badge
                    badge_y = y_offset + 530
                    pct_text = f'{m["arrow"]} {m["sign"]}{abs(m["pct"]):.2f}%'

                    # Pill background
                    pill_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                    pill_draw = ImageDraw.Draw(pill_layer)
                    pill_draw.rounded_rectangle(
                        [(FRAME_W // 2 - 140, badge_y - 32),
                         (FRAME_W // 2 + 140, badge_y + 32)],
                        radius=32,
                        fill=(*m['color'], 40),
                    )
                    pill_draw.rounded_rectangle(
                        [(FRAME_W // 2 - 140, badge_y - 32),
                         (FRAME_W // 2 + 140, badge_y + 32)],
                        radius=32,
                        outline=(*m['color'], 180),
                        width=2,
                    )
                    img = Image.alpha_composite(img, pill_layer)
                    draw = ImageDraw.Draw(img)

                    draw.text(
                        (FRAME_W // 2, badge_y),
                        pct_text,
                        font=font_pct,
                        fill=m['color'],
                        anchor='mm',
                    )

                    # PREV CLOSE
                    prev_y = y_offset + 640
                    draw.text(
                        (FRAME_W // 2, prev_y),
                        'PREV CLOSE',
                        font=font_prev_label,
                        fill=SECONDARY_COLOR,
                        anchor='mm',
                    )
                    prev_str = self._format_number(m['prev'])
                    unit_str = m['unit'] if m['unit'] in ('%', 'pts') else ''
                    prefix_str = m['unit'] if m['unit'] not in ('%', 'pts') else ''
                    draw.text(
                        (FRAME_W // 2, prev_y + 40),
                        f'{prefix_str}{prev_str}{unit_str}',
                        font=font_prev_val,
                        fill=SECONDARY_COLOR,
                        anchor='mm',
                    )

                # === DIVIDER LINE ===
                draw.line(
                    [(40, PANEL_H), (FRAME_W - 40, PANEL_H)],
                    fill=ACCENT_COLOR,
                    width=4,
                )

                # === BOTTOM INFO BAND ===
                draw.line([(0, FRAME_H - 70), (FRAME_W, FRAME_H - 70)],
                          fill=ACCENT_COLOR, width=2)
                draw.rectangle(
                    [(0, FRAME_H - 68), (FRAME_W, FRAME_H)],
                    fill=(*PANEL_COLOR, 255),
                )

                wm_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                wm_draw = ImageDraw.Draw(wm_layer)
                wm_draw.text(
                    (40, FRAME_H - 35),
                    '@ChartDrop',
                    font=font_watermark,
                    fill=(255, 255, 255, 100),
                    anchor='lm',
                )
                wm_draw.text(
                    (FRAME_W - 40, FRAME_H - 35),
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

        logger.info('[dataforge] SplitRenderer: output -> %s', output_path)
        return str(output_path)

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
    def _ease_out_cubic(t: float) -> float:
        """Ease-out cubic: fast start, decelerates to final value."""
        return 1 - (1 - t) ** 3

    @staticmethod
    def _format_number(value: float) -> str:
        """Format number for display (no currency prefix)."""
        abs_val = abs(value)
        sign = '-' if value < 0 else ''
        if abs_val >= 1_000_000_000_000:
            return f'{sign}{abs_val / 1_000_000_000_000:.2f}T'
        if abs_val >= 1_000_000_000:
            return f'{sign}{abs_val / 1_000_000_000:.2f}B'
        if abs_val >= 1_000_000:
            return f'{sign}{abs_val / 1_000_000:.2f}M'
        if abs_val >= 1_000:
            return f'{sign}{abs_val / 1_000:.1f}K'
        return f'{sign}{value:,.2f}'

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
    """Smoke test — renders a 10-second split-screen video."""
    logging.basicConfig(level=logging.INFO)

    print('\n[dataforge] Testing SplitRenderer...')

    renderer = SplitRenderer()
    print(f'  Font path: {renderer.font_path}')

    metric_a = {'name': 'Fed Funds Rate', 'current': 5.33, 'prev': 5.50, 'unit': '%'}
    metric_b = {'name': 'US Inflation (CPI)', 'current': 3.2, 'prev': 3.4, 'unit': '%'}

    print(f'\n  Metric A: {metric_a["name"]} ({metric_a["prev"]} -> {metric_a["current"]})')
    print(f'  Metric B: {metric_b["name"]} ({metric_b["prev"]} -> {metric_b["current"]})')

    print('\n  Rendering 10-second split-screen...')
    try:
        path = renderer.render(
            metric_a=metric_a,
            metric_b=metric_b,
            duration_sec=10.0,
            story_id='test_split',
            source_credit='Source: FRED',
        )
        size_mb = Path(path).stat().st_size / 1_048_576
        print(f'  [OK] {path} ({size_mb:.1f} MB)')
    except Exception as e:
        print(f'  [FAIL] {e}')
        import traceback
        traceback.print_exc()

    print('\n[dataforge] SplitRenderer test complete.')


if __name__ == '__main__':
    test_render()
