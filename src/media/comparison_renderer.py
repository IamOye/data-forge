"""
comparison_renderer.py — ComparisonRenderer

Two-metric side-by-side animated ticker race.
Left panel vs right panel, both animating simultaneously.
Winner (better % performance) gets teal glow at hold phase.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)
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

FRAME_W = 1080
FRAME_H = 1920
RENDER_FPS = 15
OUTPUT_FPS = 30
BG_COLOR      = (13, 17, 23)
PANEL_COLOR   = (17, 24, 39)
TEXT_COLOR    = (255, 255, 255)
SECONDARY     = (139, 148, 158)
ACCENT        = (0, 212, 170)
UP_COLOR      = (38, 166, 91)
DOWN_COLOR    = (231, 76, 60)
RULE_COLOR    = (45, 55, 72)

FONT_CANDIDATES = [
    os.environ.get('FONT_PATH', ''),
    '/app/assets/fonts/Roboto-Bold.ttf',
    '/app/data/fonts/Roboto-Bold.ttf',
    'assets/fonts/Roboto-Bold.ttf',
    os.path.join(os.path.dirname(__file__), '..', '..', 'assets', 'fonts', 'Roboto-Bold.ttf'),
]

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))

# Layout
PANEL_W   = 500   # each metric panel width
DIVIDER_W = 80    # centre VS divider
LEFT_X    = 0     # left panel starts here
RIGHT_X   = 580   # right panel starts here
PANEL_MID_L = 250  # centre x of left panel
PANEL_MID_R = 830  # centre x of right panel


class ComparisonRenderer:
    """
    Renders two metrics side by side as animated tickers.
    Both values animate simultaneously. Winner highlighted in teal at hold.
    """

    def __init__(self, output_dir=OUTPUT_DIR):
        self.output_dir = Path(output_dir)
        self.font_path = self._resolve_font()

    def render(
        self,
        metric_a: dict,
        metric_b: dict,
        duration_sec: float = 55.0,
        story_id: str = 'comparison_000',
        source_credit: str = 'Source: FRED',
    ) -> str:
        """
        Render a two-metric comparison ticker to MP4.

        Args:
            metric_a: dict with keys: name, current, prev, unit
            metric_b: dict with keys: name, current, prev, unit
            duration_sec: Total video duration.
            story_id: Used for output filename.
            source_credit: Attribution text.

        Returns:
            str: Path to rendered MP4.
        """
        from PIL import Image, ImageDraw

        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f'{story_id}_video.mp4'
        total_frames = int(duration_sec * RENDER_FPS)
        animate_frames = int(total_frames * 0.60)
        hold_frames = total_frames - animate_frames

        # Determine winner by pct change
        pct_a = ((metric_a['current'] - metric_a['prev']) / abs(metric_a['prev']) * 100) if metric_a['prev'] else 0.0
        pct_b = ((metric_b['current'] - metric_b['prev']) / abs(metric_b['prev']) * 100) if metric_b['prev'] else 0.0
        a_wins = pct_a >= pct_b

        color_a = UP_COLOR if pct_a >= 0 else DOWN_COLOR
        color_b = UP_COLOR if pct_b >= 0 else DOWN_COLOR

        font_label  = self._load_font(34)
        font_number = self._load_font(96)
        font_unit   = self._load_font(52)
        font_pct    = self._load_font(44)
        font_vs     = self._load_font(72)
        font_wm     = self._load_font(30)
        font_src    = self._load_font(26)
        font_winner = self._load_font(36)

        logger.info(
            '[dataforge] ComparisonRenderer: %s vs %s, %d frames for %s',
            metric_a['name'], metric_b['name'], total_frames, story_id,
        )

        volume_tmp = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw')) / 'tmp_frames_comparison'

        try:
            volume_tmp.mkdir(parents=True, exist_ok=True)

            for i in range(total_frames):
                # Ease-out animation progress
                if i < animate_frames:
                    t = i / max(animate_frames - 1, 1)
                    eased = 1 - (1 - t) ** 3
                else:
                    eased = 1.0

                val_a = metric_a['prev'] + (metric_a['current'] - metric_a['prev']) * eased
                val_b = metric_b['prev'] + (metric_b['current'] - metric_b['prev']) * eased

                in_hold = i >= animate_frames
                hold_progress = (i - animate_frames) / max(hold_frames, 1) if in_hold else 0.0

                img = Image.new('RGB', (FRAME_W, FRAME_H), BG_COLOR)
                draw = ImageDraw.Draw(img)

                # Top accent bar
                draw.rectangle([(0, 0), (FRAME_W, 8)], fill=ACCENT)

                # ===== HEADER (y: 8-180) =====
                draw.rectangle([(0, 8), (FRAME_W, 180)], fill=PANEL_COLOR)
                draw.text(
                    (FRAME_W // 2, 94),
                    'MARKET COMPARISON',
                    font=font_label,
                    fill=SECONDARY,
                    anchor='mm',
                )
                draw.text(
                    (FRAME_W // 2, 148),
                    'Live Data · ChartDrop',
                    font=self._load_font(28),
                    fill=(80, 90, 100),
                    anchor='mm',
                )

                # ===== CENTRE DIVIDER — VS (y: 180-1750) =====
                draw.rectangle([(PANEL_W, 180), (PANEL_W + DIVIDER_W, 1750)], fill=(20, 25, 35))
                draw.line([(PANEL_W, 180), (PANEL_W, 1750)], fill=RULE_COLOR, width=1)
                draw.line([(PANEL_W + DIVIDER_W, 180), (PANEL_W + DIVIDER_W, 1750)], fill=RULE_COLOR, width=1)

                # VS text — pulse gently in hold phase
                vs_alpha = int(200 + 55 * math.sin(hold_progress * math.pi * 3)) if in_hold else 200
                vs_layer = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
                vs_draw = ImageDraw.Draw(vs_layer)
                vs_draw.text(
                    (PANEL_W + DIVIDER_W // 2, FRAME_H // 2),
                    'VS',
                    font=font_vs,
                    fill=(*ACCENT, vs_alpha),
                    anchor='mm',
                )
                img = Image.alpha_composite(img.convert('RGBA'), vs_layer).convert('RGB')
                draw = ImageDraw.Draw(img)

                # ===== LEFT PANEL — METRIC A =====
                self._draw_metric_panel(
                    draw, img,
                    x_centre=PANEL_MID_L,
                    name=metric_a['name'],
                    value=val_a,
                    current=metric_a['current'],
                    prev=metric_a['prev'],
                    unit=metric_a.get('unit', '$'),
                    pct=pct_a,
                    color=color_a,
                    is_winner=a_wins,
                    in_hold=in_hold,
                    hold_progress=hold_progress,
                    font_label=font_label,
                    font_number=font_number,
                    font_unit=font_unit,
                    font_pct=font_pct,
                    font_winner=font_winner,
                    panel_left=LEFT_X,
                    panel_right=PANEL_W,
                )

                # ===== RIGHT PANEL — METRIC B =====
                self._draw_metric_panel(
                    draw, img,
                    x_centre=PANEL_MID_R,
                    name=metric_b['name'],
                    value=val_b,
                    current=metric_b['current'],
                    prev=metric_b['prev'],
                    unit=metric_b.get('unit', '$'),
                    pct=pct_b,
                    color=color_b,
                    is_winner=not a_wins,
                    in_hold=in_hold,
                    hold_progress=hold_progress,
                    font_label=font_label,
                    font_number=font_number,
                    font_unit=font_unit,
                    font_pct=font_pct,
                    font_winner=font_winner,
                    panel_left=RIGHT_X,
                    panel_right=FRAME_W,
                )

                # Bottom accent bar
                draw.rectangle([(0, FRAME_H - 8), (FRAME_W, FRAME_H)], fill=ACCENT)

                # Watermark + source
                draw.text((40, FRAME_H - 50), '@ChartDrop', font=font_wm, fill=SECONDARY)
                draw.text((FRAME_W - 40, FRAME_H - 50), source_credit,
                          font=font_src, fill=SECONDARY, anchor='ra')

                frame_path = volume_tmp / f'frame_{i:05d}.jpg'
                img.save(frame_path, 'JPEG', quality=85)
                del img

            logger.info('[dataforge] ComparisonRenderer: %d frames written', total_frames)
            self._frames_to_mp4(volume_tmp, output_path, RENDER_FPS, OUTPUT_FPS)

        finally:
            if volume_tmp.exists():
                shutil.rmtree(volume_tmp, ignore_errors=True)

        logger.info('[dataforge] ComparisonRenderer: output -> %s', output_path)
        return str(output_path)

    def _draw_metric_panel(
        self, draw, img,
        x_centre, name, value, current, prev, unit, pct, color,
        is_winner, in_hold, hold_progress,
        font_label, font_number, font_unit, font_pct, font_winner,
        panel_left, panel_right,
    ):
        from PIL import Image, ImageDraw

        sign = '+' if pct >= 0 else ''
        arrow = '▲' if pct >= 0 else '▼'
        panel_mid_y = 860

        # Winner glow border in hold phase
        if in_hold and is_winner:
            glow_alpha = int(60 + 40 * math.sin(hold_progress * math.pi * 4))
            glow = Image.new('RGBA', (FRAME_W, FRAME_H), (0, 0, 0, 0))
            glow_draw = ImageDraw.Draw(glow)
            glow_draw.rectangle(
                [(panel_left + 4, 188), (panel_right - 4, 1748)],
                outline=(*ACCENT, glow_alpha),
                width=4,
            )
            img_rgba = img.convert('RGBA')
            img_rgba = Image.alpha_composite(img_rgba, glow)
            img.paste(img_rgba.convert('RGB'))
            draw = ImageDraw.Draw(img)

        # Metric name
        draw.text(
            (x_centre, 240),
            name.upper(),
            font=font_label,
            fill=SECONDARY,
            anchor='mm',
        )

        # Animated value
        val_str = self._format_value(value, unit)
        draw.text(
            (x_centre, panel_mid_y),
            val_str,
            font=font_number,
            fill=TEXT_COLOR,
            anchor='mm',
        )

        # Unit label below value
        draw.text(
            (x_centre, panel_mid_y + 80),
            unit,
            font=font_unit,
            fill=SECONDARY,
            anchor='mm',
        )

        # Pct change badge
        pct_text = f'{arrow} {sign}{pct:.2f}%'
        draw.text(
            (x_centre, panel_mid_y + 160),
            pct_text,
            font=font_pct,
            fill=color,
            anchor='mm',
        )

        # Winner label
        if in_hold and is_winner:
            draw.text(
                (x_centre, panel_mid_y + 240),
                '★ WINNING',
                font=font_winner,
                fill=ACCENT,
                anchor='mm',
            )

        # Prev value
        draw.text(
            (x_centre, 1600),
            f'PREV: {self._format_value(prev, unit)}',
            font=self._load_font(28),
            fill=(80, 90, 100),
            anchor='mm',
        )

    @staticmethod
    def _format_value(value: float, unit: str = '$') -> str:
        abs_val = abs(value)
        sign = '-' if value < 0 else ''
        suffix_units = ('%', 'pts', 'Y')
        pre = '' if unit in suffix_units else unit
        post = unit if unit in suffix_units else ''
        if abs_val >= 1_000_000_000_000:
            return f'{sign}{pre}{abs_val / 1_000_000_000_000:.2f}T{post}'
        if abs_val >= 1_000_000_000:
            return f'{sign}{pre}{abs_val / 1_000_000_000:.2f}B{post}'
        if abs_val >= 1_000_000:
            return f'{sign}{pre}{abs_val / 1_000_000:.2f}M{post}'
        if abs_val >= 1_000:
            return f'{sign}{pre}{abs_val / 1_000:.1f}K{post}'
        if abs_val < 1:
            return f'{sign}{pre}{abs_val:.4f}{post}'
        return f'{sign}{pre}{value:,.2f}{post}'

    def _resolve_font(self):
        for candidate in FONT_CANDIDATES:
            if candidate and os.path.exists(candidate):
                from PIL import ImageFont
                ImageFont.truetype(candidate, 48)
                logger.info('[dataforge] Font resolved: %s', candidate)
                return os.path.abspath(candidate)
        raise RuntimeError('[dataforge] Roboto-Bold.ttf not found.')

    def _load_font(self, size):
        from PIL import ImageFont
        return ImageFont.truetype(self.font_path, size)

    @staticmethod
    def _frames_to_mp4(frames_dir, output_path, input_fps, output_fps=30):
        ffmpeg_bin = os.environ.get('FFMPEG_BINARY', '')
        if not ffmpeg_bin or not Path(ffmpeg_bin).exists():
            try:
                import imageio_ffmpeg
                ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
            except Exception:
                ffmpeg_bin = 'ffmpeg'

        from PIL import Image as _Image
        check = _Image.open(frames_dir / 'frame_00000.jpg')
        assert check.size == (FRAME_W, FRAME_H)
        check.close()

        cmd = [
            ffmpeg_bin, '-y',
            '-framerate', str(input_fps),
            '-i', str(frames_dir / 'frame_%05d.jpg'),
            '-vf', f'scale={FRAME_W}:{FRAME_H}:force_original_aspect_ratio=disable',
            '-s', f'{FRAME_W}x{FRAME_H}',
            '-c:v', 'libx264', '-preset', 'fast',
            '-crf', '18', '-pix_fmt', 'yuv420p',
            '-r', str(output_fps),
            str(output_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            raise RuntimeError(f'ffmpeg failed: {result.stderr[-200:]}')
        logger.info('[dataforge] ffmpeg assembled %s', output_path)
