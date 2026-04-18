"""
bar_race_renderer.py — BarRaceRenderer

Renders an animated horizontal bar chart race to MP4 using Pillow.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)

Visual spec (DataForge identity):
  Background:   #0D1117
  Bar colours:  8 accent colours, consistent per entity
  Text:         #FFFFFF primary, #8B949E secondary
  Font:         Roboto-Bold.ttf
  Frame size:   1080 x 1920 px
  Frame rate:   15fps render, 30fps output
  Codec:        H.264 libx264, yuv420p, crf=18, preset=fast

Usage:
    renderer = BarRaceRenderer()
    mp4_path = renderer.render(
        df=gdp_df, title='GDP by Country', value_label='USD',
        duration_sec=55.0, top_n=10, story_id='bar_race_001',
    )
"""

import logging
import os
import shutil
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
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
TEXT_COLOR = (255, 255, 255)       # #FFFFFF
SECONDARY_COLOR = (139, 148, 158)  # #8B949E
ACCENT_COLOR = (0, 212, 170)      # #00D4AA

ACCENT_COLORS = [
    (0, 212, 170),    # #00D4AA
    (231, 76, 60),    # #E74C3C
    (52, 152, 219),   # #3498DB
    (243, 156, 18),   # #F39C12
    (155, 89, 182),   # #9B59B6
    (26, 188, 156),   # #1ABC9C
    (230, 126, 34),   # #E67E22
    (46, 204, 113),   # #2ECC71
]

FONT_CANDIDATES = [
    os.environ.get('FONT_PATH', ''),
    '/app/assets/fonts/Roboto-Bold.ttf',
    '/app/data/fonts/Roboto-Bold.ttf',
    'assets/fonts/Roboto-Bold.ttf',
    os.path.join(os.path.dirname(__file__), '..', '..',
                 'assets', 'fonts', 'Roboto-Bold.ttf'),
]

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))

# Layout constants
HEADER_Y = 100        # Title y position
PERIOD_Y = 80         # Period label y
BAR_AREA_TOP = 200    # Top of bar area
BAR_AREA_BOTTOM = 1780  # Bottom of bar area
BAR_LEFT = 300        # Left edge of bars (entity labels go left of this)
BAR_RIGHT = 920       # Right edge of max bar — leaves 160px for value labels
BAR_PADDING = 12      # Vertical padding between bars


# ---------------------------------------------------------------------------
# BarRaceRenderer
# ---------------------------------------------------------------------------

class BarRaceRenderer:
    """
    Renders an animated horizontal bar chart race as a portrait MP4.
    Each entity keeps a consistent colour. Rankings animate smoothly.
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
        df: pd.DataFrame,
        title: str,
        value_label: str,
        duration_sec: float = 55.0,
        top_n: int = 10,
        story_id: str = 'bar_race_000',
        source_credit: str = 'Source: World Bank',
    ) -> str:
        """
        Render a bar chart race animation to MP4.

        Args:
            df:            DataFrame — index=entity names, columns=time periods,
                           values=numeric.
            title:         Title text shown at the top.
            value_label:   Label for values (e.g. 'USD', 'GDP').
            duration_sec:  Total video duration in seconds.
            top_n:         Number of bars to show.
            story_id:      Used for output filename.
            source_credit: Attribution text bottom-right.

        Returns:
            str: Path to the rendered MP4 file.
        """
        from PIL import Image, ImageDraw

        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f'{story_id}_video.mp4'

        # Assign consistent colours per entity
        all_entities = list(df.index)
        entity_colors = {
            e: ACCENT_COLORS[i % len(ACCENT_COLORS)]
            for i, e in enumerate(all_entities)
        }

        columns = list(df.columns)
        num_periods = len(columns)
        frames_per_step = max(1, int((duration_sec * RENDER_FPS) / max(num_periods - 1, 1)))
        total_frames = frames_per_step * max(num_periods - 1, 1) + 1

        # Fonts
        font_title = self._load_font(52)
        font_period = self._load_font(96)
        font_entity = self._load_font(36)
        font_value = self._load_font(32)
        font_watermark = self._load_font(36)
        font_source = self._load_font(30)

        logger.info(
            '[dataforge] BarRaceRenderer: %d periods, %d frames/step, %d total, '
            'render@%dfps->output@%dfps for %s',
            num_periods, frames_per_step, total_frames,
            RENDER_FPS, OUTPUT_FPS, story_id,
        )

        # Bar geometry
        bar_area_height = BAR_AREA_BOTTOM - BAR_AREA_TOP
        bar_slot_height = bar_area_height // top_n
        bar_height = bar_slot_height - BAR_PADDING * 2

        volume_tmp = Path(os.environ.get(
            'DATAFORGE_RAW_DIR', 'data/raw'
        )) / 'tmp_frames_bar'

        try:
            volume_tmp.mkdir(parents=True, exist_ok=True)
            frame_idx = 0

            for period_idx in range(num_periods - 1):
                col_a = columns[period_idx]
                col_b = columns[period_idx + 1]
                vals_a = df[col_a].astype(float)
                vals_b = df[col_b].astype(float)

                for f in range(frames_per_step):
                    t = f / frames_per_step
                    interpolated = vals_a + (vals_b - vals_a) * t
                    period_label = col_a if t < 0.5 else col_b

                    self._draw_frame(
                        frame_idx, volume_tmp, interpolated, period_label,
                        title, source_credit, entity_colors, top_n,
                        bar_slot_height, bar_height,
                        font_title, font_period, font_entity, font_value,
                        font_watermark, font_source,
                    )
                    frame_idx += 1

            # Final hold frame
            final_vals = df[columns[-1]].astype(float)
            self._draw_frame(
                frame_idx, volume_tmp, final_vals, columns[-1],
                title, source_credit, entity_colors, top_n,
                bar_slot_height, bar_height,
                font_title, font_period, font_entity, font_value,
                font_watermark, font_source,
            )

            logger.info('[dataforge] BarRaceRenderer: %d frames written to disk', frame_idx + 1)

            # Assemble MP4
            self._frames_to_mp4(volume_tmp, output_path, RENDER_FPS, OUTPUT_FPS)

        finally:
            if volume_tmp.exists():
                shutil.rmtree(volume_tmp, ignore_errors=True)
                logger.info('[dataforge] Temp frames cleaned up')

        logger.info('[dataforge] BarRaceRenderer: output -> %s', output_path)
        return str(output_path)

    # ------------------------------------------------------------------
    # Frame drawing
    # ------------------------------------------------------------------

    def _draw_frame(
        self, frame_idx, volume_tmp, values, period_label,
        title, source_credit, entity_colors, top_n,
        bar_slot_height, bar_height,
        font_title, font_period, font_entity, font_value,
        font_watermark, font_source,
    ):
        from PIL import Image, ImageDraw

        sorted_vals = values.sort_values(ascending=False).head(top_n)
        max_val = sorted_vals.max() if len(sorted_vals) > 0 else 1.0
        if max_val <= 0:
            max_val = 1.0

        img = Image.new('RGB', (FRAME_W, FRAME_H), BG_COLOR)
        draw = ImageDraw.Draw(img)

        # Teal accent bar top
        draw.rectangle([(0, 0), (FRAME_W, 8)], fill=ACCENT_COLOR)

        # Title — top-centre
        draw.text(
            (FRAME_W // 2, HEADER_Y),
            title,
            font=font_title,
            fill=TEXT_COLOR,
            anchor='mm',
        )

        # Period label — top-right, large
        draw.text(
            (FRAME_W - 60, PERIOD_Y),
            str(period_label),
            font=font_period,
            fill=SECONDARY_COLOR,
            anchor='rm',
        )

        # Draw bars
        bar_max_width = BAR_RIGHT - BAR_LEFT
        for rank, (entity, val) in enumerate(sorted_vals.items()):
            bar_y = BAR_AREA_TOP + rank * bar_slot_height + BAR_PADDING
            bar_w = int((val / max_val) * bar_max_width)
            bar_w = max(bar_w, 4)  # minimum visible width
            color = entity_colors.get(entity, ACCENT_COLORS[0])

            # Bar rectangle
            draw.rectangle(
                [(BAR_LEFT, bar_y), (BAR_LEFT + bar_w, bar_y + bar_height)],
                fill=color,
            )

            # Entity name — left of bar, right-aligned
            draw.text(
                (BAR_LEFT - 16, bar_y + bar_height // 2),
                str(entity),
                font=font_entity,
                fill=TEXT_COLOR,
                anchor='rm',
            )

            # Value label — right end of bar
            val_str = self._format_value(val)
            draw.text(
                (BAR_LEFT + bar_w + 12, bar_y + bar_height // 2),
                val_str,
                font=font_value,
                fill=TEXT_COLOR,
                anchor='lm',
            )

        # Teal accent bar bottom
        draw.rectangle([(0, FRAME_H - 8), (FRAME_W, FRAME_H)], fill=ACCENT_COLOR)

        # Watermark bottom-left
        draw.text(
            (40, FRAME_H - 50),
            '@ChartDrop',
            font=font_watermark,
            fill=(*SECONDARY_COLOR, 102),
        )

        # Source credit bottom-right
        draw.text(
            (FRAME_W - 40, FRAME_H - 50),
            source_credit,
            font=font_source,
            fill=(*SECONDARY_COLOR, 102),
            anchor='ra',
        )

        # Save to disk immediately
        frame_path = volume_tmp / f'frame_{frame_idx:05d}.jpg'
        img.save(frame_path, 'JPEG', quality=85)
        del img

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
            logger.info('[dataforge] Font resolved: %s', font_path)
            from PIL import ImageFont
            ImageFont.truetype(font_path, 48)  # verify
            return font_path
        else:
            logger.error('[dataforge] FONT NOT FOUND -- searched: %s', FONT_CANDIDATES)
            raise RuntimeError(
                '[dataforge] Roboto-Bold.ttf not found. '
                'Upload font to /app/assets/fonts/ on Railway volume.'
            )

    def _load_font(self, size: int):
        """Load Roboto-Bold at given size."""
        from PIL import ImageFont
        return ImageFont.truetype(self.font_path, size)

    @staticmethod
    def _format_value(value: float) -> str:
        """Format large numbers with T/B/M/K suffix."""
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

        # Verify first frame
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

        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=300,
        )

        if result.returncode != 0:
            logger.error('[dataforge] ffmpeg stderr:\n%s', result.stderr[-1000:])
            raise RuntimeError(
                f'ffmpeg failed (code {result.returncode}): '
                f'{result.stderr[-200:]}'
            )

        # Probe output
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
    """
    Smoke test — renders a 10-second bar chart race locally.
    """
    logging.basicConfig(level=logging.INFO)

    print('\n[dataforge] Testing BarRaceRenderer...')

    renderer = BarRaceRenderer()
    print(f'  Font path resolved: {renderer.font_path}')

    # Sample DataFrame: 5 countries, 5 years, GDP-like values
    np.random.seed(42)
    countries = ['USA', 'China', 'Japan', 'Germany', 'UK']
    years = ['2018', '2019', '2020', '2021', '2022']

    base_values = {
        'USA':     20_000_000_000_000,
        'China':   14_000_000_000_000,
        'Japan':    5_000_000_000_000,
        'Germany':  4_000_000_000_000,
        'UK':       2_800_000_000_000,
    }
    data: dict[str, list[float]] = {c: [] for c in countries}
    for c in countries:
        val = base_values[c]
        for _ in years:
            data[c].append(val)
            val *= 1.0 + np.random.uniform(-0.02, 0.08)

    df = pd.DataFrame(data, index=years).T

    print(f'\n  DataFrame shape: {df.shape}')
    print(f'  Entities: {list(df.index)}')
    print(f'  Periods:  {list(df.columns)}')

    print('\n  Rendering 10-second bar race...')
    try:
        path = renderer.render(
            df=df,
            title='GDP by Country',
            value_label='USD',
            duration_sec=10.0,
            top_n=5,
            story_id='test_bar_race',
            source_credit='Source: World Bank',
        )
        size_mb = Path(path).stat().st_size / 1_048_576
        print(f'  [OK] {path} ({size_mb:.1f} MB)')
    except Exception as e:
        print(f'  [FAIL] {e}')
        import traceback
        traceback.print_exc()

    print('\n[dataforge] BarRaceRenderer test complete.')


if __name__ == '__main__':
    test_render()
