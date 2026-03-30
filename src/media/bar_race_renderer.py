"""
bar_race_renderer.py — BarRaceRenderer

Renders an animated horizontal bar chart race to MP4 using matplotlib.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)

Visual spec (DataForge identity):
  Background:   #0D1117
  Bar colors:   cycle through 8 accent colors, consistent per entity
  Text color:   #FFFFFF
  Secondary:    #8B949E
  Font:         Roboto-Bold.ttf (same resolution as kinetic_renderer.py)
  Frame size:   1080 x 1920 px
  Frame rate:   30 fps
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
FPS = 30
BG_HEX = '#0D1117'
TEXT_HEX = '#FFFFFF'
SECONDARY_HEX = '#8B949E'

ACCENT_COLORS = [
    '#00D4AA', '#E74C3C', '#3498DB', '#F39C12',
    '#9B59B6', '#1ABC9C', '#E67E22', '#2ECC71',
]

FONT_PATH = os.environ.get(
    'FONT_PATH',
    '/app/assets/fonts/Roboto-Bold.ttf',
)
LOCAL_FONT_FALLBACK = str(
    Path(__file__).resolve().parent.parent.parent / 'assets' / 'fonts' / 'Roboto-Bold.ttf'
)

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))


# ---------------------------------------------------------------------------
# ffmpeg resolver (same pattern as kinetic_renderer.py)
# ---------------------------------------------------------------------------

def _resolve_ffmpeg() -> str:
    """Resolve ffmpeg binary: FFMPEG_BINARY env → imageio_ffmpeg → system."""
    ffmpeg_bin = os.environ.get('FFMPEG_BINARY', '')
    if ffmpeg_bin and Path(ffmpeg_bin).exists():
        return ffmpeg_bin
    try:
        import imageio_ffmpeg
        path = imageio_ffmpeg.get_ffmpeg_exe()
        if path and Path(path).exists():
            return path
    except Exception:
        pass
    return 'ffmpeg'


def _resolve_font() -> str:
    """Return font path — env var, local fallback, or empty string."""
    if Path(FONT_PATH).exists():
        return FONT_PATH
    if Path(LOCAL_FONT_FALLBACK).exists():
        logger.info('[dataforge] Using local font fallback: %s', LOCAL_FONT_FALLBACK)
        return LOCAL_FONT_FALLBACK
    logger.warning(
        '[dataforge] Roboto-Bold.ttf not found — matplotlib will use default font'
    )
    return ''


# ---------------------------------------------------------------------------
# Value formatting
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# BarRaceRenderer
# ---------------------------------------------------------------------------

class BarRaceRenderer:
    """
    Renders an animated horizontal bar chart race as a portrait MP4.

    Each entity keeps a consistent colour throughout the animation.
    Rankings animate smoothly between time periods.
    """

    def __init__(self, output_dir: str | Path = OUTPUT_DIR) -> None:
        self.output_dir = Path(output_dir)
        self.font_path = _resolve_font()

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
            value_label:   Label for the value axis (e.g. 'USD', 'GDP').
            duration_sec:  Total video duration in seconds.
            top_n:         Number of bars to show.
            story_id:      Used for output filename.
            source_credit: Attribution text bottom-right.

        Returns:
            str: Path to the rendered MP4 file.
        """
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from matplotlib.animation import FuncAnimation, FFMpegWriter
        from matplotlib import font_manager

        # --- Resolve ffmpeg ---
        ffmpeg_bin = _resolve_ffmpeg()
        matplotlib.rcParams['animation.ffmpeg_path'] = ffmpeg_bin
        logger.info('[dataforge] BarRaceRenderer using ffmpeg: %s', ffmpeg_bin)

        # --- Register font ---
        if self.font_path and Path(self.font_path).exists():
            font_manager.fontManager.addfont(self.font_path)
            font_prop = font_manager.FontProperties(fname=self.font_path)
            font_name = font_prop.get_name()
            matplotlib.rcParams['font.family'] = font_name
            logger.info('[dataforge] Using font: %s', font_name)

        # --- Assign consistent colours per entity ---
        all_entities = list(df.index)
        entity_colors: dict[str, str] = {}
        for i, entity in enumerate(all_entities):
            entity_colors[entity] = ACCENT_COLORS[i % len(ACCENT_COLORS)]

        # --- Build interpolated frames ---
        columns = list(df.columns)
        num_periods = len(columns)
        frames_per_step = max(1, int((duration_sec * FPS) / max(num_periods - 1, 1)))
        total_frames = frames_per_step * max(num_periods - 1, 1)

        logger.info(
            '[dataforge] BarRaceRenderer: %d periods, %d frames/step, %d total frames',
            num_periods, frames_per_step, total_frames,
        )

        # Pre-compute interpolated data for every frame
        frame_data: list[tuple[pd.Series, str]] = []
        for period_idx in range(num_periods - 1):
            col_a = columns[period_idx]
            col_b = columns[period_idx + 1]
            vals_a = df[col_a].astype(float)
            vals_b = df[col_b].astype(float)
            for f in range(frames_per_step):
                t = f / frames_per_step
                interpolated = vals_a + (vals_b - vals_a) * t
                # Show period label: switch to next label at halfway point
                label = col_a if t < 0.5 else col_b
                frame_data.append((interpolated, label))
        # Append final frame holding on the last period
        frame_data.append((df[columns[-1]].astype(float), columns[-1]))
        total_frames = len(frame_data)

        # --- Setup figure ---
        fig = plt.figure(figsize=(FRAME_W / 96, FRAME_H / 96), dpi=96)
        fig.patch.set_facecolor(BG_HEX)
        ax = fig.add_subplot(111)
        ax.set_facecolor(BG_HEX)

        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f'{story_id}_video.mp4'

        def _draw_frame(frame_idx: int) -> None:
            ax.clear()
            ax.set_facecolor(BG_HEX)

            values, period_label = frame_data[frame_idx]
            sorted_vals = values.sort_values(ascending=True)
            top_entities = sorted_vals.tail(top_n)

            max_val = top_entities.max() if len(top_entities) > 0 else 1.0
            if max_val <= 0:
                max_val = 1.0

            # Draw horizontal bars
            y_positions = np.arange(len(top_entities))
            bar_colors = [entity_colors.get(e, ACCENT_COLORS[0]) for e in top_entities.index]
            bars = ax.barh(
                y_positions, top_entities.values,
                color=bar_colors, height=0.7, edgecolor='none',
            )

            # Bar labels: entity name on left, value on right
            for i, (entity, val) in enumerate(zip(top_entities.index, top_entities.values)):
                # Entity name (left of bar)
                ax.text(
                    -max_val * 0.01, i, f'  {entity}',
                    va='center', ha='right', color=TEXT_HEX,
                    fontsize=14, fontweight='bold',
                )
                # Value (right end of bar)
                ax.text(
                    val + max_val * 0.01, i, f' {_format_value(val)}',
                    va='center', ha='left', color=TEXT_HEX,
                    fontsize=12,
                )

            # Set axis limits — max bar = 85% of frame width
            ax.set_xlim(0, max_val / 0.85)
            ax.set_ylim(-0.5, top_n - 0.5)

            # Remove axes clutter
            ax.set_yticks([])
            ax.set_xticks([])
            for spine in ax.spines.values():
                spine.set_visible(False)

            # Title — top-center
            ax.text(
                0.5, 1.02, title,
                transform=ax.transAxes, ha='center', va='bottom',
                fontsize=22, fontweight='bold', color=TEXT_HEX,
            )

            # Period label — top-right, large
            ax.text(
                0.95, 0.95, str(period_label),
                transform=ax.transAxes, ha='right', va='top',
                fontsize=48, fontweight='bold', color=SECONDARY_HEX,
                alpha=0.8,
            )

            # Watermark — bottom-left
            ax.text(
                0.02, 0.01, '@ChartDrop',
                transform=ax.transAxes, ha='left', va='bottom',
                fontsize=10, color=SECONDARY_HEX, alpha=0.4,
            )

            # Source credit — bottom-right
            ax.text(
                0.98, 0.01, source_credit,
                transform=ax.transAxes, ha='right', va='bottom',
                fontsize=10, color=SECONDARY_HEX, alpha=0.4,
            )

            fig.subplots_adjust(left=0.25, right=0.88, top=0.90, bottom=0.05)

        # --- Animate ---
        anim = FuncAnimation(
            fig, _draw_frame,
            frames=total_frames,
            repeat=False,
        )

        writer = FFMpegWriter(
            fps=FPS,
            codec='libx264',
            extra_args=['-pix_fmt', 'yuv420p', '-crf', '18', '-preset', 'fast'],
        )

        anim.save(str(output_path), writer=writer)
        plt.close(fig)

        file_size = output_path.stat().st_size / 1_048_576
        logger.info(
            '[dataforge] BarRaceRenderer: output → %s (%.1f MB)',
            output_path, file_size,
        )
        return str(output_path)


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_render() -> None:
    """
    Smoke test — renders a short 10-second bar chart race locally.
    Run: python src/media/bar_race_renderer.py
    """
    logging.basicConfig(level=logging.INFO)

    print('\n[dataforge] Testing BarRaceRenderer...')

    renderer = BarRaceRenderer()
    print(f'  Font path resolved: {renderer.font_path or "matplotlib default (no TTF found)"}')
    print(f'  ffmpeg: {_resolve_ffmpeg()}')

    # Sample DataFrame: 5 countries, 5 years, GDP-like values
    np.random.seed(42)
    countries = ['USA', 'China', 'Japan', 'Germany', 'UK']
    years = ['2018', '2019', '2020', '2021', '2022']

    # Start with base values and grow them with some randomness
    base_values = {
        'USA':     20_000_000_000_000,
        'China':   14_000_000_000_000,
        'Japan':    5_000_000_000_000,
        'Germany':  4_000_000_000_000,
        'UK':       2_800_000_000_000,
    }
    data: dict[str, list[float]] = {country: [] for country in countries}
    for country in countries:
        val = base_values[country]
        for _ in years:
            data[country].append(val)
            val *= 1.0 + np.random.uniform(-0.02, 0.08)

    df = pd.DataFrame(data, index=years).T  # index=countries, columns=years

    print(f'\n  DataFrame shape: {df.shape}')
    print(f'  Entities: {list(df.index)}')
    print(f'  Periods:  {list(df.columns)}')

    print('\n  Rendering 10-second test bar race...')
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
        print(f'  [OK] Video rendered: {path} ({size_mb:.1f} MB)')
    except RuntimeError as e:
        print(f'  [FAIL] Render failed: {e}')
        print('    (ffmpeg may not be installed locally — this will work on Railway)')
    except Exception as e:
        print(f'  [FAIL] Unexpected error: {e}')

    print('\n[dataforge] BarRaceRenderer test complete.')


if __name__ == '__main__':
    test_render()
