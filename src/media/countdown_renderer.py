"""
countdown_renderer.py — CountdownRenderer

Animated ranking reveal — items appear one by one from #10 down to #1.
Output: 1080x1920 portrait MP4 (video only — audio merged by production_pipeline.py)
"""

import logging
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
BG_COLOR = (13, 17, 23)
TEXT_COLOR = (255, 255, 255)
SECONDARY_COLOR = (139, 148, 158)
ACCENT_COLOR = (0, 212, 170)
RANK_COLORS = [
    (0, 212, 170),   # #1 teal
    (231, 76, 60),   # #2 red
    (52, 152, 219),  # #3 blue
    (243, 156, 18),  # #4 orange
    (155, 89, 182),  # #5 purple
    (26, 188, 156),  # #6
    (230, 126, 34),  # #7
    (46, 204, 113),  # #8
    (52, 73, 94),    # #9
    (127, 140, 141), # #10
]

FONT_CANDIDATES = [
    os.environ.get('FONT_PATH', ''),
    '/app/assets/fonts/Roboto-Bold.ttf',
    '/app/data/fonts/Roboto-Bold.ttf',
    'assets/fonts/Roboto-Bold.ttf',
    os.path.join(os.path.dirname(__file__), '..', '..', 'assets', 'fonts', 'Roboto-Bold.ttf'),
]

OUTPUT_DIR = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw'))

ROW_H = 148
ROW_TOP = 220
ROW_LEFT = 60
RANK_BOX_W = 90


class CountdownRenderer:
    REVEAL_FRAMES = 8
    HOLD_FRAMES = 45
    FINAL_HOLD_FRAMES = 30

    def __init__(self, output_dir=OUTPUT_DIR):
        self.output_dir = Path(output_dir)
        self.font_path = self._resolve_font()

    def render(
        self,
        items: list,
        title: str,
        subtitle: str = '',
        duration_sec: float = 55.0,
        story_id: str = 'countdown_000',
        source_credit: str = 'Source: CoinGecko',
    ) -> str:
        """
        Render countdown ranking reveal to MP4.

        Args:
            items:        List of dicts ordered #1 first, each with keys:
                          'name' (str), 'value' (str), 'change' (str, optional)
                          Max 10 items. Reversed internally for reveal order (#10 first).
            title:        Title shown at top.
            subtitle:     Subtitle below title.
            duration_sec: Total video duration in seconds.
            story_id:     Used for output filename.
            source_credit: Attribution text.

        Returns:
            str: Path to rendered MP4.
        """
        from PIL import Image, ImageDraw

        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f'{story_id}_video.mp4'

        items = items[:10]
        n = len(items)
        reveal_order = list(reversed(items))  # reveal #10 first, #1 last

        font_title    = self._load_font(44)
        font_subtitle = self._load_font(30)
        font_rank     = self._load_font(56)
        font_name     = self._load_font(38)
        font_value    = self._load_font(34)
        font_change   = self._load_font(28)
        font_wm       = self._load_font(32)
        font_src      = self._load_font(26)

        frames_per_item = self.REVEAL_FRAMES + self.HOLD_FRAMES
        total_frames = n * frames_per_item + self.FINAL_HOLD_FRAMES
        duration_frames = int(duration_sec * RENDER_FPS)
        total_frames = max(total_frames, duration_frames)

        logger.info(
            '[dataforge] CountdownRenderer: %d items, %d total frames for %s',
            n, total_frames, story_id,
        )

        volume_tmp = Path(os.environ.get('DATAFORGE_RAW_DIR', 'data/raw')) / 'tmp_frames_countdown'

        try:
            volume_tmp.mkdir(parents=True, exist_ok=True)

            for frame_idx in range(total_frames):
                item_stage = frame_idx // frames_per_item
                stage_frame = frame_idx % frames_per_item

                n_revealed = min(item_stage + 1, n)
                in_animation = (item_stage < n) and (stage_frame < self.REVEAL_FRAMES)
                anim_progress = stage_frame / self.REVEAL_FRAMES if in_animation else 1.0

                img = Image.new('RGB', (FRAME_W, FRAME_H), BG_COLOR)
                draw = ImageDraw.Draw(img)

                # Top accent bar
                draw.rectangle([(0, 0), (FRAME_W, 8)], fill=ACCENT_COLOR)

                # Title
                draw.text((FRAME_W // 2, 90), title, font=font_title,
                          fill=TEXT_COLOR, anchor='mm')
                if subtitle:
                    draw.text((FRAME_W // 2, 148), subtitle, font=font_subtitle,
                              fill=SECONDARY_COLOR, anchor='mm')

                # Draw revealed items
                for i in range(n_revealed):
                    item = reveal_order[i]
                    rank_number = n - i

                    row_y = ROW_TOP + i * ROW_H

                    if i == item_stage and in_animation:
                        slide_offset = int((1.0 - anim_progress) * 300)
                    else:
                        slide_offset = 0

                    color = RANK_COLORS[rank_number - 1] if rank_number <= len(RANK_COLORS) else RANK_COLORS[-1]

                    # Rank box
                    box_x1 = ROW_LEFT + slide_offset
                    box_y1 = row_y + 10
                    box_x2 = box_x1 + RANK_BOX_W
                    box_y2 = row_y + ROW_H - 20
                    draw.rectangle([(box_x1, box_y1), (box_x2, box_y2)], fill=color)
                    draw.text(
                        (box_x1 + RANK_BOX_W // 2, box_y1 + (box_y2 - box_y1) // 2),
                        f'#{rank_number}', font=font_rank, fill=TEXT_COLOR, anchor='mm',
                    )

                    # Name
                    name_x = ROW_LEFT + RANK_BOX_W + 20 + slide_offset
                    draw.text((name_x, row_y + 28), str(item.get('name', '')),
                              font=font_name, fill=TEXT_COLOR)

                    # Value
                    draw.text(
                        (name_x, row_y + 76),
                        str(item.get('value', '')),
                        font=font_value,
                        fill=color if rank_number == 1 else SECONDARY_COLOR,
                    )

                    # Change
                    change = item.get('change', '')
                    if change:
                        change_str = str(change)
                        change_color = (
                            (46, 204, 113) if change_str.startswith('+')
                            else (231, 76, 60) if change_str.startswith('-')
                            else SECONDARY_COLOR
                        )
                        draw.text(
                            (FRAME_W - 80 + slide_offset, row_y + 52),
                            change_str, font=font_change,
                            fill=change_color, anchor='rm',
                        )

                    # Separator
                    if i < n_revealed - 1:
                        draw.line(
                            [(ROW_LEFT, row_y + ROW_H - 2), (FRAME_W - ROW_LEFT, row_y + ROW_H - 2)],
                            fill=(139, 148, 158), width=1,
                        )

                # Bottom accent bar
                draw.rectangle([(0, FRAME_H - 8), (FRAME_W, FRAME_H)], fill=ACCENT_COLOR)

                # Watermark
                draw.text((40, FRAME_H - 50), '@ChartDrop', font=font_wm,
                          fill=(139, 148, 158))
                draw.text((FRAME_W - 40, FRAME_H - 50), source_credit,
                          font=font_src, fill=(139, 148, 158), anchor='ra')

                frame_path = volume_tmp / f'frame_{frame_idx:05d}.jpg'
                img.save(frame_path, 'JPEG', quality=85)
                del img

            logger.info('[dataforge] CountdownRenderer: %d frames written', total_frames)
            self._frames_to_mp4(volume_tmp, output_path, RENDER_FPS, OUTPUT_FPS)

        finally:
            if volume_tmp.exists():
                shutil.rmtree(volume_tmp, ignore_errors=True)

        logger.info('[dataforge] CountdownRenderer: output -> %s', output_path)
        return str(output_path)

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
