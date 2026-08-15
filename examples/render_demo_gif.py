"""Render the demo transcript JSON into a terminal-style GIF via Pillow."""

import json
import sys
import textwrap

from PIL import Image, ImageDraw, ImageFont

TRANSCRIPT, OUT = sys.argv[1], sys.argv[2]

W, H = 920, 600
PAD_X, PAD_Y = 28, 48
LINE_H = 22
FONT = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 14, index=0)
FONT_B = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 14, index=1)

BG = (13, 17, 23)
BAR = (22, 27, 34)
COLORS = {
    "head": (230, 237, 243),
    "note": (139, 148, 158),
    "sql": (121, 192, 255),
    "err": (248, 81, 73),
    "ok": (63, 185, 80),
    "gap": (0, 0, 0),
}
DOTS = [(255, 95, 86), (255, 189, 46), (39, 201, 63)]
MAX_COLS = 102


def wrap(entry):
    text = entry["text"]
    if not text:
        return [("gap", "")]
    indent = "    " if entry["kind"] == "err" else ""
    lines = textwrap.wrap(text, MAX_COLS, subsequent_indent=indent) or [""]
    return [(entry["kind"], l) for l in lines]


def frame(lines, cursor=False):
    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    d.rectangle([0, 0, W, 34], fill=BAR)
    for i, c in enumerate(DOTS):
        d.ellipse([16 + i * 22, 12, 28 + i * 22, 24], fill=c)
    d.text((W // 2 - 60, 9), "lagaam demo", font=FONT, fill=(139, 148, 158))
    y = PAD_Y
    visible = lines[-((H - PAD_Y - 20) // LINE_H):]
    for kind, text in visible:
        font = FONT_B if kind == "head" else FONT
        d.text((PAD_X, y), text, font=font, fill=COLORS.get(kind, COLORS["note"]))
        y += LINE_H
    if cursor and visible:
        kind, text = visible[-1]
        x = PAD_X + d.textlength(text, font=FONT)
        d.rectangle([x + 2, y - LINE_H + 3, x + 10, y - 3], fill=(230, 237, 243))
    return img


entries = json.load(open(TRANSCRIPT))
frames, durations = [], []
shown = []


def emit(img, ms):
    frames.append(img)
    durations.append(ms)


emit(frame([]), 500)
for entry in entries:
    for kind, line in wrap(entry):
        if kind == "sql":
            words = line.split(" ")
            for n in range(2, len(words) + 1, 2):
                partial = " ".join(words[:n])
                emit(frame(shown + [(kind, partial)], cursor=True), 70)
            shown.append((kind, line))
            emit(frame(shown), 500)
        else:
            shown.append((kind, line))
    pause = {"err": 2600, "ok": 250, "note": 900, "head": 800, "gap": 120, "sql": 400}[
        entry["kind"]
    ]
    emit(frame(shown), pause)
emit(frame(shown), 4500)

frames[0].save(
    OUT,
    save_all=True,
    append_images=frames[1:],
    duration=durations,
    loop=0,
    optimize=True,
)
print(f"{OUT}: {len(frames)} frames")
