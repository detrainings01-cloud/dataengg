"""
DE Course File Organizer & Compressor
======================================
Python 3.11+

Requirements:
    pip install tqdm

FFmpeg must be installed and added to PATH:
    Download: https://ffmpeg.org/download.html
    After installing, verify with:  ffmpeg -version

What this script does:
    1. Scans D:\DE 202512\Recordings  for numbered files (1_Intro.mp4, 2_DataTypes.mp4, etc.)
    2. Creates one output folder per class number, named after the .mp4 file
    3. Compresses each .mp4 into that folder (light compression, ~70% of original)
    4. Copies any sibling files (e.g. 3_ IF - ELSE Flow chart.png) into the same folder
    5. Scans D:\DE 202512\CodeFiles for "Class N" files/folders and copies them
       into the matching class output folder

Output structure example:
    D:\DE 202512\Output\
        1_Intro\
            1_Intro_compressed.mp4
            Class 1.ipynb
        2_DataTypes\
            2_DataTypes_compressed.mp4
        3_Conditional STMTs and Loops\
            3_Conditional STMTs and Loops_compressed.mp4
            3_ IF - ELSE Flow chart.png
            Class 3.ipynb
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

# -----------------------------------------------------------------
#  CONFIGURATION  <- adjust paths here if needed
# -----------------------------------------------------------------
RECORDINGS_DIR = Path(r"D:\DE 202512\Recordings")
CODEFILES_DIR  = Path(r"D:\DE 202512\CodeFiles")
OUTPUT_DIR     = Path(r"D:\DE 202512\Output")

# Stream-copy mode: video is NOT re-encoded (just copied as-is).
# This is near-instant, lossless, and always produces a smaller or equal file.
# FFmpeg only remuxes the container — no quality loss whatsoever.
STREAM_COPY = True

# Only used if STREAM_COPY = False  (re-encode fallback)
FFMPEG_CRF    = "28"
FFMPEG_PRESET = "medium"   # medium gives real compression unlike ultrafast

# -----------------------------------------------------------------
#  HELPERS
# -----------------------------------------------------------------

def extract_number(name: str) -> int | None:
    """Return the leading integer from a filename / dirname, or None."""
    m = re.match(r"^(\d+)", name.strip())
    return int(m.group(1)) if m else None


def check_ffmpeg() -> None:
    """Exit early with a helpful message if FFmpeg is not on PATH."""
    result = subprocess.run(
        ["ffmpeg", "-version"],
        capture_output=True
    )
    if result.returncode != 0:
        print("\n  X  FFmpeg not found on PATH.")
        print("     Download from https://ffmpeg.org/download.html")
        print("     Then add the bin\\ folder to your Windows PATH and retry.\n")
        sys.exit(1)


def build_recordings_map(recordings_dir: Path) -> dict[int, list[Path]]:
    """
    Group every file in recordings_dir by its leading number.
    Returns { class_number: [Path, ...] }
    """
    mapping: dict[int, list[Path]] = {}
    for item in sorted(recordings_dir.iterdir()):
        if item.is_file():
            num = extract_number(item.name)
            if num is not None:
                mapping.setdefault(num, []).append(item)
    return mapping


def get_primary_mp4(files: list[Path]) -> Path | None:
    """Return the .mp4 file from a list, or None."""
    for f in files:
        if f.suffix.lower() == ".mp4":
            return f
    return None


def folder_name_for_class(files: list[Path]) -> str:
    """
    Derive output folder name from the .mp4 stem,
    falling back to the first file's stem.
    """
    mp4 = get_primary_mp4(files)
    return (mp4 if mp4 else files[0]).stem   # e.g. "3_Conditional STMTs and Loops"


MAX_RETRIES = 3   # number of attempts before giving up on a file

def build_ffmpeg_cmd(src: Path, tmp_dst: Path) -> list[str]:
    """
    Build the FFmpeg command based on STREAM_COPY setting.

    STREAM_COPY = True  (default, recommended):
        Video stream is copied byte-for-byte — no re-encoding.
        Result is always <= original size, near-instant, zero quality loss.

    STREAM_COPY = False:
        Full re-encode with H.264. Use only if you need to shrink a file
        that was recorded in a high-bitrate / lossless codec.
        Set FFMPEG_PRESET = "medium" or "slow" for real size savings.
    """
    if STREAM_COPY:
        return [
            "ffmpeg", "-y",
            "-i",      str(src),
            "-c:v",    "copy",     # copy video stream as-is (no re-encode)
            "-c:a",    "aac",      # re-encode audio to AAC (safe, small)
            "-b:a",    "128k",
            str(tmp_dst),
        ]
    else:
        return [
            "ffmpeg", "-y",
            "-i",      str(src),
            "-vcodec", "libx264",
            "-crf",    FFMPEG_CRF,
            "-preset", FFMPEG_PRESET,
            "-acodec", "aac",
            "-b:a",    "128k",
            str(tmp_dst),
        ]


def compress_video(src: Path, dst: Path) -> bool:
    """
    Process src -> dst with retry mechanism.

    - Writes to a .tmp.mp4 first; renames to final name only on success.
    - Leftover .tmp.mp4 files (from a previous crash) are cleaned up automatically.
    - A completed dst file is NEVER re-processed.
    - Retries up to MAX_RETRIES times on failure.
    - Ctrl+C cleans up the temp file before exiting.
    """
    # Already done — skip immediately
    if dst.exists() and dst.stat().st_size > 0:
        return True

    tmp_dst = dst.with_suffix(".tmp.mp4")

    for attempt in range(1, MAX_RETRIES + 1):

        # Clean up leftover temp file from a previous crashed attempt
        if tmp_dst.exists():
            tmp_dst.unlink()
            print(f"     Cleaned up leftover temp file from a previous run.")

        if attempt > 1:
            print(f"     Retrying... (attempt {attempt}/{MAX_RETRIES})")

        cmd = build_ffmpeg_cmd(src, tmp_dst)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)

            if (result.returncode == 0
                    and tmp_dst.exists()
                    and tmp_dst.stat().st_size > 0):
                # Success — promote temp file to final name
                tmp_dst.rename(dst)
                return True

            print(f"     X Attempt {attempt}/{MAX_RETRIES} failed.")
            if result.stderr:
                print(f"       FFmpeg: {result.stderr[-300:]}")

        except KeyboardInterrupt:
            if tmp_dst.exists():
                tmp_dst.unlink()
            print("\n  Interrupted. Temp file cleaned up. Exiting.")
            sys.exit(0)

        except Exception as e:
            print(f"     X Attempt {attempt}/{MAX_RETRIES} raised an error: {e}")

        finally:
            if tmp_dst.exists():
                tmp_dst.unlink()

    print(f"     X All {MAX_RETRIES} attempts failed for: {src.name}  -- skipping.")
    return False


def copy_codefiles(class_num: int, target_dir: Path) -> None:
    """
    Copy every item in CODEFILES_DIR whose leading number matches class_num
    into target_dir.  Works for both files and folders.
    """
    if not CODEFILES_DIR.exists():
        return

    # Matches: "Class 3", "Class 3.ipynb", "Class 3 - Notes", "class3_data" etc.
    pattern = re.compile(
        rf"^[Cc]lass\s*{class_num}(\s|_|\.|$|-)",
        re.IGNORECASE
    )

    found_any = False
    for item in sorted(CODEFILES_DIR.iterdir()):
        if pattern.match(item.name):
            dest = target_dir / item.name
            if item.is_dir():
                if dest.exists():
                    shutil.rmtree(dest)
                shutil.copytree(item, dest)
                print(f"     [Folder] Code folder  : {item.name}")
            else:
                shutil.copy2(item, dest)
                print(f"     [File]   Code file    : {item.name}")
            found_any = True

    if not found_any:
        print(f"     -- No CodeFiles matched for Class {class_num}")


# -----------------------------------------------------------------
#  MAIN PIPELINE
# -----------------------------------------------------------------

def run() -> None:
    print("\n" + "=" * 62)
    print("  DE Course Organizer  |  Compress + Arrange (local)")
    print("=" * 62)

    # Pre-flight checks
    check_ffmpeg()

    if not RECORDINGS_DIR.exists():
        print(f"\n  X  RECORDINGS_DIR not found:\n     {RECORDINGS_DIR}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"\n  Source recordings : {RECORDINGS_DIR}")
    print(f"  Source code files : {CODEFILES_DIR}")
    print(f"  Output directory  : {OUTPUT_DIR}\n")

    # Build class map
    recordings_map = build_recordings_map(RECORDINGS_DIR)
    if not recordings_map:
        print("  WARNING: No numbered files found in Recordings folder. Exiting.")
        sys.exit(1)

    total = len(recordings_map)
    print(f"  Found {total} class(es) to process.\n")

    # Process each class
    for idx, class_num in enumerate(sorted(recordings_map), start=1):
        files       = recordings_map[class_num]
        folder_name = folder_name_for_class(files)
        class_dir   = OUTPUT_DIR / folder_name
        class_dir.mkdir(parents=True, exist_ok=True)

        print(f"  [{idx}/{total}]  Class {class_num}  -->  {folder_name}")

        mp4 = get_primary_mp4(files)

        for f in files:
            if f == mp4:
                out_name = f"{f.stem}_compressed{f.suffix}"
                out_path = class_dir / out_name

                if out_path.exists() and out_path.stat().st_size > 0:
                    print(f"     OK Already compressed (skip): {out_name}")
                else:
                    print(f"     Compressing : {f.name}")
                    ok = compress_video(f, out_path)
                    if ok:
                        orig_mb = f.stat().st_size / 1_048_576
                        comp_mb = out_path.stat().st_size / 1_048_576
                        saved   = (1 - comp_mb / orig_mb) * 100 if orig_mb else 0
                        print(f"        {orig_mb:.1f} MB  -->  {comp_mb:.1f} MB  "
                              f"({saved:.0f}% smaller)")
            else:
                # Sibling asset (PNG, PDF, etc.) — copy as-is
                dest = class_dir / f.name
                shutil.copy2(f, dest)
                print(f"     Asset copied : {f.name}")

        # Copy matching code files / folders
        copy_codefiles(class_num, class_dir)
        print()

    print("=" * 62)
    print(f"  DONE!  Organised files are in:\n     {OUTPUT_DIR}")
    print("=" * 62 + "\n")


if __name__ == "__main__":
    run()
    