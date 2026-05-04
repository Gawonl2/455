"""
FP deliverable: generate the initial and update JSONL batches used by the pipeline.

Delegates to ``src/make_live_demo_data.py`` (same logic as README "Live Demo Instructions").
Run from project root; all paths are relative.
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate live_demo initial_sample.jsonl and update_sample.jsonl from a full corpus."
    )
    parser.add_argument(
        "--full-input",
        default="data/full/raw_wiki.jsonl",
        help="Full Wikipedia JSONL (see src/download_wiki.py)",
    )
    parser.add_argument(
        "--live-dir",
        default="data/live_demo",
        help="Output directory for both batches",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=40,
        help="Documents in the initial sample",
    )
    args = parser.parse_args()
    script = ROOT / "src" / "make_live_demo_data.py"
    proc = subprocess.run(
        [
            sys.executable,
            str(script),
            "--full-input",
            str(args.full_input),
            "--live-dir",
            str(args.live_dir),
            "--sample-size",
            str(args.sample_size),
        ],
        cwd=str(ROOT),
    )
    raise SystemExit(proc.returncode)


if __name__ == "__main__":
    main()
