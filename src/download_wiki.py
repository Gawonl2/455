import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import argparse
import requests
from tqdm import tqdm


HF_ROWS_URL = (
    "https://datasets-server.huggingface.co/rows"
    "?dataset=wikimedia/wikipedia"
    "&config={config}"
    "&split=train"
    "&offset={offset}"
    "&length={length}"
)
PAGE_SIZE = 100  # max per request


def main():
    parser = argparse.ArgumentParser(description="Download Wikipedia articles to JSONL via HF datasets-server")
    parser.add_argument("--output", default="data/full/raw_wiki.jsonl")
    parser.add_argument("--limit", type=int, default=2000,
                        help="Total articles to download (default 2000)")
    parser.add_argument("--dataset-config", default="20231101.en")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    print(f"Downloading wikimedia/wikipedia [{args.dataset_config}]")
    print(f"Target: {args.limit} articles → {args.output}")

    count = 0
    with open(args.output, "w", encoding="utf-8") as f:
        with tqdm(total=args.limit, desc="Articles") as pbar:
            offset = 0
            while count < args.limit:
                batch = min(PAGE_SIZE, args.limit - count)
                url = HF_ROWS_URL.format(
                    config=args.dataset_config,
                    offset=offset,
                    length=batch
                )
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                rows = data.get("rows", [])
                if not rows:
                    break
                for item in rows:
                    row = item["row"]
                    record = {
                        "id":    str(row["id"]),
                        "url":   row["url"],
                        "title": row["title"],
                        "text":  row["text"],
                    }
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    pbar.update(1)
                offset += len(rows)
                if len(rows) < batch:
                    break

    print(f"\nDone. Saved {count} articles to {args.output}")


if __name__ == "__main__":
    main()
