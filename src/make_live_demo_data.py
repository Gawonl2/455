import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import random

from src.config import RANDOM_SEED


def main():
    parser = argparse.ArgumentParser(
        description="Create live demo data subset from full Wikipedia JSONL"
    )
    parser.add_argument(
        "--full-input",
        default="data/full/raw_wiki.jsonl",
        help="Path to full Wikipedia JSONL input"
    )
    parser.add_argument(
        "--live-dir",
        default="data/live_demo",
        help="Output directory for live demo data"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=40,
        help="Number of documents in initial sample"
    )
    args = parser.parse_args()

    # Check if input file exists
    if not os.path.exists(args.full_input):
        print(f"[make_live_demo_data] Input file not found: {args.full_input}")
        print("[make_live_demo_data] Please provide the full Wikipedia JSONL file first.")
        print("[make_live_demo_data] You can download it using the HuggingFace datasets library:")
        print("  from datasets import load_dataset")
        print("  ds = load_dataset('wikipedia', '20220301.en', split='train')")
        print("  # then write to data/full/raw_wiki.jsonl")
        sys.exit(0)

    os.makedirs(args.live_dir, exist_ok=True)

    # Read first 2000 lines (or all if fewer)
    print(f"[make_live_demo_data] Reading from {args.full_input} (up to 2000 lines)...")
    raw_docs = []
    with open(args.full_input, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            if i >= 2000:
                break
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
                raw_docs.append(doc)
            except json.JSONDecodeError:
                continue

    # Filter to valid docs
    valid_docs = []
    for doc in raw_docs:
        doc_id = str(doc.get("id", "")).strip()
        text = doc.get("text", None)
        if doc_id and text and str(text).strip():
            valid_docs.append(doc)

    print(f"[make_live_demo_data] Found {len(valid_docs)} valid docs")

    # Adjust sample_size if needed (need at least sample_size + 1 extra for update set)
    min_needed = args.sample_size + 5
    if len(valid_docs) < min_needed:
        new_sample_size = max(2, len(valid_docs) - 5)
        print(
            f"[make_live_demo_data] Only {len(valid_docs)} valid docs available. "
            f"Adjusting sample_size from {args.sample_size} to {new_sample_size}"
        )
        args.sample_size = new_sample_size

    # Random sample
    random.seed(RANDOM_SEED)
    sample = random.sample(valid_docs, args.sample_size)
    sample_ids = {str(d.get("id", "")) for d in sample}

    # Remaining docs (not in sample)
    remaining = [d for d in valid_docs if str(d.get("id", "")) not in sample_ids]

    # Write initial_sample.jsonl
    initial_path = os.path.join(args.live_dir, "initial_sample.jsonl")
    with open(initial_path, "w", encoding="utf-8") as f:
        for doc in sample:
            f.write(json.dumps(doc) + "\n")
    print(f"[make_live_demo_data] Wrote {len(sample)} docs to {initial_path}")

    # Build update_sample.jsonl
    update_docs = []

    # 1. NEW doc: first from remaining
    if remaining:
        new_doc = remaining[0]
        update_docs.append(new_doc)
    else:
        # Fallback: create a synthetic new doc
        new_doc = {
            "id": "synthetic_new_doc_001",
            "url": "https://en.wikipedia.org/wiki/Vector_database",
            "title": "Vector Database",
            "text": "A vector database is a type of database that stores data as high-dimensional vectors. "
                    "These vectors are mathematical representations of features or attributes. "
                    "Vector databases are used in machine learning applications for similarity search."
        }
        update_docs.append(new_doc)

    # 2. REVISED doc: sample[0] with updated text
    revised_doc = dict(sample[0])
    revised_doc["text"] = (
        str(revised_doc.get("text", "")) +
        "\n\nThis updated version includes new information about vector databases and retrieval systems."
    )
    update_docs.append(revised_doc)

    # 3. DUPLICATE doc: exact copy of sample[1]
    if len(sample) > 1:
        dup_doc = dict(sample[1])
        update_docs.append(dup_doc)
    else:
        dup_doc = dict(sample[0])
        update_docs.append(dup_doc)

    # 4. CORRUPTED doc: empty text/url
    corrupted_doc = {
        "id": "corrupted_doc_001",
        "title": "Corrupted Test Document",
        "url": "",
        "text": ""
    }
    update_docs.append(corrupted_doc)

    # Write update_sample.jsonl
    update_path = os.path.join(args.live_dir, "update_sample.jsonl")
    with open(update_path, "w", encoding="utf-8") as f:
        for doc in update_docs:
            f.write(json.dumps(doc) + "\n")
    print(f"[make_live_demo_data] Wrote {len(update_docs)} docs to {update_path}")

    # Print summary
    print("\n=== Live Demo Data Created ===")
    print(f"  Initial sample ({len(sample)} docs): {initial_path}")
    print(f"  Update sample ({len(update_docs)} docs): {update_path}")
    print("\n  Update sample contents:")
    print(f"    - 1 NEW doc: id={new_doc.get('id','?')}, title={new_doc.get('title','?')}")
    print(f"    - 1 REVISED doc: id={sample[0].get('id','?')}, title={sample[0].get('title','?')}")
    print(f"    - 1 DUPLICATE doc: id={sample[1].get('id','?') if len(sample)>1 else sample[0].get('id','?')}")
    print(f"    - 1 CORRUPTED doc: id=corrupted_doc_001")

    print("\n=== Demo Queries ===")
    revised_title = sample[0].get("title", "the revised document")
    new_title = new_doc.get("title", "the new document")
    print(f"  1. \"What is {revised_title}?\"")
    print(f"  2. \"What document discusses vector databases and retrieval systems?\"")
    print(f"  3. \"Tell me about {new_title}.\"")


if __name__ == "__main__":
    main()
