import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import uuid
from datetime import datetime, timezone

from tqdm import tqdm
from src.config import INGEST_BATCH_SIZE
from src.utils import now_utc


def ingest_raw_documents(conn, input_path: str, run_id: int) -> int:
    """
    Read JSONL line by line, parse each JSON line,
    and insert into raw_documents in batches.
    Returns total count ingested.
    """
    ingest_batch_id = str(uuid.uuid4())
    total = 0
    batch = []

    insert_sql = """
        INSERT INTO raw_documents
            (run_id, ingest_batch_id, id, url, title, text, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    ingested_at = now_utc()

    print(f"[INGEST] Reading from {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in tqdm(lines, desc="[INGEST] Loading documents"):
        line = line.strip()
        if not line:
            continue
        try:
            doc = json.loads(line)
        except json.JSONDecodeError:
            continue

        doc_id = str(doc.get("id", ""))
        url = str(doc.get("url", ""))
        title = str(doc.get("title", ""))
        text = doc.get("text", None)
        if text is not None:
            text = str(text)

        batch.append((run_id, ingest_batch_id, doc_id, url, title, text, ingested_at))

        if len(batch) >= INGEST_BATCH_SIZE:
            conn.executemany(insert_sql, batch)
            total += len(batch)
            batch = []
            # New batch gets a new batch_id
            ingest_batch_id = str(uuid.uuid4())

    if batch:
        conn.executemany(insert_sql, batch)
        total += len(batch)

    print(f"[INGEST] Ingested {total} documents")
    return total
