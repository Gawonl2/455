import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import re
from src.utils import compute_content_hash, now_utc


def clean_text(text: str) -> str:
    """
    - Strip whitespace
    - Normalize multiple whitespace to single space
    - Remove null bytes
    """
    if text is None:
        return ""
    # Remove null bytes
    text = text.replace("\x00", "")
    # Strip leading/trailing whitespace
    text = text.strip()
    # Normalize multiple whitespace to single space
    text = re.sub(r"\s+", " ", text)
    return text


def validate_and_clean_raw(conn, run_id: int) -> tuple:
    """
    SELECT id, url, title, text FROM raw_documents WHERE run_id=?
    Validate and clean each row.
    Returns (list of valid_doc dicts, rejected_count).
    """
    rows = conn.execute(
        "SELECT id, url, title, text FROM raw_documents WHERE run_id=?",
        [run_id]
    ).fetchall()

    valid_docs = []
    rejected_rows = []

    for row in rows:
        doc_id, url, title, text = row[0], row[1], row[2], row[3]

        raw_snapshot = json.dumps({
            "id": doc_id,
            "url": url,
            "title": title,
            "text": text[:500] if text else None
        })

        # Validation: missing id
        if not doc_id or doc_id.strip() == "":
            rejected_rows.append((
                run_id,
                doc_id or "",
                title or "",
                "MISSING_ID",
                "id is empty",
                raw_snapshot
            ))
            continue

        # Validation: empty text
        text_str = text if text is not None else ""
        if not text_str.strip():
            rejected_rows.append((
                run_id,
                doc_id,
                title or "",
                "EMPTY_TEXT",
                "text is empty or None after strip",
                raw_snapshot
            ))
            continue

        # Clean text
        cleaned = clean_text(text_str)
        if not cleaned:
            rejected_rows.append((
                run_id,
                doc_id,
                title or "",
                "EMPTY_TEXT",
                "text is empty after cleaning",
                raw_snapshot
            ))
            continue

        content_hash = compute_content_hash(cleaned)
        valid_docs.append({
            "doc_id": doc_id,
            "url": url or "",
            "title": title or "",
            "cleaned_text": cleaned,
            "content_hash": content_hash,
            "text_len": len(cleaned)
        })

    # Batch insert rejected documents
    if rejected_rows:
        conn.executemany(
            """
            INSERT INTO rejected_documents
                (run_id, doc_id, title, reason_code, reason_detail, raw_snapshot)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rejected_rows
        )

    rejected_count = len(rejected_rows)
    print(f"[CLEAN] Valid: {len(valid_docs)}, Rejected: {rejected_count}")
    return valid_docs, rejected_count
