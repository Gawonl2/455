import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import CHUNK_TOKENS, STRIDE, MIN_CHUNK_LEN
from src.utils import compute_chunk_id, compute_chunk_hash


def chunk_document(doc_id: str, content_hash: str, title: str, cleaned_text: str,
                   tokenizer, run_id: int) -> list:
    """
    Tokenize the document and produce sliding-window chunks.

    Input text: f"{title}: {cleaned_text}"
    Chunk size: 384 tokens, stride: 320 tokens.
    Returns list of chunk dicts.
    """
    full_text = f"{title}: {cleaned_text}"

    # Tokenize without special tokens for sliding window
    token_ids = tokenizer.encode(full_text, add_special_tokens=False)

    chunks = []
    chunk_index = 0
    start = 0

    while start < len(token_ids):
        end = min(start + CHUNK_TOKENS, len(token_ids))
        window_ids = token_ids[start:end]

        chunk_token_len = end - start

        if chunk_token_len < MIN_CHUNK_LEN:
            break

        # Decode token ids back to text
        chunk_text = tokenizer.decode(window_ids, skip_special_tokens=True)

        chunk_id = compute_chunk_id(doc_id, content_hash, start, end)
        chunk_hash = compute_chunk_hash(chunk_text)

        chunks.append({
            "chunk_id": chunk_id,
            "doc_id": doc_id,
            "content_hash": content_hash,
            "chunk_index": chunk_index,
            "start_tok": start,
            "end_tok": end,
            "chunk_text": chunk_text,
            "chunk_token_len": chunk_token_len,
            "chunk_hash": chunk_hash,
            "source_run_id": run_id,
            "is_active": True,
            "valid_from_run_id": run_id,
            "valid_to_run_id": None
        })

        chunk_index += 1

        if end == len(token_ids):
            break
        start += STRIDE

    return chunks


def chunk_new_documents(conn, run_id: int, doc_ids: list, tokenizer) -> int:
    """
    For each doc_id in doc_ids, load from clean_documents and generate chunks.
    Batch insert into fact_chunks.
    Returns total chunk count.
    """
    if not doc_ids:
        return 0

    insert_sql = """
        INSERT INTO fact_chunks
            (chunk_id, doc_id, content_hash, chunk_index, start_tok, end_tok,
             chunk_text, chunk_token_len, chunk_hash, source_run_id, is_active,
             valid_from_run_id, valid_to_run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    total_chunks = 0
    batch = []
    BATCH_SIZE = 1000

    for doc_id in doc_ids:
        row = conn.execute(
            """
            SELECT doc_id, content_hash, title, cleaned_text
            FROM clean_documents
            WHERE doc_id=? AND is_active=TRUE
            LIMIT 1
            """,
            [doc_id]
        ).fetchone()

        if row is None:
            continue

        d_id, content_hash, title, cleaned_text = row

        chunks = chunk_document(d_id, content_hash, title, cleaned_text, tokenizer, run_id)

        for c in chunks:
            batch.append((
                c["chunk_id"],
                c["doc_id"],
                c["content_hash"],
                c["chunk_index"],
                c["start_tok"],
                c["end_tok"],
                c["chunk_text"],
                c["chunk_token_len"],
                c["chunk_hash"],
                c["source_run_id"],
                c["is_active"],
                c["valid_from_run_id"],
                c["valid_to_run_id"]
            ))

            if len(batch) >= BATCH_SIZE:
                conn.executemany(insert_sql, batch)
                total_chunks += len(batch)
                batch = []

    if batch:
        conn.executemany(insert_sql, batch)
        total_chunks += len(batch)

    print(f"[CHUNK] Generated {total_chunks} chunks from {len(doc_ids)} documents")
    return total_chunks
