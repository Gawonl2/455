import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from src.config import (
    EMBEDDING_MODEL, EMBEDDING_DIM, PASSAGE_PREFIX, EMBED_BATCH_SIZE
)
from src.utils import compute_embedding_hash


def embed_new_chunks(conn, run_id: int, output_dir: str, model) -> int:
    """
    Embed all chunks from the current run (source_run_id=run_id).
    Save vectors to {output_dir}/embeddings/run_{run_id}.npy
    Insert into fact_embeddings.
    Returns count of embeddings created.
    """
    rows = conn.execute(
        """
        SELECT chunk_id, chunk_text
        FROM fact_chunks
        WHERE source_run_id=? AND is_active=TRUE
        """,
        [run_id]
    ).fetchall()

    if not rows:
        print(f"[EMBED] No new chunks for run {run_id}")
        return 0

    # Check for already-embedded chunks to avoid double-inserting
    existing_embeddings = set()
    existing_rows = conn.execute(
        """
        SELECT chunk_id FROM fact_embeddings
        WHERE source_run_id=?
        """,
        [run_id]
    ).fetchall()
    for r in existing_rows:
        existing_embeddings.add(r[0])

    # Filter to only chunks not yet embedded
    rows_to_embed = [(cid, ct) for cid, ct in rows if cid not in existing_embeddings]

    if not rows_to_embed:
        print(f"[EMBED] All chunks for run {run_id} already embedded")
        return 0

    chunk_ids = [r[0] for r in rows_to_embed]
    chunk_texts = [r[1] for r in rows_to_embed]

    # Prepend passage prefix for encoding
    texts_with_prefix = [f"{PASSAGE_PREFIX}{ct}" for ct in chunk_texts]

    print(f"[EMBED] Embedding {len(texts_with_prefix)} chunks...")

    vectors = model.encode(
        texts_with_prefix,
        batch_size=EMBED_BATCH_SIZE,
        normalize_embeddings=True,
        show_progress_bar=True
    )

    vectors = np.array(vectors, dtype=np.float32)

    # Save embeddings file
    embeddings_dir = os.path.join(output_dir, "embeddings")
    os.makedirs(embeddings_dir, exist_ok=True)
    emb_path = os.path.join(embeddings_dir, f"run_{run_id}.npy")
    np.save(emb_path, vectors)
    print(f"[EMBED] Saved embeddings to {emb_path}")

    # Insert into fact_embeddings
    insert_sql = """
        INSERT INTO fact_embeddings
            (chunk_id, embedding_model, embedding_dim, vector_id,
             embedding_hash, source_run_id, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    batch = []
    for local_idx, (chunk_id, vector) in enumerate(zip(chunk_ids, vectors)):
        emb_hash = compute_embedding_hash(vector)
        batch.append((
            chunk_id,
            EMBEDDING_MODEL,
            EMBEDDING_DIM,
            local_idx,        # vector_id = local index in this run's npy file
            emb_hash,
            run_id,
            True
        ))

    conn.executemany(insert_sql, batch)
    print(f"[EMBED] Inserted {len(batch)} embedding records")
    return len(batch)
