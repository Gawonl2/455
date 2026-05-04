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
    Embed all active chunks that do not yet have an active embedding.
    Save vectors to {output_dir}/embeddings/run_{run_id}.npy
    Insert into fact_embeddings.
    Returns count of embeddings created.
    """
    rows = conn.execute(
        """
        SELECT fc.chunk_id, fc.chunk_text
        FROM fact_chunks fc
        LEFT JOIN fact_embeddings fe
          ON fe.chunk_id = fc.chunk_id AND fe.is_active=TRUE
        WHERE fc.is_active=TRUE
          AND fe.chunk_id IS NULL
        ORDER BY fc.chunk_id
        """
    ).fetchall()

    if not rows:
        print(f"[EMBED] No missing active-chunk embeddings for run {run_id}")
        return 0

    chunk_ids = [r[0] for r in rows]
    chunk_texts = [r[1] for r in rows]

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
