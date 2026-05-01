import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import hashlib
import uuid
from datetime import datetime, timezone

import numpy as np
import faiss

from src.config import (
    EMBEDDING_DIM, IVF_NLIST, IVF_NPROBE, RANDOM_SEED, EMBEDDING_MODEL
)
from src.utils import now_utc


def build_index(conn, run_id: int, output_dir: str, dataset_name: str, build_mode: str) -> dict:
    """
    Build a FAISS IVF index from all active embeddings.

    Steps:
    1. Query all active embeddings
    2. Load vectors from per-run npy files
    3. Build IVF index
    4. Save index and vector_map
    5. Register in faiss_index_registry

    Returns dict with index_path, vector_map_path, num_vectors, actual_nlist.
    """
    # Query all active embeddings ordered by chunk_id for deterministic order
    rows = conn.execute(
        """
        SELECT chunk_id, source_run_id, vector_id
        FROM fact_embeddings
        WHERE is_active=TRUE
        ORDER BY chunk_id
        """
    ).fetchall()

    if not rows:
        raise ValueError("No active embeddings found. Cannot build FAISS index.")

    num_vectors = len(rows)
    print(f"[INDEX] Building IVF index with {num_vectors} vectors...")

    # Group by source_run_id for efficient loading
    run_to_rows = {}
    for chunk_id, source_run_id, vector_id in rows:
        if source_run_id not in run_to_rows:
            run_to_rows[source_run_id] = []
        run_to_rows[source_run_id].append((chunk_id, vector_id))

    # Build ordered list of (chunk_id, vector) matching sorted chunk_id order
    # We maintain the sorted order from the query
    chunk_id_order = [r[0] for r in rows]

    # Map chunk_id -> (source_run_id, vector_id)
    chunk_meta = {r[0]: (r[1], r[2]) for r in rows}

    # Load all vectors from npy files
    # First load each run's npy file once
    run_npy_cache = {}
    for src_run_id in run_to_rows.keys():
        npy_path = os.path.join(output_dir, "embeddings", f"run_{src_run_id}.npy")
        if not os.path.exists(npy_path):
            raise FileNotFoundError(f"Embedding file not found: {npy_path}")
        run_npy_cache[src_run_id] = np.load(npy_path)

    # Build final matrix in sorted chunk_id order
    vectors_list = []
    for chunk_id in chunk_id_order:
        src_run_id, vec_id = chunk_meta[chunk_id]
        vec = run_npy_cache[src_run_id][vec_id]
        vectors_list.append(vec)

    vectors = np.array(vectors_list, dtype=np.float32)

    # IVF adaptive nlist
    actual_nlist = min(IVF_NLIST, max(4, num_vectors // 10))
    print(f"[INDEX] actual_nlist={actual_nlist}, num_vectors={num_vectors}")

    # Set random seed for reproducibility
    np.random.seed(RANDOM_SEED)

    # Build FAISS IVF index
    quantizer = faiss.IndexFlatL2(EMBEDDING_DIM)
    index = faiss.IndexIVFFlat(quantizer, EMBEDDING_DIM, actual_nlist, faiss.METRIC_INNER_PRODUCT)

    # Train
    print(f"[INDEX] Training IVF index...")
    index.train(vectors)

    # Add vectors
    index.add(vectors)
    index.nprobe = IVF_NPROBE

    # Prepare output directories
    faiss_dir = os.path.join(output_dir, "faiss")
    os.makedirs(faiss_dir, exist_ok=True)

    index_path = os.path.join(faiss_dir, "index.faiss")
    vector_map_path = os.path.join(faiss_dir, "vector_map.npy")

    # Save index
    faiss.write_index(index, index_path)
    print(f"[INDEX] Saved FAISS index to {index_path}")

    # Save vector_map: numpy array of chunk_ids in FAISS ID order
    vector_map = np.array(chunk_id_order, dtype=object)
    np.save(vector_map_path, vector_map)
    print(f"[INDEX] Saved vector map to {vector_map_path}")

    # Compute index checksum
    with open(index_path, "rb") as f:
        index_checksum = hashlib.sha256(f.read()).hexdigest()

    # Deactivate old registry entries for this dataset
    conn.execute(
        """
        UPDATE faiss_index_registry
        SET is_active=FALSE
        WHERE dataset_name=? AND is_active=TRUE
        """,
        [dataset_name]
    )

    # Insert new registry entry
    index_id = str(uuid.uuid4())
    created_at = now_utc()
    conn.execute(
        """
        INSERT INTO faiss_index_registry
            (index_id, run_id, dataset_name, index_type, embedding_model,
             metric, nlist, nprobe, num_vectors, index_path, vector_map_path,
             index_checksum, build_mode, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            index_id, run_id, dataset_name, "IVFFlat", EMBEDDING_MODEL,
            "INNER_PRODUCT", actual_nlist, IVF_NPROBE, num_vectors,
            index_path, vector_map_path,
            index_checksum, build_mode, True, created_at
        ]
    )

    print(f"[INDEX] Registered index {index_id} in faiss_index_registry")

    return {
        "index_path": index_path,
        "vector_map_path": vector_map_path,
        "num_vectors": num_vectors,
        "actual_nlist": actual_nlist
    }
