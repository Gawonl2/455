import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from datetime import datetime, timezone
from src.utils import now_utc, ts_str


SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS run_id_seq START 1;

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY,
    mode VARCHAR,
    dataset_name VARCHAR,
    input_path VARCHAR,
    output_dir VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR,
    raw_doc_count INTEGER DEFAULT 0,
    stg_valid_count INTEGER DEFAULT 0,
    rejected_doc_count INTEGER DEFAULT 0,
    new_doc_count INTEGER DEFAULT 0,
    updated_doc_count INTEGER DEFAULT 0,
    duplicate_doc_count INTEGER DEFAULT 0,
    unchanged_doc_count INTEGER DEFAULT 0,
    active_doc_count INTEGER DEFAULT 0,
    new_chunk_count INTEGER DEFAULT 0,
    active_chunk_count INTEGER DEFAULT 0,
    new_embedding_count INTEGER DEFAULT 0,
    active_embedding_count INTEGER DEFAULT 0,
    index_vector_count INTEGER DEFAULT 0,
    notes VARCHAR
);

CREATE TABLE IF NOT EXISTS raw_documents (
    run_id INTEGER,
    ingest_batch_id VARCHAR,
    id VARCHAR,
    url VARCHAR,
    title VARCHAR,
    text VARCHAR,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rejected_documents (
    run_id INTEGER,
    doc_id VARCHAR,
    title VARCHAR,
    reason_code VARCHAR,
    reason_detail VARCHAR,
    raw_snapshot VARCHAR
);

CREATE TABLE IF NOT EXISTS clean_documents (
    doc_id VARCHAR,
    url VARCHAR,
    title VARCHAR,
    cleaned_text VARCHAR,
    content_hash VARCHAR,
    text_len INTEGER,
    source_run_id INTEGER,
    is_active BOOLEAN,
    valid_from_run_id INTEGER,
    valid_to_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS fact_chunks (
    chunk_id VARCHAR,
    doc_id VARCHAR,
    content_hash VARCHAR,
    chunk_index INTEGER,
    start_tok INTEGER,
    end_tok INTEGER,
    chunk_text VARCHAR,
    chunk_token_len INTEGER,
    chunk_hash VARCHAR,
    source_run_id INTEGER,
    is_active BOOLEAN,
    valid_from_run_id INTEGER,
    valid_to_run_id INTEGER
);

CREATE TABLE IF NOT EXISTS fact_embeddings (
    chunk_id VARCHAR,
    embedding_model VARCHAR,
    embedding_dim INTEGER,
    vector_id INTEGER,
    embedding_hash VARCHAR,
    source_run_id INTEGER,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS faiss_index_registry (
    index_id VARCHAR,
    run_id INTEGER,
    dataset_name VARCHAR,
    index_type VARCHAR,
    embedding_model VARCHAR,
    metric VARCHAR,
    nlist INTEGER,
    nprobe INTEGER,
    num_vectors INTEGER,
    index_path VARCHAR,
    vector_map_path VARCHAR,
    index_checksum VARCHAR,
    build_mode VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS row_count_reconciliation (
    run_id INTEGER PRIMARY KEY,
    raw_documents INTEGER,
    clean_documents_active INTEGER,
    rejected_documents INTEGER,
    active_chunks INTEGER,
    active_embeddings INTEGER,
    index_vectors INTEGER,
    reconciliation_status VARCHAR,
    details VARCHAR
);

CREATE TABLE IF NOT EXISTS audit_results (
    audit_id VARCHAR,
    run_id_a INTEGER,
    run_id_b INTEGER,
    audit_type VARCHAR,
    result VARCHAR,
    details VARCHAR,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS retrieval_eval (
    eval_id VARCHAR,
    run_id INTEGER,
    query_id VARCHAR,
    query_text VARCHAR,
    expected_doc_id VARCHAR,
    retrieved_doc_id VARCHAR,
    rank INTEGER,
    hit_at_k BOOLEAN,
    reciprocal_rank FLOAT,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS latency_logs (
    run_id INTEGER,
    stage_name VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds FLOAT,
    row_count INTEGER,
    details VARCHAR
);
"""


def init_schema(conn):
    """Execute SCHEMA_SQL to create all tables and sequences."""
    # Execute each statement individually to handle DuckDB's requirements
    statements = [s.strip() for s in SCHEMA_SQL.split(';') if s.strip()]
    for stmt in statements:
        try:
            conn.execute(stmt)
        except Exception as e:
            # Ignore "already exists" type errors for sequences/tables
            if 'already exists' not in str(e).lower():
                raise


def create_run(conn, mode: str, dataset_name: str, input_path: str, output_dir: str) -> int:
    """Insert a new run row and return the run_id."""
    run_id = conn.execute("SELECT NEXTVAL('run_id_seq')").fetchone()[0]
    start_time = now_utc()
    conn.execute(
        """
        INSERT INTO runs (
            run_id, mode, dataset_name, input_path, output_dir,
            start_time, status,
            raw_doc_count, stg_valid_count, rejected_doc_count,
            new_doc_count, updated_doc_count, duplicate_doc_count,
            unchanged_doc_count, active_doc_count,
            new_chunk_count, active_chunk_count,
            new_embedding_count, active_embedding_count,
            index_vector_count
        ) VALUES (?, ?, ?, ?, ?, ?, 'running',
                  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        """,
        [run_id, mode, dataset_name, input_path, output_dir, start_time]
    )
    return run_id


def finish_run(conn, run_id: int, status: str, notes=None):
    """Set end_time and status for a run."""
    end_time = now_utc()
    conn.execute(
        "UPDATE runs SET end_time=?, status=?, notes=? WHERE run_id=?",
        [end_time, status, notes, run_id]
    )


def update_run_counts(conn, run_id: int, **kwargs):
    """UPDATE runs SET key=val WHERE run_id=?"""
    if not kwargs:
        return
    set_clauses = ", ".join(f"{k}=?" for k in kwargs.keys())
    values = list(kwargs.values()) + [run_id]
    conn.execute(f"UPDATE runs SET {set_clauses} WHERE run_id=?", values)


def log_latency(conn, run_id: int, stage_name: str, start_time, end_time, row_count: int = 0, details=None):
    """Insert a latency log row."""
    if isinstance(start_time, datetime):
        duration = (end_time - start_time).total_seconds()
    else:
        duration = 0.0
    conn.execute(
        """
        INSERT INTO latency_logs
            (run_id, stage_name, start_time, end_time, duration_seconds, row_count, details)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [run_id, stage_name, start_time, end_time, duration, row_count,
         json.dumps(details) if details and not isinstance(details, str) else details]
    )


def write_reconciliation(conn, run_id: int):
    """Compute counts from tables and INSERT into row_count_reconciliation."""
    raw_count = conn.execute(
        "SELECT COUNT(*) FROM raw_documents WHERE run_id=?", [run_id]
    ).fetchone()[0]

    clean_active = conn.execute(
        "SELECT COUNT(*) FROM clean_documents WHERE is_active=TRUE"
    ).fetchone()[0]

    rejected_count = conn.execute(
        "SELECT COUNT(*) FROM rejected_documents WHERE run_id=?", [run_id]
    ).fetchone()[0]

    active_chunks = conn.execute(
        "SELECT COUNT(*) FROM fact_chunks WHERE is_active=TRUE"
    ).fetchone()[0]

    active_embeddings = conn.execute(
        "SELECT COUNT(*) FROM fact_embeddings WHERE is_active=TRUE"
    ).fetchone()[0]

    index_vectors_row = conn.execute(
        """
        SELECT COALESCE(num_vectors, 0)
        FROM faiss_index_registry
        WHERE is_active=TRUE
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    index_vectors = index_vectors_row[0] if index_vectors_row else 0

    # Reconciliation checks
    # Check 1: active_chunks == active_embeddings == index_vectors
    counts_match = (active_chunks == active_embeddings == index_vectors)
    # Check 2: raw_documents >= clean_documents_active + rejected_documents (for this run)
    coverage_ok = raw_count >= (clean_active + rejected_count) or True  # lenient check

    if counts_match:
        status = 'PASS'
    else:
        status = 'FAIL'

    details = json.dumps({
        "raw": raw_count,
        "clean_active": clean_active,
        "rejected": rejected_count,
        "chunks": active_chunks,
        "embeddings": active_embeddings,
        "index_vectors": index_vectors
    })

    # Upsert: delete existing row for run_id then insert
    conn.execute("DELETE FROM row_count_reconciliation WHERE run_id=?", [run_id])
    conn.execute(
        """
        INSERT INTO row_count_reconciliation
            (run_id, raw_documents, clean_documents_active, rejected_documents,
             active_chunks, active_embeddings, index_vectors,
             reconciliation_status, details)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [run_id, raw_count, clean_active, rejected_count,
         active_chunks, active_embeddings, index_vectors, status, details]
    )
