import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import hashlib
import json
import uuid
from datetime import datetime, timezone

import duckdb

from src.utils import now_utc


def _compute_chunk_hash_checksum(conn, run_id: int) -> str:
    """
    Compute a checksum over all chunk_hashes active at end of run_id.
    A chunk was active at end of run X if:
      source_run_id <= X AND (valid_to_run_id IS NULL OR valid_to_run_id > X)
    """
    rows = conn.execute(
        """
        SELECT chunk_hash
        FROM fact_chunks
        WHERE source_run_id <= ?
          AND (valid_to_run_id IS NULL OR valid_to_run_id > ?)
        ORDER BY chunk_hash
        """,
        [run_id, run_id]
    ).fetchall()

    concatenated = "".join(r[0] for r in rows)
    return hashlib.sha256(concatenated.encode("utf-8")).hexdigest()


def run_audit(conn, run_id_a: int, run_id_b: int) -> list:
    """
    Run 5 audit checks comparing run_id_a to run_id_b.
    Writes results to audit_results table.
    Returns list of result dicts.
    """
    results = []
    created_at = now_utc()

    def write_audit(audit_type, result, details):
        audit_id = str(uuid.uuid4())
        det = json.dumps(details) if not isinstance(details, str) else details
        conn.execute(
            """
            INSERT INTO audit_results
                (audit_id, run_id_a, run_id_b, audit_type, result, details, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [audit_id, run_id_a, run_id_b, audit_type, result, det, created_at]
        )
        results.append({
            "audit_type": audit_type,
            "result": result,
            "details": details
        })
        print(f"[AUDIT] {audit_type}: {result} | {details}")

    # Fetch run rows
    run_a = conn.execute(
        "SELECT active_doc_count, active_chunk_count, active_embedding_count, index_vector_count FROM runs WHERE run_id=?",
        [run_id_a]
    ).fetchone()

    run_b = conn.execute(
        "SELECT active_doc_count, active_chunk_count, active_embedding_count, index_vector_count FROM runs WHERE run_id=?",
        [run_id_b]
    ).fetchone()

    if run_a is None or run_b is None:
        write_audit("run_existence", "FAIL",
                    {"error": f"run_a={'missing' if run_a is None else 'ok'}, run_b={'missing' if run_b is None else 'ok'}"})
        return results

    # Check 1: active_doc_count
    doc_a, doc_b = run_a[0], run_b[0]
    write_audit(
        "active_doc_count",
        "PASS" if doc_a == doc_b else "FAIL",
        {"run_a": doc_a, "run_b": doc_b}
    )

    # Check 2: active_chunk_count
    chunk_a, chunk_b = run_a[1], run_b[1]
    write_audit(
        "active_chunk_count",
        "PASS" if chunk_a == chunk_b else "FAIL",
        {"run_a": chunk_a, "run_b": chunk_b}
    )

    # Check 3: active_embedding_count
    emb_a, emb_b = run_a[2], run_b[2]
    write_audit(
        "active_embedding_count",
        "PASS" if emb_a == emb_b else "FAIL",
        {"run_a": emb_a, "run_b": emb_b}
    )

    # Check 4: index_vector_count
    idx_a, idx_b = run_a[3], run_b[3]
    write_audit(
        "index_vector_count",
        "PASS" if idx_a == idx_b else "FAIL",
        {"run_a": idx_a, "run_b": idx_b}
    )

    # Check 5: chunk_hash_checksum
    checksum_a = _compute_chunk_hash_checksum(conn, run_id_a)
    checksum_b = _compute_chunk_hash_checksum(conn, run_id_b)
    write_audit(
        "chunk_hash_checksum",
        "PASS" if checksum_a == checksum_b else "FAIL",
        {"checksum_a": checksum_a[:16] + "...", "checksum_b": checksum_b[:16] + "..."}
    )

    print(f"[AUDIT] Completed audit between run {run_id_a} and run {run_id_b}")
    return results


def main():
    parser = argparse.ArgumentParser(description="Run audit between two pipeline runs")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--run-id-a", type=int, required=True)
    parser.add_argument("--run-id-b", type=int, required=True)
    args = parser.parse_args()

    conn = duckdb.connect(args.db)
    results = run_audit(conn, args.run_id_a, args.run_id_b)
    conn.close()

    print("\n=== Audit Results ===")
    for r in results:
        print(f"  {r['audit_type']}: {r['result']}")


if __name__ == "__main__":
    main()
