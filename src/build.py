import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse

from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer
import duckdb

from src import monitoring, ingest, clean, curate, chunk, embed, index_ivf, audit
from src.config import EMBEDDING_MODEL, get_embedding_device
from src.utils import now_utc


def run_pipeline(mode: str, dataset_name: str, input_path: str,
                 output_dir: str, db_path: str, index_type: str = "ivf"):
    """
    Full ETL pipeline orchestrator.

    mode: 'full' or 'augmented'
    dataset_name: 'full' or 'live_demo'
    input_path: path to JSONL input file
    output_dir: where to write embeddings/index
    db_path: path to DuckDB database
    index_type: 'ivf' (currently only supported)
    """
    # Create output directories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "embeddings"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "faiss"), exist_ok=True)

    # Connect to DuckDB
    conn = duckdb.connect(db_path)

    # Initialize schema
    monitoring.init_schema(conn)

    # Create run record
    run_id = monitoring.create_run(conn, mode, dataset_name, input_path, output_dir)

    print(f"\n=== Run {run_id}: {mode.upper()} build [{dataset_name}] ===")
    print(f"  Input:    {input_path}")
    print(f"  Output:   {output_dir}")
    print(f"  Database: {db_path}")
    print()

    try:
        # Load tokenizer and model
        embed_device = get_embedding_device()
        print(f"[SETUP] Loading tokenizer and model: {EMBEDDING_MODEL}")
        print(f"[SETUP] Embedding device: {embed_device}")
        tokenizer = AutoTokenizer.from_pretrained(EMBEDDING_MODEL)
        model = SentenceTransformer(EMBEDDING_MODEL, device=embed_device)
        print(f"[SETUP] Model loaded.")

        # ---- STAGE: INGEST ----
        t0 = now_utc()
        print(f"\n[STAGE] INGEST")
        raw_count = ingest.ingest_raw_documents(conn, input_path, run_id)
        monitoring.log_latency(conn, run_id, "ingest", t0, now_utc(), raw_count)
        monitoring.update_run_counts(conn, run_id, raw_doc_count=raw_count)

        # ---- STAGE: CLEAN ----
        t0 = now_utc()
        print(f"\n[STAGE] CLEAN")
        valid_docs, rejected_count = clean.validate_and_clean_raw(conn, run_id)
        monitoring.log_latency(conn, run_id, "clean", t0, now_utc(), len(valid_docs))
        monitoring.update_run_counts(
            conn, run_id,
            stg_valid_count=len(valid_docs),
            rejected_doc_count=rejected_count
        )

        # ---- STAGE: CURATE ----
        t0 = now_utc()
        print(f"\n[STAGE] CURATE")
        curate_result = curate.curate_documents(conn, run_id, valid_docs)
        new_ids = curate_result["new"]
        updated_ids = curate_result["updated"]
        unchanged_ids = curate_result["unchanged"]

        monitoring.log_latency(conn, run_id, "curate", t0, now_utc(), len(valid_docs))
        monitoring.update_run_counts(
            conn, run_id,
            new_doc_count=len(new_ids),
            updated_doc_count=len(updated_ids),
            unchanged_doc_count=len(unchanged_ids),
            duplicate_doc_count=curate_result["counts"].get("duplicate", 0)
        )

        print(f"  New docs:       {len(new_ids)}")
        print(f"  Updated docs:   {len(updated_ids)}")
        print(f"  Unchanged docs: {len(unchanged_ids)}")

        # ---- STAGE: CHUNK ----
        t0 = now_utc()
        print(f"\n[STAGE] CHUNK")
        docs_to_chunk = new_ids + updated_ids
        new_chunk_count = chunk.chunk_new_documents(conn, run_id, docs_to_chunk, tokenizer)
        monitoring.log_latency(conn, run_id, "chunk", t0, now_utc(), new_chunk_count)

        active_chunk_count = conn.execute(
            "SELECT COUNT(*) FROM fact_chunks WHERE is_active=TRUE"
        ).fetchone()[0]

        monitoring.update_run_counts(
            conn, run_id,
            new_chunk_count=new_chunk_count,
            active_chunk_count=active_chunk_count
        )

        # ---- STAGE: EMBED ----
        t0 = now_utc()
        print(f"\n[STAGE] EMBED")
        new_emb_count = embed.embed_new_chunks(conn, run_id, output_dir, model)
        monitoring.log_latency(conn, run_id, "embed", t0, now_utc(), new_emb_count)

        active_emb_count = conn.execute(
            "SELECT COUNT(*) FROM fact_embeddings WHERE is_active=TRUE"
        ).fetchone()[0]

        monitoring.update_run_counts(
            conn, run_id,
            new_embedding_count=new_emb_count,
            active_embedding_count=active_emb_count
        )

        # ---- STAGE: INDEX ----
        t0 = now_utc()
        print(f"\n[STAGE] INDEX")
        idx_info = index_ivf.build_index(conn, run_id, output_dir, dataset_name, mode)
        monitoring.log_latency(conn, run_id, "index", t0, now_utc(), idx_info["num_vectors"])
        monitoring.update_run_counts(
            conn, run_id,
            index_vector_count=idx_info["num_vectors"]
        )

        # Update active doc count
        active_docs = conn.execute(
            "SELECT COUNT(*) FROM clean_documents WHERE is_active=TRUE"
        ).fetchone()[0]
        monitoring.update_run_counts(conn, run_id, active_doc_count=active_docs)

        # ---- STAGE: RECONCILIATION ----
        print(f"\n[STAGE] RECONCILIATION")
        monitoring.write_reconciliation(conn, run_id)

        # ---- STAGE: AUDIT ----
        if mode == "full":
            print(f"\n[STAGE] AUDIT")
            prev = conn.execute(
                """
                SELECT run_id FROM runs
                WHERE mode='full'
                  AND dataset_name=?
                  AND input_path=?
                  AND status='success'
                  AND run_id != ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                [dataset_name, input_path, run_id]
            ).fetchone()

            if prev:
                print(f"[AUDIT] Comparing run {prev[0]} vs run {run_id}")
                audit.run_audit(conn, prev[0], run_id)
            else:
                print(f"[AUDIT] No previous successful full run found for comparison")

        # Finalize run
        if mode == "augmented":
            notes = ("FAISS rebuilt from all active embeddings; "
                     "ETL-level delta processing confirmed by new/updated counts")
        else:
            notes = None

        monitoring.finish_run(conn, run_id, "success", notes)

        # Print summary
        print(f"\n=== Run {run_id} Complete ===")
        print(f"  Status:           success")
        print(f"  Raw docs:         {raw_count}")
        print(f"  Valid docs:       {len(valid_docs)}")
        print(f"  Rejected:         {rejected_count}")
        print(f"  New docs:         {len(new_ids)}")
        print(f"  Updated docs:     {len(updated_ids)}")
        print(f"  Unchanged docs:   {len(unchanged_ids)}")
        print(f"  New chunks:       {new_chunk_count}")
        print(f"  Active chunks:    {active_chunk_count}")
        print(f"  New embeddings:   {new_emb_count}")
        print(f"  Active embeds:    {active_emb_count}")
        print(f"  Index vectors:    {idx_info['num_vectors']}")
        print(f"  Active docs:      {active_docs}")
        print()

        conn.close()
        return run_id

    except Exception as e:
        print(f"\n[ERROR] Pipeline failed: {e}")
        monitoring.finish_run(conn, run_id, "failed", str(e))
        conn.close()
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Wikipedia RAG ETL Pipeline Builder"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "augmented"],
        required=True,
        help="Build mode: full (rebuild all) or augmented (delta update)"
    )
    parser.add_argument(
        "--dataset-name",
        required=True,
        help="Dataset name (e.g. 'full' or 'live_demo')"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input JSONL file"
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output directory for embeddings and FAISS index"
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Path to DuckDB database file"
    )
    parser.add_argument(
        "--index-type",
        default="ivf",
        choices=["ivf"],
        help="FAISS index type (default: ivf)"
    )
    args = parser.parse_args()

    run_pipeline(
        mode=args.mode,
        dataset_name=args.dataset_name,
        input_path=args.input,
        output_dir=args.output,
        db_path=args.db,
        index_type=args.index_type
    )


if __name__ == "__main__":
    main()
