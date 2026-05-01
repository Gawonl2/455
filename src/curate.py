import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def curate_documents(conn, run_id: int, valid_docs: list) -> dict:
    """
    SCD logic for each valid_doc.

    For each doc:
    - NEW: not in clean_documents with is_active=TRUE -> insert
    - UNCHANGED/DUPLICATE: exists with same content_hash -> do nothing
    - UPDATED: exists with different content_hash -> deactivate old, insert new

    Returns dict with 'new', 'updated', 'unchanged', 'counts'.
    """
    new_ids = []
    updated_ids = []
    unchanged_ids = []

    new_rows = []

    for doc in valid_docs:
        doc_id = doc["doc_id"]
        content_hash = doc["content_hash"]

        # Check for existing active row
        existing = conn.execute(
            """
            SELECT content_hash
            FROM clean_documents
            WHERE doc_id=? AND is_active=TRUE
            LIMIT 1
            """,
            [doc_id]
        ).fetchone()

        if existing is None:
            # NEW document
            new_rows.append((
                doc_id,
                doc["url"],
                doc["title"],
                doc["cleaned_text"],
                content_hash,
                doc["text_len"],
                run_id,
                True,
                run_id,
                None
            ))
            new_ids.append(doc_id)

        elif existing[0] == content_hash:
            # UNCHANGED / DUPLICATE: same content
            unchanged_ids.append(doc_id)

        else:
            # UPDATED: different content_hash
            # Deactivate old clean_documents row
            conn.execute(
                """
                UPDATE clean_documents
                SET is_active=FALSE, valid_to_run_id=?
                WHERE doc_id=? AND is_active=TRUE
                """,
                [run_id, doc_id]
            )

            # Deactivate old chunks for this doc
            conn.execute(
                """
                UPDATE fact_chunks
                SET is_active=FALSE, valid_to_run_id=?
                WHERE doc_id=? AND is_active=TRUE
                """,
                [run_id, doc_id]
            )

            # Deactivate old embeddings for this doc's now-deactivated chunks
            conn.execute(
                """
                UPDATE fact_embeddings
                SET is_active=FALSE
                WHERE chunk_id IN (
                    SELECT chunk_id FROM fact_chunks
                    WHERE doc_id=? AND valid_to_run_id=?
                )
                """,
                [doc_id, run_id]
            )

            # Insert new active row
            conn.execute(
                """
                INSERT INTO clean_documents
                    (doc_id, url, title, cleaned_text, content_hash, text_len,
                     source_run_id, is_active, valid_from_run_id, valid_to_run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, ?, NULL)
                """,
                [
                    doc_id,
                    doc["url"],
                    doc["title"],
                    doc["cleaned_text"],
                    content_hash,
                    doc["text_len"],
                    run_id,
                    run_id
                ]
            )

            updated_ids.append(doc_id)

    # Batch insert all new documents
    if new_rows:
        conn.executemany(
            """
            INSERT INTO clean_documents
                (doc_id, url, title, cleaned_text, content_hash, text_len,
                 source_run_id, is_active, valid_from_run_id, valid_to_run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            new_rows
        )

    counts = {
        "new": len(new_ids),
        "updated": len(updated_ids),
        "unchanged": len(unchanged_ids),
        "duplicate": 0
    }

    print(
        f"[CURATE] New: {counts['new']}, Updated: {counts['updated']}, "
        f"Unchanged: {counts['unchanged']}"
    )

    return {
        "new": new_ids,
        "updated": updated_ids,
        "unchanged": unchanged_ids,
        "counts": counts
    }
