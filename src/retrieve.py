import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
import faiss
import duckdb

from src.config import EMBEDDING_MODEL, QUERY_PREFIX, get_embedding_device


def load_retriever(db_path: str, index_path: str) -> dict:
    """
    Load the retriever components: DuckDB connection, FAISS index, and vector_map.
    Returns a dict with conn, index, vector_map.
    """
    conn = duckdb.connect(db_path, read_only=True)
    index = faiss.read_index(index_path)

    # Load vector_map from same directory as index
    index_dir = os.path.dirname(index_path)
    vector_map_path = os.path.join(index_dir, "vector_map.npy")
    vector_map = np.load(vector_map_path, allow_pickle=True)

    return {
        "conn": conn,
        "index": index,
        "vector_map": vector_map
    }


def retrieve(query: str, db_path: str, index_path: str, top_k: int = 5) -> pd.DataFrame:
    """
    Retrieve top-k chunks for a query.

    Steps:
    1. Embed query with query prefix
    2. Search FAISS index
    3. Map FAISS IDs to chunk_ids via vector_map
    4. Query DuckDB for chunk metadata
    5. Return DataFrame with results

    Returns DataFrame with columns: rank, score, title, url, doc_id,
                                    chunk_id, chunk_text
    """
    # Load model (GPU when available — same policy as build/embed)
    model = SentenceTransformer(EMBEDDING_MODEL, device=get_embedding_device())

    # Embed query
    query_text = f"{QUERY_PREFIX}{query}"
    query_vec = model.encode(
        [query_text],
        normalize_embeddings=True,
        show_progress_bar=False
    )
    query_vec = np.array(query_vec, dtype=np.float32)

    # Load FAISS index
    index = faiss.read_index(index_path)

    # Load vector_map
    index_dir = os.path.dirname(index_path)
    vector_map_path = os.path.join(index_dir, "vector_map.npy")
    vector_map = np.load(vector_map_path, allow_pickle=True)

    # Search
    distances, faiss_ids = index.search(query_vec, top_k)
    distances = distances[0]
    faiss_ids = faiss_ids[0]

    # Map FAISS IDs to chunk_ids
    valid_results = []
    for rank, (fid, score) in enumerate(zip(faiss_ids, distances), start=1):
        if fid < 0 or fid >= len(vector_map):
            continue
        chunk_id = str(vector_map[fid])
        valid_results.append((rank, float(score), chunk_id))

    if not valid_results:
        return pd.DataFrame(columns=["rank", "score", "title", "url", "doc_id",
                                     "chunk_id", "chunk_text"])

    chunk_ids = [r[2] for r in valid_results]
    rank_map = {r[2]: (r[0], r[1]) for r in valid_results}

    # Query DuckDB
    conn = duckdb.connect(db_path, read_only=True)

    placeholders = ", ".join(["?" for _ in chunk_ids])
    sql = f"""
        SELECT fc.chunk_id, fc.doc_id, fc.chunk_text, fc.chunk_index,
               fc.start_tok, fc.end_tok, cd.title, cd.url
        FROM fact_chunks fc
        JOIN clean_documents cd ON fc.doc_id = cd.doc_id AND cd.is_active=TRUE
        WHERE fc.chunk_id IN ({placeholders})
          AND fc.is_active=TRUE
    """
    rows = conn.execute(sql, chunk_ids).fetchall()
    conn.close()

    # Build result dataframe
    records = []
    for row in rows:
        chunk_id = row[0]
        rank, score = rank_map.get(chunk_id, (999, 0.0))
        records.append({
            "rank": rank,
            "score": score,
            "chunk_id": chunk_id,
            "doc_id": row[1],
            "chunk_text": row[2],
            "chunk_index": row[3],
            "start_tok": row[4],
            "end_tok": row[5],
            "title": row[6],
            "url": row[7]
        })

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("rank").reset_index(drop=True)

    return df


def main():
    parser = argparse.ArgumentParser(description="Retrieve chunks from Wikipedia RAG pipeline")
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument("--index", required=True, help="Path to FAISS index file")
    parser.add_argument("--query", required=True, help="Query string")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results")
    args = parser.parse_args()

    print(f"\n=== Retrieval Query ===")
    print(f"Query: {args.query}")
    print(f"Top-K: {args.top_k}")
    print()

    df = retrieve(args.query, args.db, args.index, args.top_k)

    if df.empty:
        print("No results found.")
        return

    for _, row in df.iterrows():
        print(f"[Rank {row['rank']}] Score: {row['score']:.4f}")
        print(f"  Title: {row['title']}")
        print(f"  URL: {row['url']}")
        print(f"  Doc ID: {row['doc_id']}")
        print(f"  Chunk ID: {row['chunk_id'][:16]}...")
        print(f"  Text: {str(row['chunk_text'])[:200]}...")
        print()


if __name__ == "__main__":
    main()
