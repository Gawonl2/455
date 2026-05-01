# Auditable Wikipedia RAG-ETL Pipeline with FAISS IVF

A fully auditable ETL pipeline that ingests Wikipedia articles, chunks and embeds them with E5-base-v2, builds a FAISS IVF index, and supports dense retrieval — all with SCD-style versioning, cross-run audit checks, and row-count reconciliation tracked in DuckDB.

---

## Short Description

This project implements a production-grade RAG (Retrieval-Augmented Generation) data pipeline for Wikipedia. Every document, chunk, and embedding is tracked in a DuckDB metadata store with full lineage. The pipeline supports **full rebuilds** (idempotent) and **augmented builds** (delta updates), with automatic audit verification between runs.

---

## Data Grain (All 4 Core Tables)

| Table | Grain | Notes |
|-------|-------|-------|
| `raw_documents` | One row per ingested document per run | Never modified after insert |
| `clean_documents` | One active row per document (SCD Type 2) | `is_active=TRUE` for current version |
| `fact_chunks` | One row per sliding-window chunk (SCD Type 2) | `is_active=TRUE` for current chunks |
| `fact_embeddings` | One row per embedded chunk | Per-run numpy files on disk |

---

## Update Contract (4 Rules)

1. **NEW docs** — document id not seen before → inserted into `clean_documents`, chunked and embedded
2. **UPDATED docs** — same id, different content hash → old row deactivated (`is_active=FALSE, valid_to_run_id=run_id`), new row inserted; old chunks and embeddings deactivated
3. **UNCHANGED docs** — same id, same content hash → no changes to any table (idempotent)
4. **REJECTED docs** — empty text or missing id → logged in `rejected_documents` with `reason_code` and `reason_detail`

---

## Failure Modes

| Failure | Behavior |
|---------|----------|
| Empty text | Rejected with `reason_code='EMPTY_TEXT'` |
| Missing id | Rejected with `reason_code='MISSING_ID'` |
| Pipeline crash | `runs.status` set to `'failed'`, exception re-raised |
| Missing embedding file | `FileNotFoundError` raised during index build |
| Zero active embeddings | `ValueError` raised during index build |
| Duplicate submission | Treated as UNCHANGED (idempotent) |

---

## Full Rebuild Commands

### Full dataset
```bash
python src/build.py \
  --mode full \
  --dataset-name full \
  --input data/full/raw_wiki.jsonl \
  --output outputs/full \
  --db outputs/full/wiki.duckdb \
  --index-type ivf
```

### Live demo dataset
```bash
python src/build.py \
  --mode full \
  --dataset-name live_demo \
  --input data/live_demo/initial_sample.jsonl \
  --output outputs/live_demo \
  --db outputs/live_demo/wiki_demo.duckdb \
  --index-type ivf
```

---

## Augmented Build Commands

### Full dataset
```bash
python src/build.py \
  --mode augmented \
  --dataset-name full \
  --input data/full/updates.jsonl \
  --output outputs/full \
  --db outputs/full/wiki.duckdb \
  --index-type ivf
```

### Live demo dataset
```bash
python src/build.py \
  --mode augmented \
  --dataset-name live_demo \
  --input data/live_demo/update_sample.jsonl \
  --output outputs/live_demo \
  --db outputs/live_demo/wiki_demo.duckdb \
  --index-type ivf
```

---

## Live Demo Instructions

### Step 1: Prepare demo data (requires full Wikipedia JSONL)
```bash
python src/make_live_demo_data.py \
  --full-input data/full/raw_wiki.jsonl \
  --live-dir data/live_demo \
  --sample-size 40
```

### Step 2: Full build (first time)
```bash
python src/build.py \
  --mode full \
  --dataset-name live_demo \
  --input data/live_demo/initial_sample.jsonl \
  --output outputs/live_demo \
  --db outputs/live_demo/wiki_demo.duckdb \
  --index-type ivf
```

### Step 3: Full build again (idempotency test)
```bash
# Same command as Step 2 — all docs should be UNCHANGED, audit should PASS
python src/build.py \
  --mode full \
  --dataset-name live_demo \
  --input data/live_demo/initial_sample.jsonl \
  --output outputs/live_demo \
  --db outputs/live_demo/wiki_demo.duckdb \
  --index-type ivf
```

### Step 4: Augmented build (delta update)
```bash
python src/build.py \
  --mode augmented \
  --dataset-name live_demo \
  --input data/live_demo/update_sample.jsonl \
  --output outputs/live_demo \
  --db outputs/live_demo/wiki_demo.duckdb \
  --index-type ivf
```

### Step 5: Retrieve
```bash
python src/retrieve.py \
  --db outputs/live_demo/wiki_demo.duckdb \
  --index outputs/live_demo/faiss/index.faiss \
  --query "What document discusses vector databases and retrieval systems?" \
  --top-k 5
```

---

## Monitoring Artifacts

All stored in DuckDB (`outputs/{dataset}/wiki[_demo].duckdb`):

| Table | Purpose |
|-------|---------|
| `runs` | Per-run metadata, counts, status |
| `latency_logs` | Per-stage timing (ingest, clean, curate, chunk, embed, index) |
| `row_count_reconciliation` | Cross-table count checks; PASS if `active_chunks == active_embeddings == index_vectors` |
| `audit_results` | Cross-run consistency checks (5 checks: doc count, chunk count, embedding count, index vector count, chunk hash checksum) |
| `rejected_documents` | All rejected docs with reason codes |
| `faiss_index_registry` | Index provenance: path, checksum, nlist, num_vectors, build_mode |

---

## IVF Explanation

**Inverted File Index (IVF)** partitions the vector space into `nlist` Voronoi cells using k-means clustering. At search time, only `nprobe` cells are searched (instead of all vectors), giving sub-linear search time.

This pipeline uses:
- `IndexIVFFlat` with `METRIC_INNER_PRODUCT` (cosine similarity via normalized vectors)
- **Adaptive nlist**: `actual_nlist = min(100, max(4, N // 10))` — prevents nlist > N and keeps meaningful partitions for small corpora
- `nprobe = 10` — trades recall for speed
- All vectors normalized via `sentence-transformers` `normalize_embeddings=True`

---

## FAISS Index Rebuild Rationale for Augmented Mode

In augmented mode, the FAISS index is **fully rebuilt from all active embeddings**, not just the delta.

**Rationale**: FAISS IVF requires the full training set to build accurate Voronoi cell centroids. Incrementally adding vectors to an existing IVF index degrades search quality because the cells were trained on the original distribution. Full rebuild ensures:
- Consistent recall across all vectors
- Correct nprobe coverage
- Valid `vector_map.npy` alignment with current FAISS IDs

The ETL delta processing (only new/updated docs are chunked and embedded) is confirmed by the `new_doc_count` and `updated_doc_count` columns in the `runs` table. Only the index construction step touches all active embeddings.
