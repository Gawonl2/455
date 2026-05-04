# IS 455 (Spring 2026) - Final Project (FP) Announcement

- Posted: April 14, 2026
- Created: 2026-04-14 23:40:34 CDT
- Course: IS 455 - Database Design & Prototyping

## 1) Deadline and Checkpoint

- Final project package submission deadline: **Sunday, May 3, 2026, 11:59 PM (CT)**
- In-class final project presentation dates: **Monday, May 4, 2026** and **Wednesday, May 6, 2026**
- Each student will present on one of those two dates, based on the assigned schedule.
- Expected time per student is approximately **17-18 minutes**. Time is strictly enforced.
- Format is free (Slides, Notebook scroll, terminal demo, app demo, etc.), but a live technical demo is required.
- Suggested time split (guide, not mandatory): architecture + cleaning (~5 min), live update scenario (~5 min), failure-catalog walk-through (~3 min), Q&A (~3-4 min).
- No exception can be made for the assigned presentation date without explicit and verifiable university-approved justification. Without that, you will get 0 point for the presentation.
- If you cannot attend the presentation, you can submit a video record, and I will grade it with 50% cut for the presentation component.

## 2) FP Project Theme (Required for everyone)

Build a runnable ETL system around a topic you care about. The project must show what happens when data changes and how the pipeline updates in response — new batches arrive, old rows may be corrected, and realistic failures (duplicates, missing values, inconsistent fields, late-arriving records) must be surfaced through monitoring or audit. You must demonstrate not only a full rebuild but also an **augmented (incremental) build** that re-processes only what is needed when data updates.

- AI is optional. Correct use of `vectorDB`, `RAG`, or `GraphRAG` is worth **up to +15% on top of your base score** (see Section 5), but does **not** replace the ETL / update / monitoring contract.
- Synthetic data is encouraged. It should reflect real-world scenarios. If you use it, you must clearly explain how you generated it (see Section 4).
- The project should serve one of these purposes:
  1. `Tech Interview Portfolio`
  2. `Research portfolio`
  3. `Pipeline demonstration`

## 3) Required Deliverables

Submit (through Canvas) one package containing:

0. `data_generate.py` which generates data. 
1. `final_report` (Jupyter Notebook with detailed explanations and comments, or PDF).
2. `build.py` or equivalent reproducible pipeline entrypoint.
3. Project code and small supporting files needed to rerun the system.
4. `README.md` with run instructions, artifact map, and dependency notes.

## 4) Minimum Technical Requirements

- Use `DuckDB + Python`.
- Keep `raw / staging / curated` layers conceptually separate, or use a clearly equivalent layered design.
- Your system must run end-to-end as an ETL framework or equivalent pipeline.
- Include at least one monitoring / observability artifact.
- Document at least one design tradeoff.
- Document at least one failure mode and how it is detected.
- Hardcoded absolute paths are not allowed. All paths should be relative.
- Your project must be demonstrable live.

Build and update contract (required):

- Your pipeline must support **two build modes**:
  - **Full rebuild** — one-command rebuild of all layers from raw (`build.py` or equivalent). Must be idempotent: same input produces the same output.
  - **Augmented (incremental) build** — when new or corrected data arrives, re-process only what is needed (changed-only ingestion, partition-scoped rebuild, merge-on-key, upsert, or equivalent). A full rebuild alone is **not** sufficient.
- You must demonstrate at least one concrete update event live: a new batch, a corrected batch, or a late-arriving record. **A pre-prepared second batch is acceptable** — you do not have to inject data on stage; you just have to run the augmented build on it live and show the delta. Show what the system re-computes, what it leaves alone, and how the result differs from a fresh full rebuild.
- Document your update contract: how you detect what is new or changed, how you avoid double-counting, and how a stale run is superseded. Monitoring evidence should make each run distinguishable (e.g., a `runs` table row per build, with mode, input scope, and row counts).
- **Idempotency must be verified, not just claimed.** Include at least one audit artifact (e.g., `row_count_reconciliation` before/after two consecutive full rebuilds, a checksum/hash diff of curated outputs, or an `audit_results` table comparing run N and run N+1) using the audit techniques from lecture. Verbal assurance ("I ran it twice and it looked the same") is not accepted.

Data lifecycle documentation (required):

- **Update scenario** — one paragraph describing how data keeps arriving: cadence (hourly / daily / event-driven), shape of each arrival (append / replace / backfill / correction), and what triggers a run.
- **User-facing output (3-line contract)** — (1) who the downstream user is, (2) what surface they see (one of: table, view, notebook, dashboard, or API response), (3) the refresh cadence and how staleness or partial updates are communicated. No UI is required.
- **Cleaning strategy** — for each transition (raw → staging, staging → curated), 1-2 sentences summarizing what you drop / repair / reconcile / flag, plus **one short SQL or Python snippet per transition** that shows the rule in code.
- **Injected failure catalog** — at minimum **one failure mode** present in your raw data (e.g., duplicates, missing values, inconsistent fields, late-arriving records, schema drift, corrupted rows, broken FK, or corrections that supersede earlier rows). For each listed item, state three things: (a) how it is injected (if synthetic) or how it naturally arises (if real), (b) which layer detects it, (c) how the curated layer and the user-facing output behave in its presence. Additional failure modes are welcome but not required.

If you use synthetic data:

- `data_generate.py` must produce **two batches**: an initial batch and an update batch. The update batch should include at least one of: new records, corrections to earlier rows, or late-arriving records. You do not need to model a continuous stream.
- The data should support the setting described in Section 2: meaningful ETL, a real update delta, and at least your one listed failure mode.
- Clearly explain how you generated it and why the scenario is realistic.

During the report and the live demo, you must be able to answer the following six questions:

- `one row = ?` (i.e., the grain of your curated fact table)
- `full rebuild vs. augmented build — what is the difference on this run?`
- `when a new or corrected batch arrives, what does the system re-compute and what does it skip?`
- `who sees what — the user-facing surface and its refresh contract`
- `what failure modes are present in your raw data, and how does each one surface in your pipeline?`
- `what fails at runtime, and how do you detect it?`

Monitoring examples (core ETL):

- `runs` table
- `audit_results`
- `row_count_reconciliation`
- `latency_logs`
- data quality / drift checks

If you opt into AI (optional, worth up to +15% on top of your base score):

- Use `vectorDB`, `RAG`, or `GraphRAG` in a way that is **correct** (retrieval is measurable, grounding is demonstrable) and **integrated with your ETL contract** (retrieval inputs go through the same staging / curated layers as the rest of your pipeline).
- Add at least one AI-specific monitoring artifact: `retrieval_eval`, `cost_summary`, or `agent_trace`.
- AI work does not replace the core ETL / update / monitoring contract. If the AI layer fails review, you lose the bonus, not your base score.

## 5) Grading Rubric

- Final report package (code, rebuild, documentation, monitoring, and system explanation): 60%
  1. Use of learned methods and system design: 20%
  2. Report clarity and correctness: 20%
  3. Code correctness, full + augmented build discipline, and monitoring evidence: 20%
- Final presentation (live demo, architecture explanation, failure story, and Q&A): 40%
- Optional AI bonus (correct and integrated use of `vectorDB` / `RAG` / `GraphRAG`): **up to +15%** added on top of the base score. Capped at an overall ceiling of 115%.

Comment: In FP, we intentionally weight runnable system evidence and live demonstration heavily, because the course is ending with a stronger standard: not just an idea, but a reproducible, inspectable, and monitorable data system.

Projects will be graded down if they are missing core system evidence such as:

- undefined row / unit of analysis
- no pipeline layering
- no rerun story (idempotency not verified with an audit artifact)
- no augmented / incremental build — only a full rebuild
- no live update event (new / corrected / late-arriving batch)
- no user-facing output contract (consumer, surface, refresh)
- no cleaning rules shown in code (snippet per transition missing)
- no failure mode listed and traced through the pipeline
- no monitoring evidence
- app demo only, with no database or pipeline evidence

## 6) Late Policy and Integrity

- Late penalty: -10% per day for the report. For the presentation, no exception.
- You may use AI tools. 
- Do not follow any example project or track verbatim below. Use examples as inspiration only, and submit your own topic, problem framing, and implementation choices.

## 7) Suggested Project Purposes

The three purposes in Section 2 (`Tech Interview Portfolio`, `Research portfolio`, `Pipeline demonstration`) are optional framing only, not a change to the grading contract. The key is not whether your topic sounds impressive — it is whether the system is runnable, inspectable, and monitorable.

## 8) Example Project Idea

The examples below are optional inspiration tracks, not a change to the FP grading contract. They intentionally combine methods you already learned (Lectures 1-9) with near-term inspection and performance ideas up to Lecture 13.

Each write-up is prose-style by design and focuses on three practical dimensions: what data exists, what decision goal matters, and what challenge makes results fragile in real systems. Each example is followed by an **FP addendum** line reminding you what the augmented build, update event, and failure catalog would look like for that scenario — these are required for FP even when the example itself does not spell them out.

### Example 1 - Promotion-Driven Revenue Drift in E-commerce

- Data: You can start from the Olist Brazilian E-commerce Public Dataset (orders, order items, payments, reviews, customer metadata) and add a simple campaign metadata file that records coupon windows, ad channels, and discount rules.
- Kaggle link: Olist Brazilian E-Commerce Public Dataset
- Goal: Build a reliable promotion impact report that can answer whether observed revenue lift is real or inflated, and separate true conversion improvements from data assembly artifacts.
- Challenge: Promotional events are often duplicated across channels, and many-to-many joins between orders, item lines, and campaign touches can silently multiply rows; this can make revenue and conversion look better than they are, especially when late fixes are reloaded inconsistently.
- Method linkage: This scenario directly applies L3 aggregation discipline, L4 audit-first validation, L5-6 join-cardinality risk control, L9 idempotent raw/staging/curated rebuilds, and L13 plan inspection to verify that filter/project steps happen early and bottlenecks are visible.
- FP addendum: Augmented build = re-process only the orders and campaign touches in the affected window when a late fix arrives. Update event = a second-batch reload of corrected campaign metadata. Failure catalog candidate = duplicated promotional events causing row multiplication.

### Example 2 - Readmission Cohort Instability in Healthcare Data

- Data: Use Synthea-generated EHR exports (encounters, conditions, procedures, medications, patient demographics) and optionally align trends against public CMS Hospital Readmissions Reduction Program reference tables for benchmarking context.
- Kaggle link: Cynthia Data - Synthetic EHR Records
- Goal: Produce a stable monthly readmission trend report by condition group and facility profile that can support quality-improvement prioritization without shifting definitions every run.
- Challenge: Diagnosis label drift, partial patient-link quality, and late-arriving clinical updates can silently change cohort boundaries, so a report that looked correct last week may disagree with this week for reasons unrelated to clinical reality.
- Method linkage: This case uses L4 profiling and contradiction audits, L7-8 normalization and stable identity modeling for cleaner joins, L9 reproducible staging-to-curated pipeline discipline, and L13 EXPLAIN-oriented inspection to diagnose heavy scans, misplaced filters, and query-plan bottlenecks before they become operational reporting failures.
- FP addendum: Augmented build = re-process only the months touched by late-arriving encounters or relabeled diagnoses. Update event = a second-batch drop of corrected diagnosis codes. Failure catalog candidate = diagnosis label drift that shifts cohort membership across runs.

### Example 3 - Peak-Demand Forecasting from City Mobility Logs

- Data: Combine NYC TLC trip records (taxi pickup/dropoff events) with daily weather signals (NOAA) and public event calendars (city events, holidays, stadium schedules) to represent operational demand context.
- Kaggle link: NYC Taxi Trip Duration (Kaggle Competition)
- Goal: Deliver reliable surge-window demand summaries that can support zone-level staffing or fleet positioning decisions under real constraints, not only retrospective dashboards.
- Challenge: In production-like pipelines, partition boundaries and incremental loads are easy to mis-handle, missing keys can bias area-level aggregates, and unfiltered full-table scans can make the same query both slow and inconsistent under time pressure.
- Method linkage: This project concretely uses L2 filtering and NULL handling, L3 group-by correctness, L9 raw/staging/curated design with reproducible rebuild logic, week-plan partition and changed-only ingestion ideas, and L13 filter-early/project-early plan reading to keep both correctness and runtime predictable.
- FP addendum: Augmented build = partition-scoped rebuild on the affected day or zone only. Update event = a second-batch drop of trip records for one additional day plus a weather correction. Failure catalog candidate = late-arriving trip records that change surge-window aggregates.

### Example 4 - Subscription Churn Misread Under Cross-Device Identity Fragmentation

- Data: Start with the KKBox Churn Prediction dataset and extend it with synthetic identity-link logs (device IDs, cookies, hashed emails, session keys) that mimic realistic multi-device user behavior and imperfect linking confidence.
- Kaggle link: KKBox Churn Prediction Challenge
- Goal: Estimate churn risk and campaign effect at the user level in a way that remains stable when identity linkage rules or ingestion frequency change, so business actions are based on true user trajectories.
- Challenge: Fragmented identities can duplicate active users and leak information across linked records, while large linkage joins degrade performance and can hide silent counting errors when multiplicity is not audited.
- Method linkage: The setup is a direct application of L5-6 join semantics and explosion prevention, L7-8 surrogate-identity modeling and normalization strategy, L9 reproducible ETL discipline, and L13 query-plan-driven optimization to confirm that expensive joins are controlled and execution behavior matches analytical intent.
- FP addendum: Augmented build = upsert / merge-on-key on the identity-link table when the linkage rule changes, re-computing only affected users. Update event = a second-batch drop of new device→user links. Failure catalog candidate = duplicated active users caused by mis-linked identity fragments.
