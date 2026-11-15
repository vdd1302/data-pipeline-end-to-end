# 02 — Data Flow, Grain, and Reliable Analytics

**Source → Raw → staging → Warehouse → Superset**

1. For each scope and Vietnamese date, a deterministic run ID points to its search task. A verified listing page commits page + distinct discoveries atomically.
2. Each detail observation is bound to its **current** run. A previous day's detail is historical, NOT a current-run observation; therefore `_pending_details` searches for successful observations in the current run only.
3. Raw remains immutable; no fabricated current captures from cached HTML. AccessChallenge or HTTP 429/403 defers/blocks rather than injecting incomplete pages.
4. Only eligible TopCV rows from Raw enter the 45-column staging normalization. Rejected mappings are recorded in `market_ops.mapping_errors`, not silently counted as facts.
5. Load dimension members and a fact by `(posting_natural_key,crawl_run_id)` plus skill/location bridges. Use a transaction per batch with `market_ops.refresh_checkpoint`, so a crash replays the uncommitted batch.
6. BI reads `market_dw` views. A single job may have many fact snapshots, locations and skills.

## Metric definitions — no implicit inference

| Metric | Definition / caveat |
|---|---|
| `snapshot observation_count` | Count of fact rows at a snapshot date. Not the number of newly published vacancies. |
| `unique_jobs` at a snapshot | Distinct posting key in that one slice; summing across dates double-counts continuing jobs. |
| `total_jobs` from `market_dw.overview` | Distinct latest row per posting **within the collected sample**; not all currently active market vacancies. |
| `new`, `continuing`, `reappeared` | `lifecycle_status='unknown'` unless independent evidence establishes the state. Existing three boolean flags must not be used as evidence when unknown. |
| Posted-date trend | `market_dw.verified_posted_dates` includes only `is_temporal_eligible` observations with valid `posted_date_key`; ineligible/historical dates must not be portrayed as current market trends. |
| Salary average | Only rows with `salary_midpoint_known=TRUE` (VND/month), and report sample coverage. Unknown numeric values use -1 with known flags. |
| Skill count | Distinct posting per skill after connecting via skill bridge; categories are not additive across skills. |
| City count | Multiple cities per job via bridge; separate raw distinct count from allocation-weighted estimates. |

### A lifecycle caution

`is_new`, `is_continuing`, `is_reappeared` are NOT reliable negative indicators when zero/false by default. A separate `lifecycle_status='unknown'` explicitly captures insufficient source history. Do not infer vacancies are closed because a job is absent from one incomplete crawl.

## Failure handling

- Raw observation errors: log, retain provenance, do not invent an observation.
- Bad cleaned row: store mapping-error category, continue other rows; reprocess after fixing mapper.
- SQL constraint/migration failure: stop and roll back batch/checkpoint; operator must inspect and correct schema on a clone.
- Retry: use bounded host backoff and source-defined `Retry-After`, no source-defense circumvention.
