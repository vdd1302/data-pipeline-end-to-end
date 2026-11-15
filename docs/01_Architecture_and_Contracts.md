# 01 — System Architecture & Data Contracts

## Modules

| Layer | Files | Input | Output | Key invariant |
|---|---|---|---|---|
| Source configuration | `config/crawl*.yaml`, `src/crawler/config.py` | 10 roles × 2 cities | 20 validated scope configs | A route candidate is NOT proof of authorization |
| Source adapter | `src/crawler/topcv.py`, `parser.py`, `pipeline/t1_topcv_adapter.py` | Public HTML, subject to permission | Validated listing/detail data | Challenge/403/429 stop/defer; do not fabricate jobs |
| Scheduling queue | `src/market_pipeline/crawl.py`, `market_ops.scope_queue` | Scope/day + host policies | Durable run IDs, task statuses | Single host advisory lock; Vietnamese calendar day boundary |
| Raw ingestion | `src/raw_storage/ingestion.py`, `repository.py` | Parsed data with identifiers | `topcv_raw_storage.raw.*` | Atomic page+discovery commits; immutable captures per run |
| Cleaning | `src/cleaning/` | Raw source & provenance | 45-field staging row | Preserve unknown values and quality flags |
| Warehouse | `src/market_pipeline/warehouse.py` | Eligible staging rows | `market_dw` fact/dim/bridge | One fact per `(posting_natural_key,crawl_run_id)` |
| BI | `bi/`, `scripts/publish_market_superset.py` | Versioned DW views | Superset datasets/charts | Reader SELECT-only; validate metrics before publishing |
| Orchestration | `airflow/market_dags/` | Scheduled/manual triggers | Run/refresh/quality tasks | Paused on creation; 19:00 local-time primary DAG |

## Database separation

- Raw credentials grant source-specific read/write to collector, read-only to ETL where supported.
- Warehouse owner owns `market_ops`, `staging`, `market_dw`. Superset uses a read-only `market_reader` only.
- The Airflow metadata database currently shares the `market` PostgreSQL instance; this is an operational simplification, not a production HA design.
- A live crawl failure does not erase prior data; Warehouse refresh can remain independent.

## Runtime status

The original zip had live `.env`, browser sessions and a virtualenv. They are excluded. Existing deployed containers/credentials are **not** affected by ZIP editing; users must rotate exposed secrets and pause existing DAGs themselves.

## Compatibility and migration policy

`config/market_dw_schema_contract.json` enumerates the *current* physical DW tables/columns. `verify_schema_contract()` raises on missing or unexpected columns. No data is dropped/renamed automatically. Historical `t4_dw` (B3 prototype) and current `market_dw` differ. Database upgrade must happen on a backup clone and requires an approved migration if schema drift is detected.
