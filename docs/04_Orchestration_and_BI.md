# 04 — Orchestration (Airflow), Docker, BI (Superset)

## DAG schedule and defaults

| DAG | Purpose | Schedule | Default |
|---|---|---|---|
| `topcv_market_pipeline` | Main daily scope collection → DW refresh → DQ → coverage | `0 19 * * *`, timezone `Asia/Ho_Chi_Minh` | PAUSED |
| `topcv_scopes_recovery` | Resume due scopes with host cooldown → DW refresh → DQ | `*/30 * * * *` | PAUSED |
| `topcv_warehouse_recovery` | Refresh existing Raw data without requesting TopCV pages | `*/30 * * * *` | PAUSED |

When running the source DAG, a successful Warehouse refresh alone does **not** mean 20/20 sources succeeded. The primary DAG deliberately fails its final coverage task until 20/20 scope statuses are `success`.

## Deployment gating

Two independent live-source variables are required at runtime: `TOPCV_LIVE_COLLECTION_APPROVED=YES` and non-empty `TOPCV_SOURCE_AUTHORIZATION_REFERENCE` (non-secret reference to independently verified authorization). They are insufficient by themselves as legal permission. In Docker Compose they default to `NO` and blank. Existing DAG pause flags must be checked manually after a deployment upgrade.

Use the two recovery DAGs selectively; enabling both concurrently can create redundant DW refresh attempts (a DW advisory lock prevents concurrent writes but may cause a failed task). The scope recovery DAG retries only due scopes and respects the host-wide backoff. Do not bypass 403, CAPTCHA or 429 with stealth or IP rotation.

## Docker storage

The `market-db` PostgreSQL is exposed only to localhost:55440; volumes persist market DB, Airflow logs/config and browser-profile state outside the release ZIP. The `market-init` service now migrates **Airflow metadata only**; it no longer silently initializes Warehouse. Initialize Warehouse manually after backup/review.

## Superset

`bi/superset_isolated/docker-compose.yml` defines an optional isolated Superset container (port 8090). Superset metadata requires its own private secret; its compose file is not the Airflow compose. The BI reader `market_reader` should have SELECT-only on `market_dw`. Use datasets/views in `market_dw`, not the historical `t4_dw` prototype or `topcv_test_a13` unless explicitly building a test dashboard.

Metric acceptance must include exact dataset SQL, counts, time semantics, salary coverage, multi-location/skill joins, permissions and dashboard screenshot checks in the operator's runtime. The previous 13-chart receipt is historical evidence, not a test of this ZIP.
