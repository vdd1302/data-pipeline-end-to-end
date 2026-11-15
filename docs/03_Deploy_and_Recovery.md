# 03 — Safe Deployment, Backup, Recovery

## Before upgrading any running instance

1. **Pause all existing TopCV Airflow DAGs manually** (DAG pause on creation does not pause DAGs already deployed).
2. Make verified backups of `topcv_raw_storage`, `market`, Superset metadata volume and Airflow volumes; restore each backup into a disposable test instance. Never send secrets in support logs.
3. Record actual DB names/ports/roles/schema versions. Run static preflight from ZIP, then `python -m scripts.preflight_release --database` with read-only credentials.
4. If existing DW columns differ from `config/market_dw_schema_contract.json`, STOP. Do not run `--reprocess`, TRUNCATE, or direct DDL on original data. Build and review a migration on a clone first.
5. Check TopCV source authorization. If not approved, keep source DAGs paused and use fixture tests only.

## Fresh local install (Windows PowerShell)

```powershell
py -3.12 -m venv .venv312
.\.venv312\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
# Edit .env: TOPCV_RAW_DATABASE_URL=... (never commit)
.\.venv312\Scripts\python.exe -m scripts.prepare_market_runtime
.\.venv312\Scripts\python.exe -m scripts.preflight_release
# Start DB only, then explicitly initialize warehouse.
docker compose --env-file .env.market -f airflow/docker-compose.market.yml up -d market-db
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline initialize
docker compose --env-file .env.market -f airflow/docker-compose.market.yml up -d --build
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline status
```

## Restore/crash checklist

- After host restart, check Docker Desktop and local PostgreSQL. `docker compose ps` and the Airflow web UI show service status.
- Check `market_ops.scope_queue`, `market_ops.host_backoff` and `market_ops.refresh_checkpoint`. Do not manually set a run to success if it has missing current-run details.
- A checkpoint advances atomically with a DW load batch. Crash recovery re-executes uncommitted work; `ON CONFLICT` is idempotent.
- **Never silently drop a stale watermark.** If checkpoint > Raw max ID, refresh stops and requires source identity/restore inspection.
- Missing DW rows after accidental deletion require a reviewed restoration or an explicit, backed-up `reprocess`, not a normal refresh.

## Controlled reprocessing (test/clone first)

```powershell
$env:TOPCV_ALLOW_REPROCESS = 'YES'
.\.venv312\Scripts\python.exe -m scripts.run_market_pipeline refresh --reprocess --confirm-reprocess
Remove-Item Env:\TOPCV_ALLOW_REPROCESS
```

Reprocessing can change historical classifications and BI numbers. Backup, compare before/after and publish only after DQ review. There is no automatic production migration/rollback mechanism in this release.

## Secrets

Rotate credentials exposed in the old A7.2F ZIP, even though the new ZIP excludes them. Keep `.env`, `.env.market`, cookies, Chrome profiles, database dumps and Superset metadata outside source control and release ZIP.
