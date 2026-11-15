# 05 — Release Test Matrix and Remaining Production Gates

## Testing tiers

1. **Static:** parse all Python AST, validate YAML, 20 scope catalog, 10 DW tables, compile requirements, scan artifact paths.
2. **Offline regression:** `python -m unittest discover -s tests -p "test_*.py" -v`; includes salary/skills/location/date/identity, negative tests, authorization fail-close, schema contract, DAG schedule and current-run detail regression.
3. **PostgreSQL integration (requires local server):** set `MARKET_TEST_DSN` to a dedicated `market_integration_test` DB only. Tests deliberately TRUNCATE test tables. Never use a DB named `market` or `topcv_raw_storage` as this test target.
4. **Airflow/Docker runtime:** run `docker compose config`, build images, DAG import and manual triggers, restart services, check checkpoint recovery and 19:00 clock.
5. **Superset integration:** read-only reader permissions, chart API and UI render, validate posted-date and salary semantic disclaimers, backup/restore metadata.
6. **Source live:** only with authorization and network; verify each route/keyword/city, page and detail parser, 403/429 handling, successful multi-day scheduled run evidence.

## Current release's evidence

- Static + offline regression executed in a separate container, with no external network or user database: **335 discovered, 317 PASS, 18 skipped (PostgreSQL)**; clean-ZIP re-extraction and CRC verification PASS.
- PostgreSQL integration tests SKIPPED here because no PostgreSQL server/test DB is available.
- Docker/Chrome/Airflow/Superset runtime and live source collection NOT EXECUTED.
- The route catalog includes 7 historically live-verified inputs and 13 unverified candidates; **20/20 live success is not certified**.
- Data Warehouse schema drift in the historical acceptance receipts (61/87/101 columns) has not been resolved against the user's current database. `verify_schema_contract()` now fails closed if the live schema differs.

## Destructive test command (isolated DB only)

```powershell
# Create a separate, disposable PostgreSQL database market_integration_test first.
$env:MARKET_TEST_DSN = 'postgresql://TEST_USER:TEST_PASSWORD@localhost:5432/market_integration_test'
.\.venv312\Scripts\python.exe -m unittest discover -s tests -p 'test_market_postgres.py' -v
Remove-Item Env:\MARKET_TEST_DSN
```

## Production blockers

- Permission/compliance for TopCV automated collection not evidenced; capture is intentionally disabled by default.
- 13/20 scope routes unverified; TopCV 429/challenges already occurred in prior runs.
- Existing server PostgreSQL catalog, permissions, backup and clean-recovery test unavailable in this environment.
- No operator-run Docker/Airflow/Superset integration proof or real 19:00 tick for this ZIP.
- No tested automatic migration for arbitrary legacy DW schemas or real production HA/alert delivery.
- Full legacy 3,080-test suite had 29 failures/171 errors historically; its historical scripts/receipts were not preserved as a fake passing production suite.

**Therefore status is HARDENED / OFFLINE TESTED / PRODUCTION NOT CERTIFIED.** Do not claim 100% merely because ZIP/regression passes.
