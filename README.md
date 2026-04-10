# Schema Maker Anomaly Monitor

Databricks-focused anomaly monitoring project for:
- rolling data anomaly detection,
- schema drift detection,
- autoloader rescue-column inspection,
- type-drift detection from rescued payloads,
- API and dashboard-based monitoring.

The copied project has been trimmed to anomaly monitoring only. The CLI, API, and dashboard now focus exclusively on anomaly workflows.

## What It Checks

### Data anomalies
- Monthly, weekly, and daily outliers using P1/P99 bounds.
- Training data comes from prior months, so the current month is always compared against earlier history.
- Manual table-level threshold overrides can still be stored in `table_thresholds.json`.

### Schema anomalies
- Added columns.
- Removed columns.
- Column type changes across runs.
- New incoming fields detected inside autoloader rescue data.
- Suspected type drift when rescued values no longer match the table's declared type.

## How It Is Configured

### `.env`

Only Databricks settings are required for anomaly monitoring:

```env
DATABRICKS_HOST=
DATABRICKS_TOKEN=
DATABRICKS_SQL_WAREHOUSE_ID=
DATABRICKS_CATALOG=nexora_poc_catalog
DATABRICKS_SCHEMA_DOMAIN=bronze
DATABRICKS_SCHEMA_MONITORING=monitoring
ANOMALY_RESCUED_ROW_LIMIT=500
OUTPUT_TIMEZONE=UTC
```

### `anomaly_rules.json`

This file controls which tables are monitored for data anomalies and which columns should be used.

For each rule you can change:
- `table_candidates`
- `date_column_candidates`
- `group_column_candidates`
- `metric_expr`
- `metric_label`
- `lookback_windows`
- `rescued_column_candidates`

If you change datasets, update `anomaly_rules.json` instead of changing Python logic.

## Current Demo Monitors

- `CRM | Account Onboarding Volume`: weekly count of newly created CRM accounts.
- `CRM | Event Attendance Fill Rate`: monthly weighted ratio of actual attendance to estimated attendance.
- `CRM | Urgent Suggestion Share`: weekly percentage of posted suggestions marked as urgent.
- `Sales | Execution Feedback Volume`: weekly count of rep execution-feedback records.
- `Sales | Field Force Time Off Volume`: weekly count of time-off entries affecting territory coverage.

## Data Quality Guardrails

- The detector now filters on parsed dates, not just raw non-null strings.
- If a configured date column exists but contains no usable dates, the detector is skipped instead of being reported as "No Issues".
- Some bronze tables in this demo catalog, such as the `call2_*` and email result tables, currently contain malformed time-only strings in their business date fields, so they are intentionally excluded from the active demo monitors until the source load is corrected.

## Key Output Paths

- `Output/Anomaly/<run_id>/anomalies.txt`
- `Output/Anomaly/<run_id>/anomalies.json`
- `Output/SchemaSnapshots/<catalog>/<schema>/<table>.json`
- `table_thresholds.json`

## Commands

Run anomaly monitoring:

```bash
python -m src.cli anomaly-detect --schema bronze
```

Show version and active catalog defaults:

```bash
python -m src.cli version
```

Start the full project frontend + backend:

```bat
run_project.bat
```

Start the API manually:

```bash
uvicorn api.server:app --host 127.0.0.1 --port 8000 --reload
```

## API

Main endpoints:
- `POST /api/anomaly`
- `POST /api/accept_thresholds`
- `GET /api/latest_anomaly`
- `GET /api/latest_anomaly_json`

See `API_ACCESS_GUIDE.md` for the request and response format.

## Optimization Notes

The old anomaly pipeline issued many repetitive month-by-month SQL queries. The new version:
- aggregates each table once per granularity,
- computes rolling bounds in Python,
- reuses cached schema descriptions per run,
- persists schema snapshots for fast diff-based drift detection.

That keeps the original anomaly idea intact while reducing query overhead substantially.
