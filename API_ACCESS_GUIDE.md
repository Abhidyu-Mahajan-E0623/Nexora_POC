# Schema Maker API Access Guide

## Base URL

Local example:

```text
http://127.0.0.1:8000
```

## Main Endpoint

### `POST /api/anomaly`

Runs the anomaly monitor for a Databricks schema.

Request body:

```json
{
  "schema": "raw"
}
```

Response shape:

```json
{
  "run_id": "run_20260406T123456Z_abc123",
  "total_anomalies": 18,
  "data_anomalies": 11,
  "schema_anomalies": 7,
  "checks_run": 10,
  "checks_with_issues": 5,
  "report_text": "....",
  "report_data": {
    "run_id": "run_20260406T123456Z_abc123",
    "generated_at": "2026-04-06T12:34:56+00:00",
    "data_source": "catalog.raw",
    "checks_run": 10,
    "checks_with_issues": 5,
    "total_anomalies": 18,
    "data_anomalies": 11,
    "schema_anomalies": 7,
    "detectors": []
  }
}
```

Example call:

```bash
curl -X POST \
  http://127.0.0.1:8000/api/anomaly \
  -H "Content-Type: application/json" \
  -d "{\"schema\":\"raw\"}"
```

## Threshold Overrides

### `POST /api/accept_thresholds`

Stores manual min or max overrides for one table in `table_thresholds.json`.

Request body:

```json
{
  "table_name": "snr_fact_snr_sales",
  "min_val": 1000,
  "max_val": 500000
}
```

Response:

```json
{
  "success": true,
  "message": "Thresholds updated for snr_fact_snr_sales"
}
```

### `GET /api/accepted_state`

Returns the saved accepted tables and accepted schema columns from `table_thresholds.json`.

### `DELETE /api/accepted_state`

Deletes `table_thresholds.json` so saved threshold overrides and schema acceptances are cleared.

## Latest Report Endpoints

### `GET /api/latest_anomaly`

Returns the latest text report from:

```text
Output/Anomaly/<latest_run>/anomalies.txt
```

### `GET /api/latest_anomaly_json`

Returns the latest structured payload from:

```text
Output/Anomaly/<latest_run>/anomalies.json
```

## What `report_data.detectors` Contains

Each detector result includes:
- `category`: `data` or `schema`
- `display_name`
- `table_name`
- `table_fqn`
- `status`
- `anomaly_count`
- `notes`
- `monthly_anomalies`
- `weekly_anomalies`
- `daily_anomalies`
- `schema_findings`

## Error Format

On failure, the API returns:

```json
{
  "detail": "Error message"
}
```
