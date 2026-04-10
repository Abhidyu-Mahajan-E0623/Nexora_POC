"""Anomaly detection pipeline with data and schema anomaly classification."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
import json
import math
import re

from src.config.settings import Settings
from src.utils.io import (
    anomaly_json_path,
    anomaly_output_dir,
    atomic_write_json,
    atomic_write_text,
    ensure_project_dirs,
    read_json,
    schema_snapshot_path,
)
from src.utils.logging import configure_logging
from src.utils.time import new_run_id, utc_iso

if TYPE_CHECKING:
    from src.connectors.databricks_sql import DatabricksSQLClient

DEFAULT_RESCUED_COLUMNS = ("_rescued_data", "_rescued_data_json", "rescued_data")
ANOMALY_RULES_FILE = Path(__file__).resolve().parent.parent / "anomaly_rules.json"
THRESHOLDS_FILE = Path(__file__).resolve().parent.parent / "table_thresholds.json"

GRANULARITY_CONFIG: dict[str, dict[str, Any]] = {
    "month": {
        "period_expr": "DATE_TRUNC('month', TO_DATE({date_col}))",
        "title": "MONTHLY ANOMALIES",
        "label": "Monthly",
        "period_len": 7,
        "min_window": 2,
    },
    "week": {
        "period_expr": "DATE_TRUNC('week', TO_DATE({date_col}))",
        "title": "WEEKLY ANOMALIES",
        "label": "Weekly",
        "period_len": 10,
        "min_window": 4,
    },
    "day": {
        "period_expr": "TO_DATE({date_col})",
        "title": "DAILY ANOMALIES",
        "label": "Daily",
        "period_len": 10,
        "min_window": 10,
    },
}

NUMERIC_TYPE_TOKENS = ("int", "bigint", "smallint", "tinyint", "float", "double", "decimal", "numeric", "long")
DATE_TYPE_TOKENS = ("date", "timestamp")
BOOL_TRUE_VALUES = {"true", "t", "yes", "y"}
BOOL_FALSE_VALUES = {"false", "f", "no", "n"}


def load_thresholds() -> dict[str, Any]:
    """Load persisted manual threshold overrides from the single thresholds file."""
    if not THRESHOLDS_FILE.exists():
        return {}
    try:
        payload = json.loads(THRESHOLDS_FILE.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}



@dataclass
class AnomalyFinding:
    """Single data anomaly finding."""

    period: str
    actual_value: float
    lower_bound: float
    upper_bound: float
    direction: str
    group_label: str | None = None


@dataclass
class SchemaFinding:
    """Single schema anomaly finding."""

    finding_type: str
    column_name: str
    details: str
    previous_type: str | None = None
    current_type: str | None = None
    observed_type: str | None = None
    sample_values: list[str] = field(default_factory=list)
    occurrence_count: int = 0


@dataclass
class DetectorResult:
    """Result for a detector on a single table."""

    category: str
    detector: str
    display_name: str
    table_name: str
    table_fqn: str
    threshold: str
    status: str
    anomaly_count: int
    findings: list[str]
    notes: list[str]
    monthly_anomalies: list[AnomalyFinding] = field(default_factory=list)
    weekly_anomalies: list[AnomalyFinding] = field(default_factory=list)
    daily_anomalies: list[AnomalyFinding] = field(default_factory=list)
    schema_findings: list[SchemaFinding] = field(default_factory=list)


@dataclass
class AnomalyDetectionOutcome:
    """Summary of an anomaly detection run."""

    run_id: str
    total_anomalies: int
    data_anomalies: int
    schema_anomalies: int
    checks_run: int
    checks_with_issues: int
    output_dir: str
    report_path: str
    json_path: str
    detector_results: list[DetectorResult]


@dataclass
class DataAnomalyRule:
    """Configurable rule for a data anomaly detector."""

    detector: str
    display_name: str
    table_candidates: list[str]
    date_column_candidates: list[str]
    metric_expr: str
    metric_label: str
    group_column_candidates: list[str] = field(default_factory=list)
    rescued_column_candidates: list[str] = field(default_factory=lambda: list(DEFAULT_RESCUED_COLUMNS))
    granularities: list[str] = field(default_factory=lambda: ["month", "week", "day"])
    lookback_windows: dict[str, int | None] = field(default_factory=dict)
    business_summary: str | None = None
    where_clause: str | None = None
    filter_note: str | None = None
    schema_monitoring: bool = True
    enabled: bool = True


def run_bronze_anomaly_detection(
    settings: Settings,
    run_id: str | None = None,
    catalog: str | None = None,
    schema: str = "bronze",
    logger: Any | None = None,
) -> AnomalyDetectionOutcome:
    """Detect data and schema anomalies and persist both text and JSON reports."""
    from src.connectors.databricks_sql import DatabricksSQLClient

    ensure_project_dirs()
    resolved_run_id = run_id or new_run_id()
    logger = logger or configure_logging(run_id=resolved_run_id)
    resolved_catalog = (catalog or settings.DATABRICKS_CATALOG).strip()
    sql_client = DatabricksSQLClient(settings=settings, logger=logger)
    thresholds = load_thresholds()

    logger.info(
        "[anomaly] Loading detector rules from %s",
        ANOMALY_RULES_FILE,
        extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": "load_rules"},
    )
    rules = load_anomaly_rules(logger=logger)

    logger.info(
        "[anomaly] Listing tables in %s.%s",
        resolved_catalog,
        schema,
        extra={"run_id": resolved_run_id, "log_module": "anomaly", "step": "list_tables"},
    )
    available_tables = _list_tables(sql_client, resolved_catalog, schema)

    table_context = _build_table_context(rules=rules, available_tables=available_tables)
    table_columns_cache: dict[str, list[dict[str, str]]] = {}
    for table_name in sorted(table_context):
        table_columns_cache[table_name] = _describe_table_columns(
            sql_client=sql_client,
            catalog=resolved_catalog,
            schema=schema,
            table_name=table_name,
        )

    detector_results: list[DetectorResult] = []
    for rule in rules:
        detector_results.append(
            _run_data_detector(
                rule=rule,
                sql_client=sql_client,
                catalog=resolved_catalog,
                schema=schema,
                available_tables=available_tables,
                table_columns_cache=table_columns_cache,
                thresholds=thresholds,
            )
        )

    for table_name in sorted(table_context):
        detector_results.append(
            _run_schema_detector(
                sql_client=sql_client,
                settings=settings,
                catalog=resolved_catalog,
                schema=schema,
                table_name=table_name,
                table_columns=table_columns_cache.get(table_name, []),
                rescued_candidates=table_context[table_name]["rescued_candidates"],
                date_candidates=table_context[table_name]["date_candidates"],
            )
        )

    total_anomalies = sum(item.anomaly_count for item in detector_results)
    data_anomalies = sum(item.anomaly_count for item in detector_results if item.category == "data")
    schema_anomalies = sum(item.anomaly_count for item in detector_results if item.category == "schema")
    checks_with_issues = sum(1 for item in detector_results if item.status == "anomaly")

    output_dir = anomaly_output_dir(resolved_run_id)
    report_path = output_dir / "anomalies.txt"
    json_path = anomaly_json_path(resolved_run_id)

    report_text = _render_report(
        run_id=resolved_run_id,
        catalog=resolved_catalog,
        schema=schema,
        detector_results=detector_results,
    )
    report_payload = _build_report_payload(
        run_id=resolved_run_id,
        catalog=resolved_catalog,
        schema=schema,
        detector_results=detector_results,
    )
    report_payload["report_text"] = report_text
    atomic_write_text(report_path, report_text + "\n")
    atomic_write_json(json_path, report_payload)

    return AnomalyDetectionOutcome(
        run_id=resolved_run_id,
        total_anomalies=total_anomalies,
        data_anomalies=data_anomalies,
        schema_anomalies=schema_anomalies,
        checks_run=len(detector_results),
        checks_with_issues=checks_with_issues,
        output_dir=str(output_dir),
        report_path=str(report_path),
        json_path=str(json_path),
        detector_results=detector_results,
    )


def load_anomaly_rules(logger: Any | None = None) -> list[DataAnomalyRule]:
    """Load anomaly detector rules from JSON."""
    if not ANOMALY_RULES_FILE.exists():
        return _default_rules()

    try:
        payload = json.loads(ANOMALY_RULES_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        if logger:
            logger.warning("Failed to load anomaly_rules.json, using defaults: %s", exc)
        return _default_rules()

    if not isinstance(payload, list):
        if logger:
            logger.warning("anomaly_rules.json must be a JSON array, using defaults.")
        return _default_rules()

    rules: list[DataAnomalyRule] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        try:
            rules.append(_build_rule(item))
        except Exception as exc:
            if logger:
                logger.warning("Skipping invalid anomaly rule %s: %s", item.get("detector", "<unknown>"), exc)

    return [rule for rule in rules if rule.enabled] or _default_rules()


def _default_rules() -> list[DataAnomalyRule]:
    """Fallback rules used when no config is provided."""
    return [
        _build_rule(
            {
                "detector": "crm_account_onboarding_volume",
                "display_name": "CRM | Account Onboarding Volume",
                "table_candidates": ["account"],
                "date_column_candidates": ["createddate", "lastmodifieddate", "systemmodstamp"],
                "metric_expr": "COUNT(*)",
                "metric_label": "New Accounts",
                "business_summary": "Tracks the weekly number of new CRM accounts created.",
                "where_clause": "COALESCE(TRY_CAST(`isdeleted` AS BOOLEAN), FALSE) = FALSE",
                "filter_note": "Deleted CRM accounts are excluded.",
                "granularities": ["week"],
                "lookback_windows": {"week": 26},
            }
        ),
        _build_rule(
            {
                "detector": "crm_event_attendance_fill_rate",
                "display_name": "CRM | Event Attendance Fill Rate",
                "table_candidates": ["em_event_vod__c"],
                "date_column_candidates": ["start_time_vod__c", "end_time_vod__c", "createddate"],
                "metric_expr": (
                    "100.0 * SUM(CASE WHEN TRY_CAST(`estimated_attendance_vod__c` AS DOUBLE) > 0 "
                    "THEN COALESCE(TRY_CAST(`actual_attendance_vod__c` AS DOUBLE), 0.0) ELSE 0.0 END) / "
                    "NULLIF(SUM(CASE WHEN TRY_CAST(`estimated_attendance_vod__c` AS DOUBLE) > 0 "
                    "THEN TRY_CAST(`estimated_attendance_vod__c` AS DOUBLE) ELSE 0.0 END), 0.0)"
                ),
                "metric_label": "Attendance Fill Rate (%)",
                "business_summary": "Tracks how full CRM events were compared with the expected attendee count.",
                "where_clause": (
                    "TO_DATE(`start_time_vod__c`) <= CURRENT_DATE() AND "
                    "COALESCE(TRY_CAST(`isdeleted` AS BOOLEAN), FALSE) = FALSE AND "
                    "COALESCE(`status_vod__c`, '') IN ('Closed_vod', 'Completed')"
                ),
                "filter_note": "Only closed or completed events up to today are included.",
                "granularities": ["month"],
                "lookback_windows": {"month": 12},
            }
        ),
        _build_rule(
            {
                "detector": "crm_urgent_suggestion_share",
                "display_name": "CRM | Urgent Suggestion Share",
                "table_candidates": ["suggestion_vod__c"],
                "date_column_candidates": ["posted_date_vod__c", "createddate"],
                "metric_expr": "100.0 * AVG(CASE WHEN `priority_vod__c` = 'Urgent_vod' THEN 1.0 ELSE 0.0 END)",
                "metric_label": "Urgent Suggestion Share (%)",
                "business_summary": "Tracks the weekly share of suggestions marked as urgent.",
                "where_clause": "COALESCE(TRY_CAST(`isdeleted` AS BOOLEAN), FALSE) = FALSE",
                "filter_note": "Deleted suggestions are excluded.",
                "granularities": ["week"],
                "lookback_windows": {"week": 26},
            }
        ),
        _build_rule(
            {
                "detector": "sales_execution_feedback_volume",
                "display_name": "Sales | Execution Feedback Volume",
                "table_candidates": ["suggestion_feedback_vod__c"],
                "date_column_candidates": ["createddate", "lastmodifieddate", "systemmodstamp"],
                "metric_expr": "COUNT(*)",
                "metric_label": "Execution Feedback Records",
                "business_summary": "Tracks the weekly number of sales execution feedback records entered by reps.",
                "where_clause": "COALESCE(TRY_CAST(`isdeleted` AS BOOLEAN), FALSE) = FALSE",
                "filter_note": "Deleted feedback records are excluded.",
                "granularities": ["week"],
                "lookback_windows": {"week": 26},
            }
        ),
        _build_rule(
            {
                "detector": "sales_field_force_time_off_volume",
                "display_name": "Sales | Field Force Time Off Volume",
                "table_candidates": ["time_off_territory_vod__c"],
                "date_column_candidates": ["date_vod__c", "createddate"],
                "metric_expr": "COUNT(*)",
                "metric_label": "Time Off Entries",
                "business_summary": "Tracks the weekly number of approved field force time-off entries.",
                "where_clause": (
                    "`date_vod__c` <= CURRENT_DATE() AND "
                    "COALESCE(TRY_CAST(`isdeleted` AS BOOLEAN), FALSE) = FALSE AND "
                    "`status_vod__c` = 'Approved'"
                ),
                "filter_note": "Only approved time-off entries up to today are included.",
                "granularities": ["week"],
                "lookback_windows": {"week": 26},
            }
        ),
    ]


def _build_rule(payload: dict[str, Any]) -> DataAnomalyRule:
    """Normalize one rule payload into a dataclass."""
    table_candidates = _listify(payload.get("table_candidates"))
    if payload.get("table_name"):
        table_candidates.insert(0, str(payload["table_name"]))

    granularities = [
        item for item in _listify(payload.get("granularities")) or ["month", "week", "day"]
        if item in GRANULARITY_CONFIG
    ]

    lookback_windows_raw = payload.get("lookback_windows") or {}
    lookback_windows: dict[str, int | None] = {}
    if isinstance(lookback_windows_raw, dict):
        for key, value in lookback_windows_raw.items():
            if key not in GRANULARITY_CONFIG:
                continue
            if value in (None, "", 0):
                lookback_windows[key] = None
            else:
                lookback_windows[key] = int(value)

    return DataAnomalyRule(
        detector=str(payload["detector"]).strip(),
        display_name=str(payload.get("display_name") or payload["detector"]).strip(),
        table_candidates=[item.strip().lower() for item in table_candidates if item.strip()],
        date_column_candidates=[item.strip().lower() for item in _listify(payload.get("date_column_candidates")) if item.strip()],
        metric_expr=str(payload.get("metric_expr") or "COUNT(*)").strip(),
        metric_label=str(payload.get("metric_label") or "Metric Value").strip(),
        group_column_candidates=[item.strip().lower() for item in _listify(payload.get("group_column_candidates")) if item.strip()],
        rescued_column_candidates=[
            item.strip().lower()
            for item in (_listify(payload.get("rescued_column_candidates")) or list(DEFAULT_RESCUED_COLUMNS))
            if item.strip()
        ],
        granularities=granularities or ["month", "week", "day"],
        lookback_windows=lookback_windows,
        business_summary=str(payload.get("business_summary")).strip() if payload.get("business_summary") else None,
        where_clause=str(payload.get("where_clause")).strip() if payload.get("where_clause") else None,
        filter_note=str(payload.get("filter_note")).strip() if payload.get("filter_note") else None,
        schema_monitoring=bool(payload.get("schema_monitoring", True)),
        enabled=bool(payload.get("enabled", True)),
    )


def _run_data_detector(
    rule: DataAnomalyRule,
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    available_tables: set[str],
    table_columns_cache: dict[str, list[dict[str, str]]],
    thresholds: dict[str, Any],
) -> DetectorResult:
    """Run one configurable data anomaly detector."""
    table_name = _resolve_table_name(available_tables, rule.table_candidates)
    if not table_name:
        return _skipped_result(
            category="data",
            detector=rule.detector,
            display_name=rule.display_name,
            catalog=catalog,
            schema=schema,
            table_name=rule.table_candidates[0] if rule.table_candidates else "unknown_table",
            note="Configured table was not found in the selected schema.",
        )

    table_columns = table_columns_cache.get(table_name, [])
    column_names = {item["name"] for item in table_columns}
    date_column = _pick_column(column_names, rule.date_column_candidates)
    if not date_column:
        return _skipped_result(
            category="data",
            detector=rule.detector,
            display_name=rule.display_name,
            catalog=catalog,
            schema=schema,
            table_name=table_name,
            note=f"No matching date column found. Checked: {', '.join(rule.date_column_candidates) or '-'}",
        )

    group_column = _pick_column(column_names, rule.group_column_candidates)
    table_thresholds = thresholds.get(table_name, {})

    monthly_anomalies: list[AnomalyFinding] = []
    weekly_anomalies: list[AnomalyFinding] = []
    daily_anomalies: list[AnomalyFinding] = []
    findings: list[str] = []
    series_points = 0
    empty_granularities: list[str] = []

    for granularity in rule.granularities:
        config = GRANULARITY_CONFIG[granularity]
        series = _fetch_aggregated_series(
            sql_client=sql_client,
            catalog=catalog,
            schema=schema,
            table_name=table_name,
            date_column=date_column,
            metric_expr=rule.metric_expr,
            granularity=granularity,
            group_column=group_column,
            where_clause=rule.where_clause,
        )
        if not series:
            empty_granularities.append(granularity)
            continue
        series_points += len(series)
        anomalies = _detect_series_anomalies(
            series=series,
            granularity=granularity,
            lookback_window=rule.lookback_windows.get(granularity),
            min_window=int(config["min_window"]),
            table_thresholds=table_thresholds,
        )
        for anomaly in anomalies:
            findings.append(_format_data_finding(granularity, anomaly))
        if granularity == "month":
            monthly_anomalies.extend(anomalies)
        elif granularity == "week":
            weekly_anomalies.extend(anomalies)
        else:
            daily_anomalies.extend(anomalies)

    if series_points == 0:
        return _skipped_result(
            category="data",
            detector=rule.detector,
            display_name=rule.display_name,
            catalog=catalog,
            schema=schema,
            table_name=table_name,
            note=(
                f"Date column `{date_column}` exists, but no parsable date values were found for "
                f"the configured granularity ({', '.join(rule.granularities)})."
            ),
        )

    notes = [
        f"Date column: {date_column}",
        f"Metric: {rule.metric_label}",
        "Threshold: P1/P99 against prior-month training data; optional lookback can be configured in anomaly_rules.json.",
    ]
    if rule.business_summary:
        notes.insert(0, f"What this tracks: {rule.business_summary}")
    if rule.filter_note:
        notes.append(rule.filter_note)
    if empty_granularities:
        notes.append(f"No usable rows were found for: {', '.join(empty_granularities)}.")
    if rule.group_column_candidates and not group_column:
        notes.append("Configured group column was not found, so the detector ran at overall-table level.")
    elif group_column:
        notes.append(f"Grouped by: {group_column}")

    return DetectorResult(
        category="data",
        detector=rule.detector,
        display_name=rule.display_name,
        table_name=table_name,
        table_fqn=_fqn(catalog, schema, table_name),
        threshold="Values below P1 or above P99 versus prior-month training data",
        status="anomaly" if findings else "ok",
        anomaly_count=len(findings),
        findings=findings,
        notes=notes,
        monthly_anomalies=monthly_anomalies,
        weekly_anomalies=weekly_anomalies,
        daily_anomalies=daily_anomalies,
    )


def _run_schema_detector(
    sql_client: DatabricksSQLClient,
    settings: Settings,
    catalog: str,
    schema: str,
    table_name: str,
    table_columns: list[dict[str, str]],
    rescued_candidates: list[str],
    date_candidates: list[str],
) -> DetectorResult:
    """Run schema drift checks for one table."""
    current_schema = {item["name"]: item["data_type"] for item in table_columns}
    notes: list[str] = []
    schema_findings: list[SchemaFinding] = []
    snapshot_file = schema_snapshot_path(catalog, schema, table_name)
    previous_schema = _load_schema_snapshot(snapshot_file)

    if previous_schema:
        schema_findings.extend(_diff_schema_columns(previous_schema, current_schema))
    else:
        notes.append("Baseline schema snapshot created for future comparisons.")

    column_names = set(current_schema)
    rescue_column = _pick_column(column_names, rescued_candidates or list(DEFAULT_RESCUED_COLUMNS))
    date_column = _pick_column(column_names, date_candidates)

    if rescue_column:
        notes.append(f"Rescue column checked: {rescue_column}")
        schema_findings.extend(
            _inspect_rescued_data(
                sql_client=sql_client,
                table_fqn=_fqn(catalog, schema, table_name),
                rescue_column=rescue_column,
                date_column=date_column,
                current_schema=current_schema,
                row_limit=settings.ANOMALY_RESCUED_ROW_LIMIT,
            )
        )
    else:
        notes.append("No rescue column found, so rescue-based schema checks were skipped.")

    _save_schema_snapshot(snapshot_file, catalog=catalog, schema=schema, table_name=table_name, current_schema=current_schema)

    return DetectorResult(
        category="schema",
        detector=f"schema_monitor_{table_name}",
        display_name="Schema Drift Monitor",
        table_name=table_name,
        table_fqn=_fqn(catalog, schema, table_name),
        threshold="Schema snapshot diff plus rescue-column inspection",
        status="anomaly" if schema_findings else "ok",
        anomaly_count=len(schema_findings),
        findings=[item.details for item in schema_findings],
        notes=notes,
        schema_findings=schema_findings,
    )


def _fetch_aggregated_series(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    table_name: str,
    date_column: str,
    metric_expr: str,
    granularity: str,
    group_column: str | None = None,
    where_clause: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch grouped metric values with one query per table/granularity."""
    config = GRANULARITY_CONFIG[granularity]
    date_expr = config["period_expr"].format(date_col=_qid(date_column))

    if group_column:
        group_select = f"CAST({_qid(group_column)} AS STRING) AS group_label,"
        group_by = f"CAST({_qid(group_column)} AS STRING), {date_expr}"
        non_null_group = f"AND {_qid(group_column)} IS NOT NULL"
    else:
        group_select = "'ALL' AS group_label,"
        group_by = date_expr
        non_null_group = ""
    extra_filter = f"AND ({where_clause})" if where_clause else ""

    query = f"""
    SELECT
        CAST({date_expr} AS STRING) AS period,
        {group_select}
        {metric_expr} AS metric_value
    FROM {_fqn(catalog, schema, table_name)}
    WHERE {date_expr} IS NOT NULL
      {non_null_group}
      {extra_filter}
    GROUP BY {group_by}
    ORDER BY group_label, period
    """

    series: list[dict[str, Any]] = []
    for row in sql_client.fetch_all(query):
        period = str(row.get("period", "")).strip()
        value = _to_float(row.get("metric_value"))
        if not period or value is None:
            continue
        series.append(
            {
                "period": period[:10],
                "group_label": str(row.get("group_label", "ALL") or "ALL"),
                "metric_value": value,
            }
        )
    return series


def _detect_series_anomalies(
    series: list[dict[str, Any]],
    granularity: str,
    lookback_window: int | None,
    min_window: int,
    table_thresholds: dict[str, Any] | None = None,
) -> list[AnomalyFinding]:
    """Detect anomalies from pre-aggregated period series."""
    grouped_rows: dict[str, list[tuple[date, str, float]]] = defaultdict(list)
    for row in series:
        period_date = _parse_iso_date(str(row["period"]))
        if period_date is None:
            continue
        grouped_rows[str(row.get("group_label") or "ALL")].append(
            (period_date, str(row["period"]), float(row["metric_value"]))
        )

    table_thresholds = table_thresholds or {}
    custom_min = _to_float(table_thresholds.get("min_val"))
    custom_max = _to_float(table_thresholds.get("max_val"))
    results: list[AnomalyFinding] = []

    for group_label, rows in grouped_rows.items():
        rows.sort(key=lambda item: item[0])
        rows_by_month: dict[date, list[tuple[date, str, float]]] = defaultdict(list)
        for item in rows:
            month_key = date(item[0].year, item[0].month, 1)
            rows_by_month[month_key].append(item)

        prior_rows: list[tuple[date, str, float]] = []
        for month_key in sorted(rows_by_month):
            training_rows = prior_rows[-lookback_window:] if lookback_window else prior_rows
            training_values = [item[2] for item in training_rows]
            if len(training_values) >= min_window:
                lower_bound = _percentile_cont(training_values, 0.01)
                upper_bound = _percentile_cont(training_values, 0.99)
                if custom_min is not None:
                    lower_bound = min(lower_bound, custom_min)
                if custom_max is not None:
                    upper_bound = max(upper_bound, custom_max)

                for period_date, period_text, actual_value in rows_by_month[month_key]:
                    if lower_bound <= actual_value <= upper_bound:
                        continue
                    results.append(
                        AnomalyFinding(
                            period=period_text,
                            actual_value=actual_value,
                            lower_bound=lower_bound,
                            upper_bound=upper_bound,
                            direction=(
                                "Higher value than expected range"
                                if actual_value > upper_bound
                                else "Lower value than expected range"
                            ),
                            group_label=None if group_label == "ALL" else group_label,
                        )
                    )
            prior_rows.extend(rows_by_month[month_key])

    results.sort(key=lambda item: (item.group_label or "", item.period))
    return results


def _diff_schema_columns(previous_schema: dict[str, str], current_schema: dict[str, str]) -> list[SchemaFinding]:
    """Diff previous and current schema snapshots."""
    findings: list[SchemaFinding] = []

    for column_name in sorted(current_schema):
        if column_name not in previous_schema:
            findings.append(
                SchemaFinding(
                    finding_type="column_added",
                    column_name=column_name,
                    current_type=current_schema[column_name],
                    details=f"Column added: `{column_name}` is now present with type `{current_schema[column_name]}`.",
                )
            )
            continue

        previous_type = _normalize_type(previous_schema[column_name])
        current_type = _normalize_type(current_schema[column_name])
        if previous_type != current_type:
            findings.append(
                SchemaFinding(
                    finding_type="column_type_changed",
                    column_name=column_name,
                    previous_type=previous_schema[column_name],
                    current_type=current_schema[column_name],
                    details=(
                        f"Column type changed: `{column_name}` moved from "
                        f"`{previous_schema[column_name]}` to `{current_schema[column_name]}`."
                    ),
                )
            )

    for column_name in sorted(previous_schema):
        if column_name not in current_schema:
            findings.append(
                SchemaFinding(
                    finding_type="column_removed",
                    column_name=column_name,
                    previous_type=previous_schema[column_name],
                    details=f"Column removed: `{column_name}` existed earlier as `{previous_schema[column_name]}` but is missing now.",
                )
            )

    return findings


def _inspect_rescued_data(
    sql_client: DatabricksSQLClient,
    table_fqn: str,
    rescue_column: str,
    date_column: str | None,
    current_schema: dict[str, str],
    row_limit: int,
) -> list[SchemaFinding]:
    """Inspect rescue payloads for hidden schema drift and type drift."""
    where_clauses = [
        f"{_qid(rescue_column)} IS NOT NULL",
        f"TRIM(CAST({_qid(rescue_column)} AS STRING)) <> ''",
    ]
    order_clause = ""
    if date_column:
        order_clause = f"ORDER BY TO_DATE({_qid(date_column)}) DESC"

    query = f"""
    SELECT CAST({_qid(rescue_column)} AS STRING) AS rescued_payload
    FROM {table_fqn}
    WHERE {' AND '.join(where_clauses)}
    {order_clause}
    LIMIT {int(row_limit)}
    """

    samples_by_key: dict[str, list[str]] = defaultdict(list)
    counts_by_key: dict[str, int] = defaultdict(int)
    for row in sql_client.fetch_all(query):
        payload = _parse_rescued_payload(str(row.get("rescued_payload") or ""))
        if not payload:
            continue
        for raw_key, raw_value in payload.items():
            key = str(raw_key).strip().lower()
            if not key:
                continue
            counts_by_key[key] += 1
            sample = _stringify_value(raw_value)
            if sample and sample not in samples_by_key[key] and len(samples_by_key[key]) < 3:
                samples_by_key[key].append(sample)

    findings: list[SchemaFinding] = []
    for column_name in sorted(counts_by_key):
        samples = samples_by_key.get(column_name, [])
        if column_name not in current_schema:
            findings.append(
                SchemaFinding(
                    finding_type="rescued_column_detected",
                    column_name=column_name,
                    occurrence_count=counts_by_key[column_name],
                    sample_values=samples,
                    details=(
                        f"Unexpected incoming column detected in rescue data: `{column_name}` appeared "
                        f"{counts_by_key[column_name]} time(s). Samples: {', '.join(samples) or '-'}."
                    ),
                )
            )
            continue

        observed_type = _infer_observed_type(samples)
        if _types_look_incompatible(current_schema[column_name], observed_type):
            findings.append(
                SchemaFinding(
                    finding_type="rescued_type_change_suspected",
                    column_name=column_name,
                    current_type=current_schema[column_name],
                    observed_type=observed_type,
                    occurrence_count=counts_by_key[column_name],
                    sample_values=samples,
                    details=(
                        f"Possible type drift detected for `{column_name}`. Current schema expects "
                        f"`{current_schema[column_name]}`, but rescued values look like `{observed_type}`. "
                        f"Samples: {', '.join(samples) or '-'}."
                    ),
                )
            )

    return findings


def _build_table_context(
    rules: list[DataAnomalyRule],
    available_tables: set[str],
) -> dict[str, dict[str, list[str]]]:
    """Build per-table context used by schema monitoring."""
    context: dict[str, dict[str, set[str]]] = {}
    for rule in rules:
        if not rule.schema_monitoring:
            continue
        table_name = _resolve_table_name(available_tables, rule.table_candidates)
        if not table_name:
            continue
        entry = context.setdefault(
            table_name,
            {
                "rescued_candidates": set(DEFAULT_RESCUED_COLUMNS),
                "date_candidates": set(),
            },
        )
        entry["rescued_candidates"].update(rule.rescued_column_candidates)
        entry["date_candidates"].update(rule.date_column_candidates)

    if not context:
        for table_name in sorted(available_tables):
            context[table_name] = {
                "rescued_candidates": set(DEFAULT_RESCUED_COLUMNS),
                "date_candidates": set(),
            }

    return {
        table_name: {
            "rescued_candidates": sorted(values["rescued_candidates"]),
            "date_candidates": sorted(values["date_candidates"]),
        }
        for table_name, values in context.items()
    }


def _describe_table_columns(
    sql_client: DatabricksSQLClient,
    catalog: str,
    schema: str,
    table_name: str,
) -> list[dict[str, str]]:
    """Read table columns using DESCRIBE TABLE."""
    rows = sql_client.fetch_all(f"DESCRIBE TABLE {_fqn(catalog, schema, table_name)}")
    columns: list[dict[str, str]] = []
    for row in rows:
        col_name = str(row.get("col_name", "") or "").strip()
        data_type = str(row.get("data_type", "") or "").strip()
        if not col_name:
            break
        if col_name.startswith("#"):
            break
        columns.append({"name": col_name.lower(), "data_type": data_type})
    return columns


def _load_schema_snapshot(path: Path) -> dict[str, str]:
    """Load the latest schema snapshot from disk."""
    if not path.exists():
        return {}
    payload = read_json(path)
    columns = payload.get("columns", []) if isinstance(payload, dict) else []
    result: dict[str, str] = {}
    if isinstance(columns, list):
        for item in columns:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip().lower()
            data_type = str(item.get("data_type") or "").strip()
            if name:
                result[name] = data_type
    return result


def _save_schema_snapshot(
    path: Path,
    catalog: str,
    schema: str,
    table_name: str,
    current_schema: dict[str, str],
) -> None:
    """Persist the latest schema snapshot for future comparisons."""
    payload = {
        "captured_at": utc_iso(),
        "catalog": catalog,
        "schema": schema,
        "table": table_name,
        "columns": [
            {"name": column_name, "data_type": current_schema[column_name]}
            for column_name in sorted(current_schema)
        ],
    }
    atomic_write_json(path, payload)


def _build_report_payload(
    run_id: str,
    catalog: str,
    schema: str,
    detector_results: list[DetectorResult],
) -> dict[str, Any]:
    """Build the structured JSON payload written alongside the text report."""
    return {
        "run_id": run_id,
        "generated_at": utc_iso(),
        "data_source": f"{catalog}.{schema}",
        "checks_run": len(detector_results),
        "checks_with_issues": sum(1 for item in detector_results if item.status == "anomaly"),
        "total_anomalies": sum(item.anomaly_count for item in detector_results),
        "data_anomalies": sum(item.anomaly_count for item in detector_results if item.category == "data"),
        "schema_anomalies": sum(item.anomaly_count for item in detector_results if item.category == "schema"),
        "detectors": [asdict(item) for item in detector_results],
    }


def _render_report(
    run_id: str,
    catalog: str,
    schema: str,
    detector_results: list[DetectorResult],
) -> str:
    """Render a human-readable text report."""
    total_anomalies = sum(item.anomaly_count for item in detector_results)
    data_anomalies = sum(item.anomaly_count for item in detector_results if item.category == "data")
    schema_anomalies = sum(item.anomaly_count for item in detector_results if item.category == "schema")
    checks_with_issues = sum(1 for item in detector_results if item.status == "anomaly")

    lines: list[str] = []
    lines.append("=" * 70)
    lines.append("          DATA AND SCHEMA ANOMALY REPORT")
    lines.append("=" * 70)
    lines.append("")
    lines.append(f"  Report ID          : {run_id}")
    lines.append(f"  Generated          : {utc_iso()}")
    lines.append(f"  Data Source        : {catalog}.{schema}")
    lines.append(f"  Checks Run         : {len(detector_results)}")
    lines.append(f"  Checks with Issues : {checks_with_issues}")
    lines.append(f"  Total Anomalies    : {total_anomalies}")
    lines.append(f"  Data Anomalies     : {data_anomalies}")
    lines.append(f"  Schema Anomalies   : {schema_anomalies}")
    lines.append("")
    lines.append("-" * 70)

    for category in ("data", "schema"):
        category_results = [item for item in detector_results if item.category == category]
        if not category_results:
            continue

        lines.append("")
        lines.append(f"  {category.upper()} CHECKS")
        lines.append("  " + "-" * 64)

        for result in category_results:
            status_label = "Issues Detected" if result.status == "anomaly" else (
                "Skipped" if result.status == "skipped" else (
                    "Error" if result.status == "error" else "No Issues"
                )
            )
            lines.append("")
            lines.append(f"  CHECK: {result.display_name}")
            lines.append(f"  Category: {result.category.title()}")
            lines.append(f"  Table: {result.table_fqn.replace('`', '')}")
            lines.append(
                f"  Result: {status_label} ({result.anomaly_count} finding{'s' if result.anomaly_count != 1 else ''})"
            )
            lines.append("")

            if result.notes:
                lines.append("    Notes:")
                for note in result.notes:
                    lines.append(f"    - {note}")
                lines.append("")

            if result.category == "data":
                if result.monthly_anomalies:
                    _render_anomaly_table(lines, "month", result.monthly_anomalies)
                if result.weekly_anomalies:
                    _render_anomaly_table(lines, "week", result.weekly_anomalies)
                if result.daily_anomalies:
                    _render_anomaly_table(lines, "day", result.daily_anomalies)
                if not result.findings and result.status == "ok":
                    lines.append("    All values are within the expected range. No data anomalies found.")
                    lines.append("")
            else:
                if result.schema_findings:
                    lines.append("    Schema Findings:")
                    lines.append("")
                    for index, finding in enumerate(result.schema_findings, start=1):
                        lines.append(f"    {index}. {finding.details}")
                    lines.append("")
                elif result.status == "ok":
                    lines.append("    No schema anomalies found.")
                    lines.append("")

            lines.append("-" * 70)

    lines.append("")
    lines.append("=" * 70)
    lines.append("  END OF REPORT")
    lines.append("=" * 70)
    return "\n".join(lines)


def _render_anomaly_table(lines: list[str], granularity: str, anomalies: list[AnomalyFinding]) -> None:
    """Render a formatted anomaly table for one granularity."""
    config = GRANULARITY_CONFIG[granularity]
    title = str(config["title"])
    period_len = int(config["period_len"])

    lines.append(f"    {title}")
    lines.append("    " + "-" * 62)
    has_group = any(item.group_label for item in anomalies)
    if has_group:
        lines.append(f"    {'Period':<12} {'Group':<20} {'Value':>12} {'Expected Range':>22} {'Status':<16}")
    else:
        lines.append(f"    {'Period':<12} {'Value':>14} {'Expected Range':>22} {'Status':<16}")
    lines.append("    " + "-" * 62)
    for item in anomalies:
        period = item.period[:period_len]
        expected = f"{_fmt_num(item.lower_bound)} - {_fmt_num(item.upper_bound)}"
        if has_group:
            lines.append(
                f"    {period:<12} {(item.group_label or ''):<20} {_fmt_num(item.actual_value):>12} "
                f"{expected:>22} {item.direction:<16}"
            )
        else:
            lines.append(
                f"    {period:<12} {_fmt_num(item.actual_value):>14} {expected:>22} {item.direction:<16}"
            )
    lines.append("    " + "-" * 62)
    lines.append("")


def _format_data_finding(granularity: str, finding: AnomalyFinding) -> str:
    """Format one data anomaly as a single summary line."""
    label = GRANULARITY_CONFIG[granularity]["label"]
    period = finding.period[: int(GRANULARITY_CONFIG[granularity]["period_len"])]
    group_suffix = f" [{finding.group_label}]" if finding.group_label else ""
    return (
        f"{label} {period}{group_suffix}: {finding.direction} "
        f"(value={_fmt_num(finding.actual_value)}, range={_fmt_num(finding.lower_bound)}-{_fmt_num(finding.upper_bound)})"
    )


def _skipped_result(
    category: str,
    detector: str,
    display_name: str,
    catalog: str,
    schema: str,
    table_name: str,
    note: str,
) -> DetectorResult:
    """Build a skipped detector result."""
    return DetectorResult(
        category=category,
        detector=detector,
        display_name=display_name,
        table_name=table_name,
        table_fqn=_fqn(catalog, schema, table_name),
        threshold="-",
        status="skipped",
        anomaly_count=0,
        findings=[],
        notes=[note],
    )


def _list_tables(sql_client: DatabricksSQLClient, catalog: str, schema: str) -> set[str]:
    """List available table names in a schema."""
    rows = sql_client.fetch_all(f"SHOW TABLES IN {_qid(catalog)}.{_qid(schema)}")
    return {
        str(row.get("tablename") or row.get("table_name") or row.get("table") or "").strip().lower()
        for row in rows
        if (row.get("tablename") or row.get("table_name") or row.get("table"))
    }


def _resolve_table_name(available_tables: set[str], candidates: list[str]) -> str | None:
    """Pick the first matching table candidate."""
    for candidate in candidates:
        if candidate.lower() in available_tables:
            return candidate.lower()
    return None


def _pick_column(columns: set[str], candidates: list[str]) -> str | None:
    """Pick the first matching column candidate."""
    lowered = {item.lower() for item in columns}
    for candidate in candidates:
        if candidate.lower() in lowered:
            return candidate.lower()
    return None


def _percentile_cont(values: list[float], quantile: float) -> float:
    """Continuous percentile equivalent to SQL percentile_cont."""
    ordered = sorted(float(item) for item in values)
    if not ordered:
        raise ValueError("values must not be empty")
    if len(ordered) == 1:
        return ordered[0]

    position = quantile * (len(ordered) - 1)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    if lower_index == upper_index:
        return lower_value
    weight = position - lower_index
    return lower_value + (upper_value - lower_value) * weight


def _parse_rescued_payload(raw_payload: str) -> dict[str, Any]:
    """Parse an autoloader rescue payload into a dictionary when possible."""
    payload = raw_payload.strip()
    if not payload or payload in {"{}", "null", "NULL"}:
        return {}

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return {}

    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            return {}

    return parsed if isinstance(parsed, dict) else {}


def _infer_observed_type(samples: list[str]) -> str:
    """Infer a coarse logical type from rescued value samples."""
    if not samples:
        return "unknown"
    observed = {_classify_literal(item) for item in samples if item}
    if not observed:
        return "unknown"
    if observed <= {"integer", "float"}:
        return "numeric"
    if len(observed) == 1:
        return next(iter(observed))
    return "/".join(sorted(observed))


def _classify_literal(value: str) -> str:
    """Classify one string literal into a coarse type bucket."""
    lowered = value.strip().lower()
    if lowered in BOOL_TRUE_VALUES | BOOL_FALSE_VALUES:
        return "boolean"
    if re.fullmatch(r"-?\d+", lowered):
        return "integer"
    if re.fullmatch(r"-?\d+\.\d+", lowered):
        return "float"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", lowered):
        return "date"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}[ t]\d{2}:\d{2}:\d{2}.*", lowered):
        return "timestamp"
    return "string"


def _types_look_incompatible(expected_type: str, observed_type: str) -> bool:
    """Return True when rescued values strongly suggest a type drift."""
    normalized = _normalize_type(expected_type)
    if any(token in normalized for token in NUMERIC_TYPE_TOKENS):
        return observed_type in {"boolean", "string", "boolean/string", "date", "timestamp"}
    if "boolean" in normalized:
        return observed_type not in {"boolean", "unknown"}
    if any(token in normalized for token in DATE_TYPE_TOKENS):
        return observed_type in {"numeric", "integer", "float", "string", "boolean"}
    return False


def _parse_iso_date(value: str) -> date | None:
    """Parse YYYY-MM-DD strings into date objects."""
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _stringify_value(value: Any) -> str:
    """Render a rescued JSON value as a short sample string."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()[:60]
    return json.dumps(value, ensure_ascii=True)[:60]


def _normalize_type(data_type: str) -> str:
    """Normalize a SQL type string for comparison."""
    return re.sub(r"\s+", " ", data_type.strip().lower())


def _listify(value: Any) -> list[str]:
    """Convert an arbitrary value to a list of strings."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    return [str(value)]


def _fqn(catalog: str, schema: str, table: str) -> str:
    """Build a quoted fully qualified table name."""
    return f"{_qid(catalog)}.{_qid(schema)}.{_qid(table)}"


def _qid(identifier: str) -> str:
    """Quote an identifier for Databricks SQL."""
    return f"`{identifier.replace('`', '``')}`"


def _to_float(value: Any) -> float | None:
    """Safely coerce any scalar value to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_num(value: float | None) -> str:
    """Render numbers for reports."""
    if value is None:
        return "N/A"
    return f"{value:,.2f}"
