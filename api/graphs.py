"""Minimal anomaly execution wrapper used by the FastAPI server."""

from __future__ import annotations

import logging
from typing import Any, TypedDict

from src.config.settings import load_settings_or_raise
from src.utils.io import read_json
from src.utils.logging import configure_logging
from src.utils.time import new_run_id
from src_anomaly.pipeline import run_bronze_anomaly_detection


class AnomalyState(TypedDict, total=False):
    """State for anomaly detection."""

    schema: str
    run_id: str
    total_anomalies: int
    data_anomalies: int
    schema_anomalies: int
    checks_run: int
    checks_with_issues: int
    report_text: str
    report_data: dict[str, Any]
    error: str


def anomaly_node(state: AnomalyState) -> dict[str, Any]:
    """Run the anomaly detection pipeline and return text plus structured output."""
    logger = logging.getLogger("schema_maker.graph")
    logger.info("[anomaly] Node started", extra={"log_module": "anomaly", "step": "start"})
    try:
        settings = load_settings_or_raise()
        schema = state.get("schema", settings.DATABRICKS_SCHEMA_DOMAIN or "bronze")
        run_id = new_run_id()
        pipe_logger = configure_logging(run_id=run_id)

        outcome = run_bronze_anomaly_detection(
            settings=settings,
            run_id=run_id,
            catalog=settings.DATABRICKS_CATALOG,
            schema=schema,
            logger=pipe_logger,
        )
        report_data = read_json(outcome.json_path)
        return {
            "run_id": outcome.run_id,
            "total_anomalies": outcome.total_anomalies,
            "data_anomalies": outcome.data_anomalies,
            "schema_anomalies": outcome.schema_anomalies,
            "checks_run": outcome.checks_run,
            "checks_with_issues": outcome.checks_with_issues,
            "report_text": str(report_data.get("report_text", "")),
            "report_data": report_data,
        }
    except Exception as exc:
        logger.exception("[anomaly] Node failed: %s", exc, extra={"log_module": "anomaly", "step": "error"})
        return {"error": str(exc)}


class DirectAnomalyGraph:
    """Small invoke-compatible wrapper for the existing API contract."""

    def invoke(self, state: AnomalyState | None = None) -> dict[str, Any]:
        return anomaly_node(state or {})


anomaly_graph = DirectAnomalyGraph()
