"""FastAPI server exposing anomaly detection endpoints."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import json
import logging
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from api.graphs import anomaly_graph

logger = logging.getLogger("schema_maker.server")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
OUTPUT_DIR = PROJECT_ROOT / "Output"

# In-memory store for the latest anomaly report (essential for Render free tier)
_LATEST_REPORT: dict | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Schema Maker API starting up")
    yield
    logger.info("Schema Maker API shutting down")


app = FastAPI(
    title="Nexora Anomaly Monitor API",
    description="REST API for data anomaly and schema drift monitoring.",
    version="2.0.0",
    lifespan=lifespan,
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every incoming request and outgoing response with timing."""
    start = time.perf_counter()
    client = request.client.host if request.client else "unknown"
    logger.info("-> %s %s from %s", request.method, request.url.path, client)
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info("<- %s %s -> %s (%.0f ms)", request.method, request.url.path, response.status_code, duration_ms)
    return response


class AnomalyRequest(BaseModel):
    """Request body for anomaly detection."""

    schema_name: str = Field(
        default="bronze",
        alias="schema",
        description="Databricks schema to scan for anomalies.",
    )

    model_config = {"populate_by_name": True}


class AcceptThresholdsRequest(BaseModel):
    """Request body for saving manual threshold overrides."""

    table_name: str
    min_val: float | None = None
    max_val: float | None = None


class AcceptThresholdsResponse(BaseModel):
    """Response for threshold updates."""

    success: bool
    message: str


class SchemaFinding(BaseModel):
    """A single accepted schema finding."""

    column_name: str = ""
    finding_type: str = ""
    data_type: str = ""


class AcceptSchemaRequest(BaseModel):
    """Request body for accepting schema anomaly findings."""

    table_name: str
    accepted_findings: list[SchemaFinding] = Field(default_factory=list)


class AcceptSchemaResponse(BaseModel):
    """Response for schema acceptance."""

    success: bool
    message: str


class AnomalyResponse(BaseModel):
    """Response model for anomaly detection."""

    run_id: str
    total_anomalies: int
    data_anomalies: int
    schema_anomalies: int
    checks_run: int
    checks_with_issues: int
    report_text: str
    report_data: dict = Field(default_factory=dict)


class ReportSubmission(BaseModel):
    """Payload for submitting a report from Databricks."""

    report_data: dict


@app.post(
    "/api/anomaly",
    response_model=AnomalyResponse,
    summary="Run anomaly detection",
    description="Runs data anomaly and schema drift monitoring on the specified schema.",
)
async def run_anomaly(request: AnomalyRequest = AnomalyRequest()) -> AnomalyResponse:
    """Run anomaly detection and return both text and structured output."""
    result = anomaly_graph.invoke({"schema": request.schema_name})
    if result.get("error"):
        raise HTTPException(status_code=500, detail=result["error"])
    return AnomalyResponse(
        run_id=result.get("run_id", ""),
        total_anomalies=result.get("total_anomalies", 0),
        data_anomalies=result.get("data_anomalies", 0),
        schema_anomalies=result.get("schema_anomalies", 0),
        checks_run=result.get("checks_run", 0),
        checks_with_issues=result.get("checks_with_issues", 0),
        report_text=result.get("report_text", ""),
        report_data=result.get("report_data", {}) or {},
    )


@app.post(
    "/api/report",
    summary="Submit anomaly report",
    description="Updates the latest anomaly report in memory (use this from Databricks).",
)
async def submit_report(request: ReportSubmission):
    """Save the report into memory for the dashboard and gatekeeper."""
    global _LATEST_REPORT
    _LATEST_REPORT = request.report_data
    logger.info("Report submitted and cached in memory.")
    return {"success": True, "message": "Report updated"}


@app.get(
    "/api/gatekeeper",
    summary="Pipeline Gatekeeper",
    description="Returns the latest report only if anomalies exist. Used to pause Databricks pipelines.",
)
async def gatekeeper():
    """Gatekeeper logic: return report if total_anomalies > 0, else empty."""
    if _LATEST_REPORT and _LATEST_REPORT.get("total_anomalies", 0) > 0:
        return _LATEST_REPORT
    return {}


@app.post(
    "/api/accept_thresholds",
    response_model=AcceptThresholdsResponse,
    summary="Accept thresholds for a table",
    description="Updates table_thresholds.json with custom min/max values for a specific table.",
)
async def accept_thresholds(request: AcceptThresholdsRequest):
    """Save custom threshold overrides for a table."""
    thresholds_file = PROJECT_ROOT / "table_thresholds.json"
    short_name = request.table_name.split(".")[-1]

    current_thresholds = {}
    if thresholds_file.exists():
        try:
            current_thresholds = json.loads(thresholds_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Failed to read thresholds file: %s", exc)

    current_thresholds.setdefault(short_name, {})

    if request.min_val is not None:
        current_thresholds[short_name]["min_val"] = request.min_val
    if request.max_val is not None:
        current_thresholds[short_name]["max_val"] = request.max_val

    current_thresholds[short_name]["accepted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        thresholds_file.write_text(json.dumps(current_thresholds, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.error("Failed to write threshold file: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to save thresholds")

    return AcceptThresholdsResponse(success=True, message=f"Thresholds updated for {short_name}")


@app.post(
    "/api/accept_schema",
    response_model=AcceptSchemaResponse,
    summary="Accept schema findings for a table",
    description="Records accepted schema columns in accepted_schemas.json. "
    "Accepted columns won't re-trigger as anomalies unless their data type changes.",
)
async def accept_schema(request: AcceptSchemaRequest):
    """Save accepted schema findings into the single thresholds file."""
    thresholds_file = PROJECT_ROOT / "table_thresholds.json"
    short_name = request.table_name.split(".")[-1]

    data: dict = {}
    if thresholds_file.exists():
        try:
            data = json.loads(thresholds_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Failed to read thresholds file: %s", exc)

    data.setdefault(short_name, {})
    data[short_name].setdefault("accepted_columns", [])

    existing = {c["column_name"]: c for c in data[short_name].get("accepted_columns", [])}
    for finding in request.accepted_findings:
        if finding.column_name:
            existing[finding.column_name] = {
                "column_name": finding.column_name,
                "finding_type": finding.finding_type,
                "data_type": finding.data_type,
                "accepted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

    data[short_name]["accepted_columns"] = list(existing.values())
    data[short_name]["schema_accepted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    try:
        thresholds_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.error("Failed to write thresholds file: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to save accepted schemas")

    return AcceptSchemaResponse(success=True, message=f"Schema accepted for {short_name}")


@app.get(
    "/api/accepted_state",
    summary="Get current acceptance state",
    description="Returns the accepted tables and schemas from the single thresholds file. "
    "Used by the frontend on page load to restore acceptance state.",
)
async def get_accepted_state():
    """Read the single thresholds file and return accepted state for the frontend."""
    thresholds_file = PROJECT_ROOT / "table_thresholds.json"

    data: dict = {}
    if thresholds_file.exists():
        try:
            data = json.loads(thresholds_file.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Failed to read thresholds file: %s", exc)

    accepted_tables = {}
    accepted_schemas = {}

    for table_name, config in data.items():
        if not isinstance(config, dict):
            continue
        # If it has an accepted_at timestamp, it's an accepted data table
        if "accepted_at" in config:
            accepted_tables[table_name] = True
        # If it has accepted_columns, build the schema acceptance state
        if "accepted_columns" in config:
            columns_dict = {}
            for col in config["accepted_columns"]:
                col_name = col.get("column_name", "")
                if col_name:
                    columns_dict[col_name] = {
                        "finding_type": col.get("finding_type", ""),
                        "data_type": col.get("data_type", "")
                    }
            if columns_dict:
                accepted_schemas[table_name] = {"columns": columns_dict}

    return {
        "accepted_tables": accepted_tables,
        "accepted_schemas": accepted_schemas
    }

def _get_latest_file(directory: Path, filename: str) -> Path | None:
    """Find the target file first in the latest run sub-dir, then directly in the directory."""
    if not directory.exists():
        return None
    run_dirs = sorted(
        [item for item in directory.iterdir() if item.is_dir()],
        key=lambda item: item.name,
        reverse=True,
    )
    for run_dir in run_dirs:
        target_file = run_dir / filename
        if target_file.exists():
            return target_file
    direct = directory / filename
    return direct if direct.exists() else None


@app.get("/api/latest_anomaly")
async def get_latest_anomaly():
    """Serve the most recent anomalies.txt file."""
    file_path = _get_latest_file(OUTPUT_DIR / "Anomaly", "anomalies.txt")
    if not file_path:
        raise HTTPException(status_code=404, detail="No anomaly report found")
    return FileResponse(file_path, media_type="text/plain")


@app.get("/api/latest_anomaly_json")
async def get_latest_anomaly_json():
    """Serve the most recent anomalies.json file (check memory first)."""
    if _LATEST_REPORT:
        return _LATEST_REPORT

    file_path = _get_latest_file(OUTPUT_DIR / "Anomaly", "anomalies.json")
    if not file_path:
        raise HTTPException(status_code=404, detail="No anomaly payload found")
    return FileResponse(file_path, media_type="application/json")


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    """Return an empty favicon response to avoid noisy 404s in the browser."""
    return Response(status_code=204)


if DASHBOARD_DIR.exists():
    app.mount("/", StaticFiles(directory=str(DASHBOARD_DIR), html=True), name="dashboard")
