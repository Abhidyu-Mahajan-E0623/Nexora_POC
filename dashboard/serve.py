"""Simple HTTP server to serve the anomaly dashboard locally.

Usage:
    python dashboard/serve.py

Then open http://localhost:8050 in your browser.

Acceptance state is stored in a SINGLE file: table_thresholds.json
This file holds both data-threshold overrides and accepted-schema columns.
If you delete this file, ALL acceptances are reset.
"""

import http.server
import json
import time
from pathlib import Path

PORT = 8050
PROJECT_ROOT = Path(__file__).resolve().parent.parent  # Schema_Maker_Final
DASHBOARD_DIR = Path(__file__).resolve().parent         # dashboard/
ANOMALY_OUTPUT_DIR = PROJECT_ROOT / "Output" / "Anomaly"
THRESHOLDS_FILE = PROJECT_ROOT / "table_thresholds.json"


def _read_json(path: Path) -> dict:
    """Read a JSON file or return empty dict if missing/invalid."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, data: dict) -> None:
    """Write a dict to a JSON file."""
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Serves dashboard files and handles API endpoints."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DASHBOARD_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/data/anomalies.txt":
            self._serve_latest_anomaly_report()
            return
        if self.path == "/api/accepted_state":
            self._handle_get_accepted_state()
            return
        super().do_GET()

    def do_POST(self):
        """Handle POST requests for accept endpoints."""
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            body = json.loads(raw_body)
        except Exception:
            self._json_response(400, {"success": False, "message": "Invalid JSON"})
            return

        if self.path == "/api/accept_thresholds":
            self._handle_accept_thresholds(body)
        elif self.path == "/api/accept_schema":
            self._handle_accept_schema(body)
        else:
            self._json_response(404, {"success": False, "message": "Not found"})

    def do_DELETE(self):
        """Handle reset requests for accepted state."""
        if self.path == "/api/accepted_state":
            self._handle_reset_accepted_state()
            return
        self._json_response(404, {"success": False, "message": "Not found"})

    # ----- GET Accepted State -----
    def _handle_get_accepted_state(self):
        """Read the single thresholds file and return accepted tables & schemas."""
        data = _read_json(THRESHOLDS_FILE)
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

        self._json_response(200, {
            "accepted_tables": accepted_tables,
            "accepted_schemas": accepted_schemas
        })

    def _handle_reset_accepted_state(self):
        """Delete saved threshold overrides and schema acceptances."""
        if THRESHOLDS_FILE.exists():
            THRESHOLDS_FILE.unlink()
            message = "Accepted anomaly history reset."
        else:
            message = "Accepted anomaly history was already empty."
        self._json_response(200, {"success": True, "message": message})

    # ----- Accept Thresholds -----
    def _handle_accept_thresholds(self, body: dict):
        table_name = body.get("table_name", "").split(".")[-1]
        if not table_name:
            self._json_response(400, {"success": False, "message": "table_name required"})
            return

        thresholds = _read_json(THRESHOLDS_FILE)
        thresholds.setdefault(table_name, {})

        min_val = body.get("min_val")
        max_val = body.get("max_val")
        if min_val is not None:
            thresholds[table_name]["min_val"] = min_val
        if max_val is not None:
            thresholds[table_name]["max_val"] = max_val
        thresholds[table_name]["accepted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        _write_json(THRESHOLDS_FILE, thresholds)
        self._json_response(200, {"success": True, "message": f"Thresholds updated for {table_name}"})

    # ----- Accept Schema -----
    def _handle_accept_schema(self, body: dict):
        table_name = body.get("table_name", "").split(".")[-1]
        if not table_name:
            self._json_response(400, {"success": False, "message": "table_name required"})
            return

        findings = body.get("accepted_findings", [])
        thresholds = _read_json(THRESHOLDS_FILE)
        thresholds.setdefault(table_name, {})
        thresholds[table_name].setdefault("accepted_columns", [])

        existing = {c["column_name"]: c for c in thresholds[table_name].get("accepted_columns", [])}
        for f in findings:
            col_name = f.get("column_name", "")
            if col_name:
                existing[col_name] = {
                    "column_name": col_name,
                    "finding_type": f.get("finding_type", ""),
                    "data_type": f.get("data_type", ""),
                    "accepted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                }

        thresholds[table_name]["accepted_columns"] = list(existing.values())
        thresholds[table_name]["schema_accepted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        _write_json(THRESHOLDS_FILE, thresholds)
        self._json_response(200, {"success": True, "message": f"Schema accepted for {table_name}"})

    # ----- Helpers -----
    def _json_response(self, status: int, data: dict):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _serve_latest_anomaly_report(self):
        """Find and serve the latest anomalies.txt from Output/Anomaly."""
        if not ANOMALY_OUTPUT_DIR.exists():
            self.send_error(404, "Anomaly output directory not found")
            return

        # Sort run directories by name (they contain timestamps) to find the latest
        run_dirs = sorted(
            [d for d in ANOMALY_OUTPUT_DIR.iterdir() if d.is_dir()],
            key=lambda d: d.name,
            reverse=True,
        )

        for run_dir in run_dirs:
            anomaly_file = run_dir / "anomalies.txt"
            if anomaly_file.exists():
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                self.wfile.write(anomaly_file.read_bytes())
                print(f"  Served: {anomaly_file}")
                return

        self.send_error(404, "No anomaly report found in Output/Anomaly/")

    def log_message(self, format, *args):
        """Custom log format — only log API calls."""
        try:
            msg = format % args
        except Exception:
            msg = str(args)
        if "/data/" in msg or "/api/" in msg:
            print(f"  [API] {msg}")


if __name__ == "__main__":
    print(f"==============================================")
    print(f"   Anomaly Detection Dashboard")
    print(f"   http://localhost:{PORT}")
    print(f"----------------------------------------------")
    print(f"   Dashboard : {DASHBOARD_DIR}")
    print(f"   Data from : {ANOMALY_OUTPUT_DIR}")
    print(f"   Thresholds: {THRESHOLDS_FILE}")
    print(f"==============================================")
    print()

    server = http.server.HTTPServer(("", PORT), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.shutdown()
