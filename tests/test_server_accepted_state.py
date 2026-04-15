"""Tests for accepted-state API endpoints."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

import api.server as server


def test_get_accepted_state_returns_tables_and_schema_columns(tmp_path, monkeypatch) -> None:
    thresholds_file = tmp_path / "table_thresholds.json"
    thresholds_file.write_text(
        json.dumps(
            {
                "sales_data": {
                    "min_val": 100.0,
                    "max_val": 250.0,
                    "accepted_at": "2026-04-15T00:00:00Z",
                },
                "hcp_details": {
                    "accepted_columns": [
                        {
                            "column_name": "hcp_type",
                            "finding_type": "column_added",
                            "data_type": "string",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(server, "THRESHOLDS_FILE", thresholds_file)
    client = TestClient(server.app)

    response = client.get("/api/accepted_state")

    assert response.status_code == 200
    assert response.json() == {
        "accepted_tables": {"sales_data": True},
        "accepted_schemas": {
            "hcp_details": {
                "columns": {
                    "hcp_type": {
                        "finding_type": "column_added",
                        "data_type": "string",
                    }
                }
            }
        },
    }


def test_delete_accepted_state_removes_thresholds_file(tmp_path, monkeypatch) -> None:
    thresholds_file = tmp_path / "table_thresholds.json"
    thresholds_file.write_text(json.dumps({"sales_data": {"accepted_at": "2026-04-15T00:00:00Z"}}), encoding="utf-8")
    monkeypatch.setattr(server, "THRESHOLDS_FILE", thresholds_file)
    client = TestClient(server.app)

    response = client.delete("/api/accepted_state")

    assert response.status_code == 200
    assert response.json()["success"] is True
    assert not thresholds_file.exists()
