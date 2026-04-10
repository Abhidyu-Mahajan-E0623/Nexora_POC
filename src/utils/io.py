"""Filesystem helpers with atomic writes and path contracts."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable

import orjson

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_DIR = PROJECT_ROOT / "Input"
OUTPUT_DIR = PROJECT_ROOT / "Output"

OUTPUT_LOGS_DIR = OUTPUT_DIR / "logs"
OUTPUT_ANOMALY_DIR = OUTPUT_DIR / "Anomaly"
OUTPUT_SCHEMA_SNAPSHOTS_DIR = OUTPUT_DIR / "SchemaSnapshots"


def ensure_project_dirs() -> None:
    """Create required Input/Output folders if missing."""
    for path in (
        INPUT_DIR,
        OUTPUT_DIR,
        OUTPUT_LOGS_DIR,
        OUTPUT_ANOMALY_DIR,
        OUTPUT_SCHEMA_SNAPSHOTS_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)


def anomaly_output_dir(run_id: str) -> Path:
    """Return per-run anomaly output directory and create it."""
    path = OUTPUT_ANOMALY_DIR / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def anomaly_json_path(run_id: str) -> Path:
    """Return the structured JSON payload path for an anomaly run."""
    return anomaly_output_dir(run_id) / "anomalies.json"


def schema_snapshot_path(catalog: str, schema: str, table: str) -> Path:
    """Return the persistent snapshot path for a table schema."""
    path = OUTPUT_SCHEMA_SNAPSHOTS_DIR / catalog / schema
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{table}.json"


def atomic_write_bytes(path: Path, payload: bytes) -> Path:
    """Atomically write bytes to target file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(delete=False, dir=path.parent) as temp:
        temp.write(payload)
        temp.flush()
        temp_path = Path(temp.name)
    temp_path.replace(path)
    return path


def atomic_write_text(path: Path, payload: str, encoding: str = "utf-8") -> Path:
    """Atomically write text file."""
    return atomic_write_bytes(path, payload.encode(encoding))


def atomic_write_json(path: Path, payload: Any, indent: bool = True) -> Path:
    """Atomically write JSON file."""
    option = 0
    if indent:
        option |= orjson.OPT_INDENT_2
    data = orjson.dumps(payload, option=option)
    return atomic_write_bytes(path, data)


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    """Write JSONL rows."""
    lines = [orjson.dumps(row).decode("utf-8") for row in rows]
    content = "\n".join(lines)
    if content:
        content += "\n"
    return atomic_write_text(path, content)


def read_json(path: Path | str) -> Any:
    """Read JSON payload."""
    return orjson.loads(Path(path).read_bytes())


def read_jsonl(path: Path | str) -> list[dict[str, Any]]:
    """Read JSONL rows from file."""
    rows: list[dict[str, Any]] = []
    path = Path(path)
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped:
            rows.append(orjson.loads(stripped))
    return rows


def sha256_hex(text: str) -> str:
    """Return sha256 hash for prompt metadata."""
    return sha256(text.encode("utf-8")).hexdigest()
