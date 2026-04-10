"""LLM Insight Agent — converts ML-based RCA JSON into natural language insights.

Usage (standalone):
    python -m src_insight.generator
    python -m src_insight.generator --rca Input/rca_data.json --context Input/context_guide.md
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from openai import AzureOpenAI

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RCA_PATH = PROJECT_ROOT / "Input" / "rca_data.json"
DEFAULT_CONTEXT_PATH = PROJECT_ROOT / "Input" / "context_guide.md"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "Output" / "Insight"

logger = logging.getLogger("nexora.insight_agent")


# ---------------------------------------------------------------------------
# Settings loader (reuses existing .env config)
# ---------------------------------------------------------------------------

def _load_env_settings() -> dict[str, str]:
    """Load Azure OpenAI settings from .env via the project's Settings class."""
    try:
        from src.config.settings import load_settings_or_raise
        settings = load_settings_or_raise()
        return {
            "endpoint": settings.AZURE_OPENAI_ENDPOINT or "",
            "api_key": settings.AZURE_OPENAI_API_KEY or "",
            "api_version": settings.AZURE_OPENAI_API_VERSION,
            "deployment": settings.AZURE_OPENAI_CHAT_DEPLOYMENT or "",
        }
    except Exception as exc:
        logger.error("Failed to load settings: %s", exc)
        raise


# ---------------------------------------------------------------------------
# Input loaders
# ---------------------------------------------------------------------------

def load_rca_json(path: Path) -> dict[str, Any]:
    """Read and parse the RCA JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"RCA data file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_context_guide(path: Path) -> str:
    """Read the context guide markdown file."""
    if not path.exists():
        raise FileNotFoundError(f"Context guide not found: {path}")
    return path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a senior data quality analyst at a pharmaceutical company. You are \
writing a concise executive summary for business stakeholders (VP of Data, \
Chief Data Officer).

Rules:
1. Use a professional, corporate tone — clear and direct.
2. Avoid technical jargon; use plain English.
3. Always cite the exact percentage contribution of each column.
4. Structure the output with numbered sections:
   - Executive Summary (2-3 sentences)
   - Key Findings (bullet points with % contributions)
   - Business Impact Assessment
   - Recommended Actions
5. Keep the total output under 400 words.
6. Do NOT use markdown formatting — produce clean plain text only.
"""


def build_user_prompt(rca_data: dict[str, Any], context: str) -> str:
    """Build the user message from RCA data and domain context."""
    report = rca_data.get("anomaly_report", rca_data)

    table_name = report.get("table_name", "unknown")
    detector = report.get("detector", "unknown")
    anomaly_date = report.get("anomaly_date", "unknown")
    actual_value = report.get("actual_value", "N/A")
    expected_range = report.get("expected_range", "N/A")
    rca_entries = report.get("rca_analysis", [])

    rca_lines = []
    for entry in rca_entries:
        col = entry.get("column", "unknown")
        pct = entry.get("contribution_pct", 0)
        insight = entry.get("insight", "")
        top_vals = entry.get("top_values", [])
        top_str = ", ".join(
            f"{v.get('value', '?')} ({v.get('count', '?')} records)"
            for v in top_vals
        )
        rca_lines.append(
            f"  - Column: {col}\n"
            f"    Contribution: {pct}%\n"
            f"    Top values: {top_str or 'N/A'}\n"
            f"    ML Insight: {insight}"
        )

    rca_block = "\n".join(rca_lines)

    return (
        f"=== DOMAIN CONTEXT ===\n"
        f"{context}\n\n"
        f"=== ANOMALY DETAILS ===\n"
        f"Table: {table_name}\n"
        f"Detector: {detector}\n"
        f"Date: {anomaly_date}\n"
        f"Actual Value: {actual_value}\n"
        f"Expected Range: {expected_range}\n\n"
        f"=== ROOT CAUSE ANALYSIS (from ML model) ===\n"
        f"{rca_block}\n\n"
        f"Please produce an executive insight report based on the above."
    )


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------

def generate_insight(rca_data: dict[str, Any], context: str, settings: dict[str, str]) -> str:
    """Call Azure OpenAI and return the generated insight text."""
    client = AzureOpenAI(
        azure_endpoint=settings["endpoint"],
        api_key=settings["api_key"],
        api_version=settings["api_version"],
    )

    user_message = build_user_prompt(rca_data, context)

    logger.info("[insight] Sending request to Azure OpenAI deployment '%s'", settings["deployment"])
    start = time.perf_counter()

    response = client.chat.completions.create(
        model=settings["deployment"],
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.3,
        max_tokens=800,
    )

    elapsed = time.perf_counter() - start
    logger.info("[insight] Response received in %.1f seconds", elapsed)

    return response.choices[0].message.content.strip()


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------

def save_insight(text: str, output_dir: Path) -> Path:
    """Write the insight report to a timestamped text file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    output_file = output_dir / f"output_insight_{timestamp}.txt"
    output_file.write_text(text, encoding="utf-8")

    # Also write a convenience "latest" copy
    latest = output_dir / "output_insight.txt"
    latest.write_text(text, encoding="utf-8")

    return output_file


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the LLM Insight Agent as a standalone script."""
    parser = argparse.ArgumentParser(description="LLM Insight Agent for RCA interpretation")
    parser.add_argument("--rca", type=str, default=str(DEFAULT_RCA_PATH),
                        help="Path to the RCA JSON file (default: Input/rca_data.json)")
    parser.add_argument("--context", type=str, default=str(DEFAULT_CONTEXT_PATH),
                        help="Path to the context guide (default: Input/context_guide.md)")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT_DIR),
                        help="Output directory (default: Output/Insight)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info("=" * 60)
    logger.info("  Nexora LLM Insight Agent")
    logger.info("=" * 60)

    # 1. Load settings
    logger.info("[1/4] Loading Azure OpenAI configuration...")
    settings = _load_env_settings()

    # 2. Load inputs
    logger.info("[2/4] Loading RCA data from: %s", args.rca)
    rca_data = load_rca_json(Path(args.rca))

    logger.info("[2/4] Loading context guide from: %s", args.context)
    context = load_context_guide(Path(args.context))

    # 3. Generate insight
    logger.info("[3/4] Generating insight via Azure OpenAI...")
    insight_text = generate_insight(rca_data, context, settings)

    # 4. Save output
    output_path = save_insight(insight_text, Path(args.output))
    logger.info("[4/4] Insight saved to: %s", output_path)

    logger.info("=" * 60)
    logger.info("  Done. Output preview:")
    logger.info("=" * 60)
    print()
    print(insight_text)
    print()


if __name__ == "__main__":
    main()
