"""CLI entrypoint for anomaly monitoring."""

from __future__ import annotations

from typing import Annotated

import typer

from src.config.settings import load_settings_or_raise
from src.connectors.databricks_sql import DatabricksSQLError
from src.utils.logging import configure_logging
from src.utils.time import new_run_id
from src_anomaly.pipeline import run_bronze_anomaly_detection

app = typer.Typer(help="Run data anomaly and schema drift monitoring.")


@app.command("anomaly-detect")
def anomaly_detect(
    schema: Annotated[
        str,
        typer.Option("--schema", help="Source schema to scan for anomalies."),
    ] = "raw",
) -> None:
    """Run anomaly detection for the requested schema."""
    settings = load_settings_or_raise()
    try:
        run_id = new_run_id()
        logger = configure_logging(run_id=run_id)
        outcome = run_bronze_anomaly_detection(
            settings=settings,
            run_id=run_id,
            catalog=settings.DATABRICKS_CATALOG,
            schema=schema,
            logger=logger,
        )
        typer.echo(f"run_id={outcome.run_id}")
        typer.echo(f"checks_run={outcome.checks_run}")
        typer.echo(f"checks_with_issues={outcome.checks_with_issues}")
        typer.echo(f"total_anomalies={outcome.total_anomalies}")
        typer.echo(f"data_anomalies={outcome.data_anomalies}")
        typer.echo(f"schema_anomalies={outcome.schema_anomalies}")
        typer.echo(f"anomaly_report={outcome.report_path}")
        typer.echo(f"anomaly_json={outcome.json_path}")
    except DatabricksSQLError as exc:
        typer.secho(f"Databricks error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc


@app.command("version")
def version() -> None:
    """Print application version info."""
    settings = load_settings_or_raise()
    typer.echo(f"{settings.APP_NAME} {settings.APP_VERSION}")
    typer.echo(f"catalog={settings.DATABRICKS_CATALOG}")
    typer.echo(f"default_schema={settings.DATABRICKS_SCHEMA_DOMAIN}")


if __name__ == "__main__":
    app()
