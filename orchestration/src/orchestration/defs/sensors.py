from dagster import sensor, RunRequest, SensorEvaluationContext, define_asset_job
from pathlib import Path
import os

# NOTE: unlike cloud databases (Snowflake, S3, etc.), DuckDB has no native
# notification system for incoming data. The sensor below polls the source
# CSV files periodically and triggers a freshness check when it detects
# that any file has been modified.

SOURCE_DIR = Path("data/raw")  # <source_data_path>

freshness_check_job = define_asset_job(name="freshness_check_job")

@sensor(job=freshness_check_job, minimum_interval_seconds=86400)  # polls every 24 hours
def source_freshness_sensor(context: SensorEvaluationContext):
    last_mtime = float(context.cursor) if context.cursor else 0.0

    latest_mtime = max(
        os.path.getmtime(os.path.join(root, f))
        for root, _, files in os.walk(SOURCE_DIR)
        for f in files
        if f.endswith(".csv")
    )

    if latest_mtime > last_mtime:
        context.update_cursor(str(latest_mtime))
        yield RunRequest(run_key=str(latest_mtime))