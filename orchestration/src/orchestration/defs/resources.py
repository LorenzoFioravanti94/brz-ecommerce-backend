from dagster_dbt import DbtCliResource
from pathlib import Path

DBT_PROJECT_DIR = (
    Path(__file__).parent.parent.parent.parent.parent / "brz_ecommerce_backend"
)

dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    target="prod"
)