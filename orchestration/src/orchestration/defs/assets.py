from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path

DBT_PROJECT_DIR = (
    Path(__file__).parent.parent.parent.parent.parent / "brz_ecommerce_backend"
)

@dbt_assets(manifest=DBT_PROJECT_DIR / "target" / "manifest.json")
def brz_ecommerce_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()