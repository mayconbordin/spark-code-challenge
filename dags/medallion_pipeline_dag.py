"""Airflow DAG: billups_medallion_pipeline.

Runs the PySpark medallion pipeline as three sequential tasks
(bronze -> silver -> gold). Each task shells out to the project's CLI:

    python -m billups_pipeline.pipeline --layers <layer>

Why BashOperator (not SparkSubmitOperator):
    The ``billups_pipeline`` package builds and configures its own SparkSession
    from environment variables (SPARK_MASTER, BILLUPS_WAREHOUSE, ...). Driving it
    with BashOperator therefore avoids spark-submit classpath/--conf juggling and
    keeps a single, identical code path whether run from Airflow, Jupyter, or a
    shell. The package connects to the standalone cluster via SPARK_MASTER.

Trigger: manual only (schedule_interval=None, catchup=False).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

# Environment every task needs to find the data, the shared Iceberg warehouse,
# the Spark cluster, and the (mounted, not installed) project package.
PIPELINE_ENV = (
    "PYTHONPATH=/opt/billups/src "
    "BILLUPS_DATA_DIR=/opt/billups/data "
    "BILLUPS_WAREHOUSE=/opt/billups/warehouse "
    "SPARK_MASTER=spark://spark-master:7077"
)

default_args = {
    "owner": "billups",
    "retries": 0,
}


def _layer_command(layer: str) -> str:
    """Build the shell command that runs a single medallion layer."""
    return f"{PIPELINE_ENV} python -m billups_pipeline.pipeline --layers {layer}"


with DAG(
    dag_id="billups_medallion_pipeline",
    description="Bronze -> Silver -> Gold medallion build on the Spark cluster.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger only
    catchup=False,
    tags=["billups", "medallion", "spark", "iceberg"],
) as dag:
    bronze = BashOperator(
        task_id="bronze",
        bash_command=_layer_command("bronze"),
    )

    silver = BashOperator(
        task_id="silver",
        bash_command=_layer_command("silver"),
    )

    gold = BashOperator(
        task_id="gold",
        bash_command=_layer_command("gold"),
    )

    bronze >> silver >> gold
