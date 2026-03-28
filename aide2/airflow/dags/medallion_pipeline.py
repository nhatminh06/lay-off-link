"""
Daily medallion ETL: bronze → silver → gold → Feast materialization.

Uses Spark for batch layers and a MinIO raw-data sensor before ingest.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
except ImportError:  # pragma: no cover — optional provider
    SparkSubmitOperator = None  # type: ignore[misc, assignment]


def _project_root() -> str:
    return Variable.get(
        "lay_off_link_root",
        default_var=os.environ.get("LAY_OFF_LINK_ROOT", "/home/pnmynh/Documents/lay-off-link"),
    )


def _on_failure_callback(context: dict[str, Any]) -> None:
    """Failure callback: log task id and exception; extend with PagerDuty/Slack."""
    ti = context.get("task_instance")
    logger.error(
        "Task failed: dag_id=%s task_id=%s try=%s exception=%s",
        context.get("dag").dag_id if context.get("dag") else None,
        ti.task_id if ti else None,
        ti.try_number if ti else None,
        context.get("exception"),
    )


def _sla_miss_callback(
    dag: Any, task_list: Any, blocking_task_list: Any, slas: Any, blocking_tis: Any
) -> None:
    """SLA miss notifier placeholder."""
    logger.warning(
        "SLA miss: dag=%s tasks=%s blocking=%s",
        getattr(dag, "dag_id", dag),
        task_list,
        blocking_task_list,
    )


def _raw_data_exists() -> bool:
    """
    Return True when the MinIO raw bucket contains at least one object under the prefix.

    Uses Airflow connection ``minio_default`` (AWS-compatible S3) or env fallbacks.
    """
    try:
        import boto3
    except ImportError:
        logger.warning("boto3 not installed; install boto3 for MinIO raw-data sensor.")
        return False

    bucket = Variable.get("minio_raw_bucket", default_var="raw")
    prefix = Variable.get("minio_raw_prefix", default_var="nyc-taxi/")
    conn_id = Variable.get("minio_connection_id", default_var="minio_default")

    try:
        from airflow.hooks.base import BaseHook

        c = BaseHook.get_connection(conn_id)
        extra = c.extra_dejson if hasattr(c, "extra_dejson") else {}
        endpoint = extra.get("endpoint_url") or os.environ.get(
            "MINIO_ENDPOINT", "http://127.0.0.1:9000"
        )
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=c.login or os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=c.password or os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            region_name=extra.get("region_name", "us-east-1"),
        )
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0
    except Exception as exc:  # noqa: BLE001
        logger.warning("MinIO raw check failed, reporting not ready: %s", exc)
        return False


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="medallion_etl_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold Spark pipeline with Feast materialize",
    schedule_interval="0 2 * * *",  # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "spark", "feast"],
) as dag:
    wait_for_raw_data = PythonSensor(
        task_id="wait_for_raw_data",
        python_callable=_raw_data_exists,
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 6,
        soft_fail=False,
    )

    root = _project_root()
    spark_master = Variable.get("spark_master", default_var="local[*]")
    spark_conn_id = Variable.get("spark_connection_id", default_var="spark_default")
    feast_repo = Variable.get("feast_repo_path", default_var=f"{root}/feature_repo")

    common_spark_conf = {
        "spark.executor.memory": Variable.get("spark_executor_memory", default_var="2g"),
        "spark.driver.memory": Variable.get("spark_driver_memory", default_var="1g"),
    }

    if SparkSubmitOperator is not None:
        ingest_to_bronze = SparkSubmitOperator(
            task_id="ingest_to_bronze",
            application=f"{root}/aide2/spark/bronze_ingestion.py",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            name="ingest_to_bronze",
            conf=common_spark_conf,
            sla=timedelta(hours=3),
            sla_miss_callback=_sla_miss_callback,
        )
        transform_to_silver = SparkSubmitOperator(
            task_id="transform_to_silver",
            application=f"{root}/aide2/spark/silver_transform.py",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            name="transform_to_silver",
            conf=common_spark_conf,
            sla=timedelta(hours=3),
            sla_miss_callback=_sla_miss_callback,
        )
        aggregate_to_gold = SparkSubmitOperator(
            task_id="aggregate_to_gold",
            application=f"{root}/aide2/spark/gold_aggregation.py",
            conn_id=spark_conn_id,
            verbose=True,
            deploy_mode="client",
            name="aggregate_to_gold",
            conf=common_spark_conf,
            sla=timedelta(hours=3),
            sla_miss_callback=_sla_miss_callback,
        )
    else:
        ingest_to_bronze = BashOperator(
            task_id="ingest_to_bronze",
            bash_command=f"spark-submit --master {spark_master} {root}/aide2/spark/bronze_ingestion.py",
            sla=timedelta(hours=3),
            sla_miss_callback=_sla_miss_callback,
        )
        transform_to_silver = BashOperator(
            task_id="transform_to_silver",
            bash_command=f"spark-submit --master {spark_master} {root}/aide2/spark/silver_transform.py",
            sla=timedelta(hours=3),
            sla_miss_callback=_sla_miss_callback,
        )
        aggregate_to_gold = BashOperator(
            task_id="aggregate_to_gold",
            bash_command=f"spark-submit --master {spark_master} {root}/aide2/spark/gold_aggregation.py",
            sla=timedelta(hours=3),
            sla_miss_callback=_sla_miss_callback,
        )

    update_feature_store = BashOperator(
        task_id="update_feature_store",
        bash_command=(
            f"cd {feast_repo} && "
            "feast materialize "
            "$(date -u -d '1 day ago' +%Y-%m-%dT%H:%M:%S) "
            "$(date -u +%Y-%m-%dT%H:%M:%S)"
        ),
        sla=timedelta(hours=1),
        sla_miss_callback=_sla_miss_callback,
    )

    (
        wait_for_raw_data
        >> ingest_to_bronze
        >> transform_to_silver
        >> aggregate_to_gold
        >> update_feature_store
    )
