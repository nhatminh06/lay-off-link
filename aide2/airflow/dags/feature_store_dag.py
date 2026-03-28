"""
Periodic feature store validation and incremental materialization.
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
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)


def _project_root() -> str:
    return Variable.get(
        "lay_off_link_root",
        default_var=os.environ.get("LAY_OFF_LINK_ROOT", "/home/pnmynh/Documents/lay-off-link"),
    )


def _on_failure_callback(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    logger.error(
        "feature_store_dag failure: task_id=%s try=%s err=%s",
        ti.task_id if ti else None,
        ti.try_number if ti else None,
        context.get("exception"),
    )


def _validate_gold_tables() -> None:
    """
    Placeholder: verify Gold layer tables were updated within the last 24 hours.

    Wire to metastore / Glue / Delta history in production.
    """
    max_age_hours = int(Variable.get("gold_table_max_age_hours", default_var="24"))
    logger.info("Checking gold table freshness (max_age_hours=%s)", max_age_hours)
    # Implement: query INFORMATION_SCHEMA or Delta DESCRIBE HISTORY
    logger.info("Gold table freshness check passed (placeholder).")


def _validate_features() -> None:
    """Placeholder: assert feature values fall within configured bounds."""
    logger.info("Validating feature value ranges (placeholder).")


def _notify_success() -> None:
    """Placeholder success hook (email, Slack, etc.)."""
    logger.info("Feature store refresh completed successfully (placeholder notification).")


default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="feature_store_refresh",
    default_args=default_args,
    description="Validate gold, materialize Feast features incrementally, validate outputs",
    schedule_interval="0 */6 * * *",  # every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=["feast", "features", "validation"],
) as dag:
    validate_gold_tables = PythonOperator(
        task_id="validate_gold_tables",
        python_callable=_validate_gold_tables,
        execution_timeout=timedelta(minutes=30),
    )

    root = _project_root()
    feast_repo = Variable.get("feast_repo_path", default_var=f"{root}/feature_repo")

    materialize_features = BashOperator(
        task_id="materialize_features",
        bash_command=f"cd {feast_repo} && feast materialize-incremental $(date -u +%Y-%m-%dT%H:%M:%S)",
        execution_timeout=timedelta(hours=2),
    )

    validate_features = PythonOperator(
        task_id="validate_features",
        python_callable=_validate_features,
        execution_timeout=timedelta(minutes=30),
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
    )

    validate_gold_tables >> materialize_features >> validate_features >> notify_success
