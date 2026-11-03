from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from h2bq.bq import client
from h2bq.config import load_config
from h2bq.loader import run_load
from h2bq.validator import run_dvt
from h2bq.views import create_views

TABLES = ["taxi_zones", "yellow_trips"]


def _conf_date(ctx, key):
    v = (ctx["dag_run"].conf or {}).get(key)
    return date.fromisoformat(v) if v else None


def _load(table, **ctx):
    run_load(client(), load_config(table), _conf_date(ctx, "start"), _conf_date(ctx, "end"))


def _validate(table, **ctx):
    run_dvt(client(), load_config(table))


def _views(**ctx):
    create_views(client())


with DAG(
    dag_id="hive2bq_migration",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["migration", "gcp"],
) as dag:
    start = EmptyOperator(task_id="start")
    views = PythonOperator(task_id="create_projection_views", python_callable=_views)

    for name in TABLES:
        with TaskGroup(group_id=name) as tg:
            load = PythonOperator(task_id="load", python_callable=_load, op_kwargs={"table": name})
            validate = PythonOperator(task_id="validate", python_callable=_validate,
                                      op_kwargs={"table": name}, retries=0)
            load >> validate
        start >> tg >> views
