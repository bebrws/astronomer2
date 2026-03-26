from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator
from airflow.sdk import dag, chain, Asset
from pendulum import datetime

from include.mission_control import MissionControlOperator

_DUCKDB_CONN_ID = "duckdb_astrotrips"


@dag(
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    tags=["astrotrips", "reporting"],
    template_searchpath=f"{AIRFLOW_HOME}/include/sql"
)
def daily_report():

    _ingest_data = SQLExecuteQueryOperator(
        task_id="ingest",
        conn_id=_DUCKDB_CONN_ID,
        sql="generate.sql",
        params={ "n_bookings": 5 }
    )

    _generate_report = SQLExecuteQueryOperator(
        task_id="generate_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="report.sql",
        parameters={ "reportDate": "{{ ds }}" }
    )

    _validate_report= SQLColumnCheckOperator(
        task_id="validate_report",
        conn_id=_DUCKDB_CONN_ID,
        table="daily_planet_report",
        column_mapping={
            "planet_name": {
                "null_check":     {"equal_to": 0},
                "distinct_check": {"geq_to": 3},
            },
            "total_passengers": {
                "null_check":     {"equal_to": 0},
                "min":            {"geq_to": 1},
            }
        },
        outlets=Asset("daily_report")
    )

    _mission_control = MissionControlOperator(task_id="mission_control")

    chain(
        _ingest_data,
        _generate_report,
        _validate_report,
        _mission_control
    )


daily_report()
