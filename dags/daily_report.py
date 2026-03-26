from airflow.configuration import AIRFLOW_HOME
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator
from airflow.sdk import dag, chain, Asset
from pendulum import datetime

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

daily_report()
