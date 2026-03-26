from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task, chain, Asset

_DUCKDB_CONN_ID = "duckdb_astrotrips"


@dag(
    schedule=Asset("daily_report"),
    tags=["astrotrips", "reporting"]
)
def publish_report():

    _get_report = SQLExecuteQueryOperator(
        task_id="get_report",
        conn_id=_DUCKDB_CONN_ID,
        sql="SELECT * FROM daily_planet_report WHERE report_date = (SELECT MAX(report_date) FROM daily_planet_report)"
    )

    @task
    def print_report(ti = None):
        from include.utils import print_report_row

        rows = ti.xcom_pull(task_ids="get_report") or []

        print("::group::Daily Planet Report")

        print("Planet | Passengers | Active | Done | Gross USD | Discount | Net USD")
        print("-" * 65)

        for row in rows:
            print_report_row(row)

        print("::endgroup::")

    chain(
        _get_report,
        print_report()
    )


publish_report()
