from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLInsertRowsOperator
from airflow.sdk import chain, dag, task

_DUCKDB_CONN_ID = "duckdb_astrotrips"


@dag(
    schedule="@daily",
    tags=["astrotrips", "ingest"],
)
def weather_ingest():

    _get_planets = SQLExecuteQueryOperator(
        task_id="get_planets",
        conn_id=_DUCKDB_CONN_ID,
        sql="SELECT DISTINCT planet_id FROM planets",
    )

    @task
    def extract_planet_ids(query_result):
        """Extract planet IDs from query result rows."""
        return [row[0] for row in query_result]

    @task
    def fetch_weather(planet_id: int, logical_date=None):
        from include.weather_api import get_planet_weather
        return get_planet_weather(planet_id, logical_date.date())

    @task
    def prepare_rows(weather_data: list[dict]):
        """Transform weather dicts to tuples for SQLInsertRowsOperator."""
        return [
            (w["planet_id"], w["reading_date"], w["temperature_c"], w["storm_risk"], w["visibility"])
            for w in weather_data
        ]

    planet_ids = extract_planet_ids(_get_planets.output)
    weather_data = fetch_weather.expand(planet_id=planet_ids)
    rows = prepare_rows(weather_data)

    _insert_rows = SQLInsertRowsOperator(
        task_id="load_weather",
        conn_id=_DUCKDB_CONN_ID,
        table_name="planet_weather",
        columns=["planet_id", "reading_date", "temperature_c", "storm_risk", "visibility"],
        rows=rows,
        preoperator="DELETE FROM planet_weather WHERE reading_date = '{{ ds }}';",
    )

    chain(rows, _insert_rows)


weather_ingest()
