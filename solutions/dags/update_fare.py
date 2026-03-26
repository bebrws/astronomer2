from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.sdk import dag, task, chain, Param

_DUCKDB_CONN_ID = "duckdb_astrotrips"


@dag(tags=["astrotrips", "hitl"])
def update_fare():

    _get_fares = SQLExecuteQueryOperator(
        task_id="get_current_fares",
        conn_id=_DUCKDB_CONN_ID,
        sql="""
            SELECT r.route_id, p.planet_name, r.base_fare_usd
            FROM routes r
            JOIN planets p ON p.planet_id = r.destination_id
            ORDER BY r.route_id
        """
    )

    @task
    def format_fares(rows):
        """Format fare rows for display in HITL body."""
        return "\n".join([f"Route {r[0]}: {r[1]} - ${r[2]}" for r in rows])

    _formatted_fares = format_fares(_get_fares.output)

    _hitl_input = HITLEntryOperator(
        task_id="input_new_fare",
        subject="Update route base fare",
        body="Current fares:\n\n{{ ti.xcom_pull(task_ids='format_fares') }}",
        params={
            "route_id": Param(1, type="integer", description="Route ID to update"),
            "new_fare": Param(1000, type="integer", description="New base fare in USD"),
        },
    )

    @task
    def build_update_params(hitl_output):
        """Extract params from HITL output for the UPDATE query."""
        return {
            "new_fare": hitl_output["params_input"]["new_fare"],
            "route_id": hitl_output["params_input"]["route_id"],
        }

    _update_params = build_update_params(_hitl_input.output)

    _apply_update = SQLExecuteQueryOperator(
        task_id="apply_fare_update",
        conn_id=_DUCKDB_CONN_ID,
        sql="UPDATE routes SET base_fare_usd = $new_fare WHERE route_id = $route_id",
        parameters=_update_params,
    )

    _get_updated_fares = SQLExecuteQueryOperator(
        task_id="get_updated_fares",
        conn_id=_DUCKDB_CONN_ID,
        sql="""
            SELECT r.route_id, p.planet_name, r.base_fare_usd
            FROM routes r
            JOIN planets p ON p.planet_id = r.destination_id
            ORDER BY r.route_id
        """,
    )

    @task
    def print_updated_fares(rows):
        """Print the updated fares to logs."""
        print("::group::Updated Route Fares")
        print("Route ID | Destination | Base Fare")
        print("-" * 40)
        for row in rows:
            print(f"{row[0]:8} | {row[1]:11} | ${row[2]}")
        print("::endgroup::")

    chain(
        _formatted_fares,
        _hitl_input,
        _update_params,
        _apply_update,
        _get_updated_fares,
        print_updated_fares(_get_updated_fares.output)
    )


update_fare()
