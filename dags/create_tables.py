import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from custom_operators import StaticQueryOperator
from helpers import SqlQueriesDDL

default_args = {
    "owner": "lf",
    "start_date": pendulum.datetime(2024, 1, 26),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=1),
    "catchup": False,
}


@dag(
    description="Create the tables for the Data Warehouse",
    schedule_interval="@once",
    default_args=default_args,
)
def create_tables():
    start_operator = EmptyOperator(task_id="Begin_execution")

    create_staging_events_table = StaticQueryOperator(
        task_id="create_staging_events_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_staging_events_table,
    )

    create_staging_songs_table = StaticQueryOperator(
        task_id="create_staging_songs_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_staging_songs_table,
    )

    create_artists_table = StaticQueryOperator(
        task_id="create_artists_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_artists_table,
    )

    create_songs_table = StaticQueryOperator(
        task_id="create_songs_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_songs_table,
    )

    create_time_table = StaticQueryOperator(
        task_id="create_time_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_time_table,
    )

    create_users_table = StaticQueryOperator(
        task_id="create_users_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_users_table,
    )

    create_songplays_table = StaticQueryOperator(
        task_id="create_songplays_table",
        redshift="redshift",
        query=SqlQueriesDDL.create_songplays_table,
    )

    end_operator = EmptyOperator(task_id="End_execution")

    start_operator >> create_staging_events_table
    start_operator >> create_staging_songs_table
    start_operator >> create_artists_table
    start_operator >> create_songs_table
    start_operator >> create_time_table
    start_operator >> create_users_table
    create_users_table >> create_songplays_table
    create_songplays_table >> end_operator

create_tables_dag = create_tables()