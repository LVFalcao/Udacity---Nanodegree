from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.stage_redshift import StageToRedshiftOperator
from airflow.operators.load_fact import LoadFactOperator
from airflow.operators.load_dimension import LoadDimensionOperator
from airflow.operators.data_quality import DataQualityOperator
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=2),
    "catchup": False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def sparkify_datawarehouse():

    start_operator = DummyOperator(task_id='Begin_execution')

    # Begin Staging
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket='udacity_dend',
        s3_key='log_data',
        region='us-west-2',
        extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket='udacity_dend',
        s3_key='song_data',
        region='us-west-2',
        extra_params="JSON 'auto'"
    )
    # End Staging


    # Begin Dimensional Table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        sql=SqlQueries.user_table_insert,
        table='users'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        sql=SqlQueries.song_table_insert,
        table='songs'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        sql=SqlQueries.artist_table_insert,
        table='artists'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        sql=SqlQueries.time_table_insert,
        table='time'
    )
    # End Dimensional Table


    # Begin Fact Table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        query=SqlQueries.songplay_table_insert,
        append=False
    )
    # End Fact Table

    # Begin Data Quality
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['users', 'songs', 'artists', 'time', 'songplays']
    )
    # End Data Quality

    end_operator = DummyOperator(task_id='End_execution')


    # Task ordering for the DAG tasks
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

sparkify_datawarehouse_dag = sparkify_datawarehouse()