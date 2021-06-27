from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.create_tables import CreateTableRedShiftOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries,DataQualitySqlQueries

default_args = {
    'start_date': datetime(2021, 6, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_ETL',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_path="s3://udacity-dend/log_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    data_format="JSON",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_path="s3://udacity-dend/song_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    data_format="JSON",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    fields="songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent",
    table="songplays",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    table="users",
    fields="user_id, first_name, last_name, gender, level",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    table="songs",
    fields="song_id, title, artist_id, year, duration",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    table="artists",
    fields="artist_id, name, location, latitude, longitude",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    table="time",
    fields="start_time, hour, day, week, month, year, weekday",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    append_only=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_queries=DataQualitySqlQueries.dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Setup Task Dependacies

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_time_dimension_table


load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks

run_quality_checks >> end_operator