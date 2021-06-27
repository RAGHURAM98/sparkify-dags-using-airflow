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

dag = DAG('sparkify_create_table',
          default_args=default_args,
          description='Create tables in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='@once'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

## Creating tables first in Redshift
songplay_table_create = CreateTableRedShiftOperator(
    task_id = 'create_songplays_table',
    dag = dag,
    table = 'songplays',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.songplay_table_create
)

artist_table_create = CreateTableRedShiftOperator(
    task_id = 'create_artist_table',
    dag = dag,
    table = 'artists',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.artist_table_create
)

song_table_create = CreateTableRedShiftOperator(
    task_id = 'create_songs_table',
    dag = dag,
    table = 'songs',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.song_table_create
)

user_table_create = CreateTableRedShiftOperator(
    task_id = 'create_users_table',
    dag = dag,
    table = 'users',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.user_table_create
)

time_table_create = CreateTableRedShiftOperator(
    task_id = 'create_times_table',
    dag = dag,
    table = 'times',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.time_table_create
)

staging_events_table_create = CreateTableRedShiftOperator(
    task_id = 'create_staging_events_table',
    dag = dag,
    table = 'staging_events',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.staging_events_table_create
)

staging_songs_table_create = CreateTableRedShiftOperator(
    task_id = 'create_staging_songs_table',
    dag = dag,
    table = 'staging_songs',
    redshift_conn_id = 'redshift',
    create_table_sql = SqlQueries.staging_songs_table_create
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#Setup Task Dependacies

start_operator >> songplay_table_create
start_operator >> artist_table_create
start_operator >> song_table_create
start_operator >> user_table_create
start_operator >> time_table_create
start_operator >> staging_events_table_create
start_operator >> staging_songs_table_create

artist_table_create >> end_operator
song_table_create >> end_operator
user_table_create >> end_operator
time_table_create >> end_operator
songplay_table_create >> end_operator
staging_songs_table_create >> end_operator
staging_events_table_create >> end_operator