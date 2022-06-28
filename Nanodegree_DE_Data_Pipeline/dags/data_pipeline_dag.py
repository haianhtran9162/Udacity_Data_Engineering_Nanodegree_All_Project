from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


# DAG default parameter rule
# 1. The DAG does not have dependencies on past runs
# 2. On failure, the task are retried 3 times
# 3. Retries happen every 5 minutes
# 4. Catchup is turned off
# 5. Do not email on retry
default_args = {
    'owner': 'anhth9162',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'start_date': datetime(2022, 4, 26)
}

dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path='s3://udacity-dend/log_json_path.json',
    region='us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path='auto',
    region='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    truncate_table=True,
    query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    truncate_table=True,
    query=SqlQueries.artist_table_insert
)

# Check list count NULL value in the table
# check_list = [
#     {"table": "users",
#      "query": "SELECT COUNT(userid) FROM users WHERE userid IS NULL OR first_name IS NULL OR last_name IS NULL OR gender IS NULL OR level IS NULL",
#      "pass_conditition": 0},
#     {"table": "songs",
#      "query": "SELECT COUNT(songid) FROM songs WHERE songid IS NULL OR title IS NULL OR artistid IS NULL OR year IS NULL OR duration IS NULL",
#      "pass_conditition": 0},
#     {"table": "artists",
#      "query": "SELECT COUNT(artistid) FROM artists WHERE artistid IS NULL OR name IS NULL OR location IS NULL OR lattitude IS NULL OR longitude IS NULL",
#      "pass_conditition": 0}]

check_list = [
    {"table": "users",
     "query": "SELECT COUNT(userid) FROM users WHERE userid IS NULL",
     "pass_conditition": 0},
    {"table": "songs",
     "query": "SELECT COUNT(songid) FROM songs WHERE songid IS NULL",
     "pass_conditition": 0},
    {"table": "artists",
     "query": "SELECT COUNT(artistid) FROM artists WHERE artistid IS NULL",
     "pass_conditition": 0}]

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    truncate_table=True,
    query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    check_list=check_list,
    redshift_conn_id='redshift',
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator