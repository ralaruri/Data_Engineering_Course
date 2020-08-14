from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'ramzi_alaruri',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_retry':False,
    'retry_delay':300,
    'retries': 3
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='log_data',
    table = "staging_events",
    copy_options="JSON 's3://udacity-dend/log_json_path.json'"

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_bucket='udacity-dend',
    s3_prefix='song_data',
    table = "staging_songs",
    copy_options="FORMAT AS JSON 'auto'"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.songplay_table_insert,
    table = "songplays"
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.user_table_insert,
    table = "users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.song_table_insert,
    table = "songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "artists",
    sql_query = SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql_query = SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_quality_data_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables = [ "songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task

create_tables_task>>stage_events_to_redshift
create_tables_task>>stage_songs_to_redshift

stage_events_to_redshift>>load_songplays_table
stage_songs_to_redshift>>load_songplays_table

load_songplays_table>>load_song_dimension_table
load_songplays_table>>load_user_dimension_table
load_songplays_table>>load_artist_dimension_table
load_songplays_table>>load_time_dimension_table


load_song_dimension_table>>run_quality_checks
load_user_dimension_table>>run_quality_checks
load_artist_dimension_table>>run_quality_checks
load_time_dimension_table>>run_quality_checks


run_quality_checks>>end_operator



"""
Control Flow we want to
 1. Begin_execution
 2. Run in parellel stage_events + stage_songs
 3. have created the fact table
 4. load the dimesional tables (4 of them) in parellel
 5. run the data run_quality_checks
 6. end the execution
"""
