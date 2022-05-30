from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactDimensionOperator,
                               DataQualityOperator)
from helpers import SqlQueries
from airflow.models import Variable


#/opt/airflow/start.sh

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 5, 29),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('ibge_data_dag',
          default_args=default_args,
          description='Load and transform brazil population data in Redshift',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_population_data_to_redshift = StageToRedshiftOperator(
    task_id='stage_population_data_task',
    dag=dag,
    redshift_conn_id='redshift',
    create_table_query=SqlQueries.create_staging_population,
    destination_schema='raw_data',
    destination_table='brazil_population',
    s3_path='s3://udacity-brazil-election-votes/population/',
    file_format= "json 'auto'",
    copy_parameters="blanksasnull emptyasnull",
    aws_credentials='aws_credentials'
)


load_dim_city_table = LoadFactDimensionOperator(
    task_id='load_dim_city_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="dim",
    destination_table="city",
    create_table_query=SqlQueries.create_dim_city,
    insert_table_query=SqlQueries.insert_dim_city
)

run_quality_checks_dim_city = DataQualityOperator(
    task_id='run_quality_checks_dim_city_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='dim',
    id_column='id',
    table='city',
    checks=['unique_key',
            'load_successful']
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_population_data_to_redshift >> load_dim_city_table >> run_quality_checks_dim_city >> end_operator




