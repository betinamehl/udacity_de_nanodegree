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

dag = DAG('election_data_dag',
          default_args=default_args,
          description='Load and transform brazil election data in Redshift',
          schedule_interval='0 * * * *',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

wait_operator = DummyOperator(task_id='Load_complete',  dag=dag)


stage_candidates_data_to_redshift = StageToRedshiftOperator(
    task_id='stage_candidates_data_task',
    dag=dag,
    redshift_conn_id='redshift',
    create_table_query=SqlQueries.create_staging_candidates,
    destination_schema='raw_data',
    destination_table='election_candidates',
    s3_path='s3://udacity-brazil-election-votes/consulta_cand/',
    file_format= 'csv',
    copy_parameters=" delimiter as ',' truncatecolumns IGNOREHEADER 1 blanksasnull emptyasnull",
    aws_credentials='aws_credentials'
)

stage_votes_data_to_redshift = StageToRedshiftOperator(
    task_id='stage_votes_data_task',
    dag=dag,
    redshift_conn_id='redshift',
    create_table_query=SqlQueries.create_staging_votes,
    destination_schema='raw_data',
    destination_table='election_votes',
    s3_path='s3://udacity-brazil-election-votes/votacao_secao/',
    file_format= 'csv',
    copy_parameters="delimiter as ',' truncatecolumns IGNOREHEADER 1 blanksasnull emptyasnull",
    aws_credentials='aws_credentials'
)


load_dim_candidate_table = LoadFactDimensionOperator(
    task_id='load_dim_candidate_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="dim",
    destination_table="candidate",
    create_table_query=SqlQueries.create_dim_candidate,
    insert_table_query=SqlQueries.insert_dim_candidate
)

load_dim_date_table = LoadFactDimensionOperator(
    task_id='load_dim_date_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="dim",
    destination_table="date",
    create_table_query=SqlQueries.create_dim_date,
    insert_table_query=SqlQueries.insert_dim_date
)

load_dim_election_table = LoadFactDimensionOperator(
    task_id='load_dim_election_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="dim",
    destination_table="election",
    create_table_query=SqlQueries.create_dim_election,
    insert_table_query=SqlQueries.insert_dim_election
)

load_dim_party_table = LoadFactDimensionOperator(
    task_id='load_dim_party_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="dim",
    destination_table="party",
    create_table_query=SqlQueries.create_dim_party,
    insert_table_query=SqlQueries.insert_dim_party
)

load_dim_location_table = LoadFactDimensionOperator(
    task_id='load_dim_location_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="dim",
    destination_table="location",
    create_table_query=SqlQueries.create_dim_location,
    insert_table_query=SqlQueries.insert_dim_location
)

load_fact_votes_table = LoadFactDimensionOperator(
    task_id='load_fact_votes_table_task',
    dag=dag,
    redshift_conn_id="redshift",
    destination_schema="fact",
    destination_table="votes",
    create_table_query=SqlQueries.create_fact_votes,
    insert_table_query=SqlQueries.insert_fact_votes
)

run_quality_checks_dim_location = DataQualityOperator(
    task_id='run_quality_checks_dim_location_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='dim',
    table='location',
    id_column='id',
    checks=['unique_key',
            'load_successful']
)

run_quality_checks_dim_date = DataQualityOperator(
    task_id='run_quality_checks_dim_date_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='dim',
    table='date',
    id_column='date',
    checks=['unique_key',
            'load_successful']
)

run_quality_checks_dim_election = DataQualityOperator(
    task_id='run_quality_checks_dim_election_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='dim',
    table='election',
    id_column='id',
    checks=['unique_key',
            'load_successful']
)

run_quality_checks_dim_candidate = DataQualityOperator(
    task_id='run_quality_checks_dim_candidate_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='dim',
    table='candidate',
    id_column='cpf_number',
    checks=['unique_key',
            'load_successful']
)

run_quality_checks_dim_party = DataQualityOperator(
    task_id='run_quality_checks_dim_party_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='dim',
    table='party',
    id_column='id',
    checks=['unique_key',
            'load_successful']
)

run_quality_checks_fact_votes = DataQualityOperator(
    task_id='run_quality_checks_fact_votes_task',
    dag=dag,
    redshift_conn_id='redshift',
    table_schema='fact',
    table='votes',
    checks=['load_successful']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_candidates_data_to_redshift >> wait_operator
start_operator >> stage_votes_data_to_redshift >> wait_operator

wait_operator >> load_dim_candidate_table >> run_quality_checks_dim_candidate >> end_operator
wait_operator >> load_dim_date_table >> run_quality_checks_dim_date >> end_operator
wait_operator >> load_dim_party_table >> run_quality_checks_dim_party >> end_operator
wait_operator >> load_dim_location_table >> run_quality_checks_dim_location >> end_operator
wait_operator >> load_dim_election_table >> run_quality_checks_dim_election >> end_operator
wait_operator >> load_fact_votes_table >> run_quality_checks_fact_votes >> end_operator



