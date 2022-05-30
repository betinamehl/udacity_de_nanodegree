from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_schema="",
                 table="",
                 id_column="",
                 checks="",
                 *args, **kwargs):
       

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_schema=table_schema
        self.table=table
        self.id_column=id_column
        self.checks=checks


    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Successfully connected to Redshift')
        
        ##UNIQUE KEY
        if 'unique_key' in self.checks:
            
            unique_id_template = f"""with base as (
                                        select 
                                            {self.id_column},
                                            row_number() over (partition by {self.id_column}) as row_n
                                         from {self.table_schema}.{self.table}
                                    )
                                 
                                    select count(*) from base where row_n > 1
                               """
            
            num_of_records = redshift.get_records(unique_id_template)[0][0]
            
            if num_of_records == 0:
                self.log.info(f'Table {self.table_schema}.{self.table} successfully passed the unique key quality check.')
            else:
                raise AirflowException(f"Table {self.table_schema}.{self.table} did not pass the quality checks")
            
         ##LOAD SUCCESSFULL
        if 'load_successful' in self.checks:
            
            load_successful_template = f""" 
                                            select 
                                                count(*)
                                            from {self.table_schema}.{self.table}

                                            """
            
            num_of_records = redshift.get_records(load_successful_template)[0][0]
            
            if num_of_records > 0:
                self.log.info(f'Table {self.table_schema}.{self.table} successfully passed the load successful quality check.')
            else:
                raise AirflowException(f"Table {self.table_schema}.{self.table} did not pass the quality checks")
        
        
        
        
            
        
        