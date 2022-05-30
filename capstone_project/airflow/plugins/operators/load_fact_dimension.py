from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 destination_schema="",
                 destination_table="",
                 create_table_query="",
                 insert_table_query="",
                 *args, **kwargs):

        super(LoadFactDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_schema=destination_schema
        self.destination_table=destination_table
        self.create_table_query=create_table_query
        self.insert_table_query=insert_table_query

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Successfully connected to Redshift')
        
        redshift.run(f"DROP TABLE IF EXISTS {self.destination_schema}.{self.destination_table}")
        self.log.info(f'Successfully dropped table {self.destination_schema}.{self.destination_table} in Redshift')
            
        redshift.run(f"CREATE TABLE IF NOT EXISTS {self.destination_schema}.{self.destination_table} ({self.create_table_query})")
        self.log.info(f'Successfully dropped table {self.destination_schema}.{self.destination_table} in Redshift')
            
        redshift.run(f"INSERT INTO {self.destination_schema}.{self.destination_table} ({self.insert_table_query})")
        self.log.info(f'Successfully inserted data into table {self.destination_table} in Redshift')
