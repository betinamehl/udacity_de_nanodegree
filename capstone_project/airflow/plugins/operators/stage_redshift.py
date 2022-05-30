from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_path",)
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 create_table_query="",
                 s3_path="",
                 aws_credentials="",
                 format_type="",
                 destination_schema="",
                 destination_table="",
                 copy_parameters="",
                 file_format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_table_query = create_table_query
        self.s3_path=s3_path
        self.aws_credentials=aws_credentials
        self.format_type=format_type
        self.destination_schema=destination_schema
        self.destination_table=destination_table
        self.copy_parameters=copy_parameters
        self.file_format=file_format
        

    def execute(self, context):
        
        
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Successfully connected to Redshift')
        
        redshift.run(f'DROP TABLE IF EXISTS {self.destination_schema}.{self.destination_table}')
        self.log.info(f'Successfully dropped {self.destination_schema}.{self.destination_table} table in Redshift')

        redshift.run(self.create_table_query)
        self.log.info(f'Successfully created {self.destination_table} table in Redshift')
        
        copy_statement_template = """
                                    copy {destination_schema}.{destination_table}
                                    from '{s3_path}'
                                    ACCESS_KEY_ID '{aws_credentials_key_id}'
                                    SECRET_ACCESS_KEY '{aws_credentials_secret_key}'
                                    compupdate off region 'us-west-2'
                                    format as {file_format}
                                    {copy_parameters}
                                   
                                    """
                      
        copy_statement = copy_statement_template.format(destination_schema=self.destination_schema,
                                                        destination_table=self.destination_table,
                                                        s3_path=self.s3_path,                               
                                                        aws_credentials_key_id=credentials.access_key,
                                                        aws_credentials_secret_key=credentials.secret_key,
                                                        file_format=self.file_format,
                                                        copy_parameters=self.copy_parameters)

        redshift.run(copy_statement)
        self.log.info(f'Successfully copied data from {self.s3_path} into table {self.destination_schema}.{self.destination_table} in Redshift')
                                                        
 
        
        





