from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS {} '{}';
    """


    @apply_defaults
    def __init__(self,
                 # Defining operator parameters here
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 region="us-west-2",
                 data_format="",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.region = region
        self.data_format = data_format
        self.json_path=json_path


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("Deleting data from destination Redshift table")
        redshift_hook.run("DELETE FROM {}".format(self.table))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.data_format,
                self.json_path
            )
        self.log.info("Copying data from S3 to Redshift")
        redshift_hook.run(formatted_sql)