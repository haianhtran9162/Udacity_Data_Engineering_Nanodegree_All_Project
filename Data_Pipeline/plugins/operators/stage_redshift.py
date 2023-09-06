from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, table="", redshift_conn_id="", aws_credentials_id="", s3_bucket="", s3_key="", json_path="", region="", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region

    def execute(self, context):
        self.log.info("StageToRedshiftOperator is starting ...")
        
        self.log.info("Start get AWS Info ...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Get AWS Info done.")
        self.log.info("Start copy data S3 to Redshift ...")
        redshift.run(f"DELETE FROM {self.table}")
        s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{s3_key}"
        redshift.run(f"COPY {self.table} FROM '{s3_path}'\
                        ACCESS_KEY_ID '{credentials.access_key}'\
                        SECRET_ACCESS_KEY '{credentials.secret_key}'\
                        FORMAT AS JSON '{self.json_path}'\
                        REGION AS '{self.region}'")
        self.log.info("Copy data S3 to Redshift done.")
    




