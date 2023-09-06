from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, check_list=[], redshift_conn_id="", *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_list = check_list,
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator starting...')
        self.log.info("Starting connect to DB...")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.check_list[0]:
            self.log.info(check)
            table = check["table"]
            query = check["query"]
            pass_conditition = check["pass_conditition"]
            self.log.info(f"Starting check data quality for {table} table...")
            records = redshift_hook.get_records(query)
            results = records[0][0]
            if results > pass_conditition:
                raise ValueError(f"Data quality check failed. The {table} table had {results} NULL record(s).")
            else:
                self.log.info(f"Data quality check done. The {table} table check passed with {records[0][0]} records")