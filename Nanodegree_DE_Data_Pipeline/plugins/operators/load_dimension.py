from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self, table="", redshift_conn_id="", truncate_table=False, query="", *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table
        self.query = query

    def execute(self, context):
        self.log.info("LoadDimensionOperator is starting ...")
        self.log.info("Starting connect to DB...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            self.log.info("Truncate the table ...")
            redshift.run(f"TRUNCATE TABLE {self.table}")
            self.log.info("Truncate the table done.")
        self.log.info(f"Insert data into the table {self.table}...")
        redshift.run(f"INSERT INTO {self.table} {self.query}")
        self.log.info("Done.")
