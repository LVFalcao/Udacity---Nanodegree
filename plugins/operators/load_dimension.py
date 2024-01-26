from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Airflow operator :: load data into dimensional table from staging tables (events and song)
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info('Begin Dimensional - Copying data from staging to dimensional table')
        redshift = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating dimension table - {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info("Inserting data to dimension table")
        formatted_sql = self.sql.format(self.table)
        redshift.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")

        self.log.info('End Dimensional - Copying data from staging to dimensional table')
