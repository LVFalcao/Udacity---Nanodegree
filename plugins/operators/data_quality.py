from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# Airflow operator :: data quality check
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Begin Data quality - Data quality')

        redshift = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            number_records = redshift.get_records(f'SELECT COUNT(*) FROM {table}')

            if len(number_records) < 1 or len(number_records[0]) < 1 or number_records[0][0] == 0:
                message = f'Data quality check failure: table "{table}" is empty'
                self.log.error(message)
                raise ValueError(message)
            self.log.info(f'Data quality check on table "{table}" passed with {number_records[0][0]} records')

        self.log.info('End Data quality - Data quality')
