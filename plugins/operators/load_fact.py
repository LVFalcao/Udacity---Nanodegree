from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# Airflow operator :: load data into fact table from staging tables (events and song)
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 append="False",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.append = append

    def execute(self, context):
        self.log.info('Begin Factual - Copying data from staging to factual table')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.append:
            self.log.info(f"Appending to Redshift table {self.table}")
            query = f"""
                        CREATE TEMP TABLE temp_{self.table} (LIKE {self.table});

                        INSERT INTO temp_{self.table}
                        {self.query};

                        DELETE FROM {self.table}
                        USING temp_{self.table};

                        INSERT INTO {self.table}
                        SELECT * FROM temp_{self.table};
                    """
            redshift_hook.run(query)
            self.log.info(f"Completed appending to Redshift table {self.table}")

        else:
            self.log.info(f"Clearing data from Redshift table {self.table}")
            redshift_hook.run(f"DELETE FROM {self.table}")

            self.log.info(f"Insert data to Redshift table {self.table}")
            query = f"""
                        INSERT INTO {self.table}
                        {self.query}
                    """
            redshift_hook.run(query)

        self.log.info('End Factual - Copying data from staging to factual table')