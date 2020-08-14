from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="",
                 sql_query="",
                 append_data=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query=sql_query
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading Fact Table.')
        if self.append_data == True: 
            sql = """INSERT INTO {table}
                 {sql_query};""".format(table=self.table, sql_query=self.sql_query)
            self.log.info(f'Loading data into {self.table} fact table')
            redshift_hook.run(sql)
        else: 
            sql = """DELETE FROM {table} {sql_query};""".format(table=self.table, sql_query=self.sql_query)
            self.log.info(f'Deleting data into {self.table} fact table')
            redshift_hook.run(sql)
            sql = """INSERT INTO {table} {sql_query};""".format(table=self.table, sql_query=self.sql_query)
            self.log.info(f'Loading data into {self.table} fact table')
            redshift_hook.run(sql)