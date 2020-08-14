from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="redshift",
                 sql_query="",
                 append_data = True,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append_data = append_data

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading Dimension table.')
        if self.append_data == True: 
            sql = """INSERT INTO {table}
                 {sql_query};""".format(table=self.table, sql_query=self.sql_query)
            self.log.info(f'Loading data into {self.table} Dimension table')
            redshift_hook.run(sql)
        else: 
            sql = """DELETE FROM {table} {sql_query};""".format(table=self.table, sql_query=self.sql_query)
            self.log.info(f'Deleting data into {self.table} Dimension table')
            redshift_hook.run(sql)
            sql = """INSERT INTO {table} {sql_query};""".format(table=self.table, sql_query=self.sql_query)
            self.log.info(f'Loading data into {self.table} Dimension table')
            redshift_hook.run(sql)