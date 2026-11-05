import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 check_sql="",
                 expected_value="",
                 describe="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_sql = check_sql
        self.expected_value = expected_value
        self.describe = describe

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("\n".join([
            'DataQuality check',
            self.describe,
            'expected value is {}'.format(self.expected_value),
            self.check_sql
        ]))

        records = redshift_hook.get_records(self.check_sql)
        if not records or not records[0] or records[0][0] is None:
            raise ValueError(f"Data quality check failed: {self.describe} returned no results")
        actual = records[0][0]
        if actual < 1:
            raise ValueError(f"Data quality check failed: {self.describe} returned 0 rows")
        if int(self.expected_value) != actual:
            raise ValueError(f"Data quality check failed. \n expected: {self.expected_value} \n actual: {actual}")
        self.log.info(f"Data quality on \n {self.describe} \n check passed with \n expected: {self.expected_value} \n actual: {actual}")
