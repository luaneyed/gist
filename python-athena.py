from datetime import date
from time import sleep
from typing import List, Dict, Tuple

import boto3
from botocore.client import BaseClient

TABLE_NAME = 'athena_table_name'


class Partition:
    def __init__(self, created_at: date, type: str):
        self.partitions = {'created_at': created_at, 'type': type}

    def implode(self, template: str, glue):
        return glue.join(template.format(k, v) for k, v in self.partitions.items())


class QueryExecution:
    def __init__(self, client: BaseClient, query_execution_id: str):
        self.client = client
        self.query_execution_id = query_execution_id
        self.waited = False

    def wait(self, timeout: int = 20):
        if self.waited:
            return

        for i in range(timeout):
            status = self.client.get_query_execution(QueryExecutionId=self.query_execution_id)['QueryExecution']['Status']
            state = status['State']
            reason = status.get('StateChangeReason', 'No reason is given.')

            if state == 'FAILED':
                raise Exception('Athena execution {} is failed. {}'.format(self.query_execution_id, reason))
            elif state == 'CANCELLED':
                raise Exception('Athena execution {} is canceled. {}'.format(self.query_execution_id, reason))
            elif state == 'SUCCEEDED':
                self.waited = True
                return

            if i != timeout - 1:
                sleep(1)

        raise Exception('Athena execution {} is not processed in {} seconds.'.format(self.query_execution_id, timeout))

    def get_result(self, timeout: int = 20) -> Dict:
        self.wait(timeout)
        return self.client.get_query_results(QueryExecutionId=self.query_execution_id)


class AthenaClient:
    def __init__(self, db_name: str, s3_bucket: str, output_location: str):
        self.athena_client = boto3.client('athena')
        self.db_name = db_name
        self.s3_bucket = s3_bucket
        self.output_location = output_location

    def add_partition(self, partition: Partition, directory: str) -> QueryExecution:
        query_adding_partition_to_athena = 'alter table {} add partition ({}) location "{}/{}";' \
            .format(TABLE_NAME, partition.implode('{}="{}"', ', '), self.s3_bucket, directory)

        return self.execute(query_adding_partition_to_athena)

    def get_count(self) -> int:
        query = f'select count(*) from {TABLE_NAME}'
        column_names, rows = self._execute_select(query)
        return int(rows[0][0])

    def get_some(self) -> List[Dict[str, str]]:
        query = f'select * from {TABLE_NAME} limit 10'
        column_names, rows = self._execute_select(query)
        return [dict(zip(column_names, row)) for row in rows]

    def _execute_select(self, query: str) -> Tuple[List[str], List[List[str]]]:
        rows = self.execute(query).get_result()['ResultSet']['Rows']
        result = [[column_value['VarCharValue'] for column_value in row['Data']] for row in rows]
        return result[0], result[1:]

    def execute_sync(self, query: str, timeout: int = 20):
        query_execution = self.execute(query)
        query_execution.wait(timeout)

    def execute(self, query: str) -> QueryExecution:
        query_execution_id = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': self.db_name,
            },
            ResultConfiguration={
                'OutputLocation': self.output_location,
            }
        )['QueryExecutionId']

        return QueryExecution(self.athena_client, query_execution_id)
