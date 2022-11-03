# -*- coding: utf-8 -*-

from google.cloud.bigquery import Client
from google.cloud.bigquery import Table
from google.cloud.bigquery import TimePartitioning, TimePartitioningType
from google.cloud.bigquery import LoadJobConfig
from google.cloud.exceptions import NotFound


class BigQuery:
    """
    BigQuery上のテーブルを操作
    """
    def __init__(self, table_id, schema, time_partitioning_field=None, clustering_fields=None):
        self.client = Client()
        self.table_id = table_id
        self.schema = schema
        self.time_partitioning_field = time_partitioning_field
        self.clustering_fields = clustering_fields

    def create_table_if_not_exits(self):
        """
        テーブルが存在しない場合は新規作成
        """
        try:
            self.client.get_table(self.table_id)
            print("Table {} already exists.".format(self.table_id))
        except NotFound:
            table = Table(self.table_id, schema=self.schema)
            if self.time_partitioning_field:
                table.time_partitioning = TimePartitioning(
                    type_=TimePartitioningType.DAY,
                    field=self.time_partitioning_field,
                    expiration_ms=None
                )
            if self.clustering_fields:
                table.clustering_fields = self.clustering_fields
            self.client.create_table(table)
            print("Table {} is created.".format(self.table_id))

    def update(self, df, start, end, timestamp_col):
        """
        指定した期間のデータを削除し最新のデータを挿入
        """
        self.delete_rows(start, end, timestamp_col)
        self.insert_rows(df)

    def delete_rows(self, start, end, timestamp_col):
        """
        指定した期間のデータを削除
        """
        query = """
            DELETE
            FROM {table_id}
            WHERE 
                {timestamp_col} >= TIMESTAMP('{start}', 'Asia/Tokyo')
                AND
                {timestamp_col} <= TIMESTAMP('{end}', 'Asia/Tokyo')
        """.format(
            table_id=self.table_id, 
            start=start, 
            end=end,
            timestamp_col=timestamp_col
        )
        job = self.client.query(query)
        job.result()
        print("Deleted existing rows from {} to {}.".format(start, end))

    def insert_rows(self, df):
        """
        DataFrameをテーブルに挿入
        """
        job_config = LoadJobConfig(schema=self.schema, write_disposition='WRITE_APPEND')
        job = self.client.load_table_from_dataframe(df, self.table_id, job_config=job_config)
        job.result()
        print("Inserted updated rows.")
