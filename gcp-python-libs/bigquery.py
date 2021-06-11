import logging

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict


class DotzBigQueryClient:
    """BigQuery client for common bigquery operations"""

    def __init__(self, project):
        self.project = project
        self.client = bigquery.Client(project=project)

    def create_dataset(self, dataset, project=None, description=""):
        dataset_project = self.project if not project else project
        dataset = bigquery.Dataset(f"{dataset_project}.{dataset}")
        try:
            bq_dataset = self.client.get_dataset(dataset)
        except NotFound:
            dataset.description = description
            bq_dataset = self.client.create_dataset(dataset)
        return bq_dataset

    def check_table_exists(self, table):
        try:
            table = self.client.get_table(table)
            exists = True
        except NotFound:
            exists = False
        return exists
        
    def create_table(
        self, table, schema, partition_field=None, load_time_partition=False, partition_type="DAY"
    ):
        if partition_field and load_time_partition:
            raise ValueError("partition_field and load_time_partition can't be set at the same time.")
        try:
            table = self.client.get_table(table)
        except NotFound:
            table = bigquery.Table(table, schema)
            if partition_field:
                time_partitioning = bigquery.TimePartitioning(
                    type_=partition_type,
                    field=partition_field,
                )
                table.time_partitioning = time_partitioning
            elif load_time_partition:
                time_partitioning = bigquery.TimePartitioning(
                    type_=partition_type,
                )
                table.time_partitioning = time_partitioning

            self.client.create_table(table)
        return table

    def create_view(self, view_name, query_view):
        try:
            view = self.client.get_table(view_name)
        except NotFound:
            view = bigquery.Table(view_name)
            view.view_query = query_view
            view.view_use_legacy_sql = False
            self.client.create_table(view)
        return view

    def create_hive_partitioned_external_table(
        self, dataset, table, source_format, source_uri_prefix
    ):

        hive_options = bigquery.external_config.HivePartitioningOptions()
        hive_options.mode = "AUTO"
        hive_options.source_uri_prefix = source_uri_prefix

        external_config = bigquery.ExternalConfig(source_format.upper())
        external_config.hive_partitioning = hive_options
        external_config.source_uris = [f"{source_uri_prefix}*.{source_format.lower()}"]

        table = bigquery.Table(f"{self.project}.{dataset}.{table}")
        table.external_data_configuration = external_config

        try:
            self.client.create_table(table)
        except Conflict:
            pass
        return table

    def update_table_schema(self, dataset, table, schema):
        table = self.client.get_table(f"{dataset}.{table}")
        table.schema = schema
        return self.client.update_table(table, fields=["schema"])
    
    def query(self, sql):
        query = self.client.query(sql)
        return query.result().pages

    def authorize_view(self, dataset, shared_dataset, view, shared_project=None):
        """Authorizes a view in a shared dataset, possibly in another project."""
        if not shared_project:
            shared_project = self.project
        shared_dataset_ref = self.client.get_dataset(f"{shared_project}.{shared_dataset}")
        view_ref = shared_dataset_ref.table(view)
        dataset_ref = self.client.get_dataset(dataset)
        access_entries = dataset_ref.access_entries
        access_entries.append(bigquery.AccessEntry(role=None, entity_type="view", entity_id=view_ref.to_api_repr()))
        dataset_ref.access_entries = access_entries
        return self.client.update_dataset(dataset_ref, fields=["access_entries"])
