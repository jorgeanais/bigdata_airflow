# Copyright 2023 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.cloud_sql import \
    CloudSQLExportInstanceOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
import pendulum # Usar pendulum es el estándar moderno en Airflow

args = {
    'owner': 'mad-developer',
}

GCP_PROJECT_ID = 'teaching-494213'
INSTANCE_NAME = 'mysql-instance-source'
BUCKET_NAME = 'bigdata-data-bucket'
EXPORT_URI = 'gs://{BUCKET}/mysql_export/from_composer/stations/stations.csv'.format(
    BUCKET=BUCKET_NAME)
SQL_QUERY = "SELECT * FROM apps_db.stations"

export_body = {
    "exportContext": {
        "fileType": "csv",
        "uri": EXPORT_URI,
        "csvExportOptions": {
            "selectQuery": SQL_QUERY
        }
    }
}

with DAG(
    dag_id='level_2_dag_load_bigquery',
    default_args=args,
    schedule='0 5 * * *',
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    catchup=False,
) as dag:

    sql_export_task = CloudSQLExportInstanceOperator(
        task_id='sql_export_task',
        project_id=GCP_PROJECT_ID,
        body=export_body,
        instance=INSTANCE_NAME,
    )

    gcs_to_bq_example = GCSToBigQueryOperator(
        task_id="gcs_to_bq_example",
        bucket=BUCKET_NAME,
        source_objects=['mysql_export/from_composer/stations/stations.csv'],
        destination_project_dataset_table='raw_bikesharing.stations',
        schema_fields=[
            {'name': 'station_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'region_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'capacity', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_TRUNCATE'
    )

    bq_to_bq = BigQueryInsertJobOperator(
        task_id="bq_to_bq",
        configuration={
                "query": {
                    "query": "SELECT count(*) as count FROM `raw_bikesharing.stations`",
                    "useLegacySql": False,
                    "destinationTable": {
                        "projectId": GCP_PROJECT_ID,
                        "datasetId": "dwh_bikesharing",
                        "tableId": "temporary_stations_count"
                    },
                    "createDisposition": 'CREATE_IF_NEEDED',
                    "writeDisposition": 'WRITE_TRUNCATE',
                    "priority": "BATCH",
                }
        }
    )

    sql_export_task >> gcs_to_bq_example >> bq_to_bq

