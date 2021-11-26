from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import requests

MYSQL_CONNECTION = "mysql_default" 
mysql_output_path = "/home/airflow/gcs/data/covid19_data.csv"

def get_data_from_mysql(transaction_path):
    # รับ transaction_path มาจาก task ที่เรียกใช้

    # เรียกใช้ MySqlHook เพื่อต่อไปยัง MySQL จาก connection ที่สร้างไว้ใน Airflow
    mysqlserver = MySqlHook(MYSQL_CONNECTION)
    
    # Query จาก database โดยใช้ Hook ที่สร้าง ผลลัพธ์ได้ pandas DataFrame
    df = mysqlserver.get_pandas_df(sql="SELECT * FROM covid_19_data")
    # Save ไฟล์ CSV ไปที่ transaction_path ("/home/airflow/gcs/data/covid19_datae_transaction.csv")
    # จะไปอยู่ที่ GCS โดยอัตโนมัติ
    df.to_csv(transaction_path, index=False, encoding='utf-8', header=None)
    print(f"Output to {transaction_path}")




default_args = {
    'owner': 'thitivut',
    'depends_on_past': False,
    'email': ['thitivut.com@hotmail.co.th'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='covid-19-project',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
) as dag:

    t1 = PythonOperator(
        task_id="get_data_from_mysql",
        python_callable=get_data_from_mysql,
        op_kwargs={
            "transaction_path": mysql_output_path,
        },
    )

    t2 = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket='asia-east2-covid-19-project-71f4f4f8-bucket',
        source_objects=['data/covid19_data.csv'],
        destination_project_dataset_table='covid19_dataset.covid19_data',
        schema_fields=[
            {
                "mode": "NULLABLE",
                "name": "date",
                "type": "DATE"
            },
            {
                "mode": "NULLABLE",
                "name": "user_id",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "gender",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "risk",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "province",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "age_number",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "nationality",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "job",
                "type": "STRING"
            },
        ],
        write_disposition='WRITE_TRUNCATE',
    )


    
t1 >> t2

