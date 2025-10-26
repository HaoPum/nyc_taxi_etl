from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'Test_Spark_job',
    default_args=default_args,
    description='Test spark job',
    schedule_interval='@daily',
    max_active_runs=1
)

run_spark = SparkSubmitOperator(
    task_id='run_spark_job',
    application='/opt/airflow/dags/spark_jobs/test_sparkjob.py',
    conn_id='spark_default',
    spark_binary='/opt/bitnami/spark/bin/spark-submit',
    conf={'spark.master': 'spark://spark-master:7077'},
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
    dag=dag
)

run_spark