from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta, datetime

default_arg = {
    'owner': 'data-engineer',
    'depend_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0)
}

dag = DAG(
    'test_data',
    default_args=default_arg,
    description='test data',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1
)

spark_test_data_task = SparkSubmitOperator(
    task_id='spark_test_data',
    application='/opt/airflow/dags/spark_jobs/test.py',
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.sql.catalog.spark_catalog': 'org.apache.iceberg.spark.SparkSessionCatalog',
        'spark.sql.catalog.spark_catalog.type': 'hive',
        'spark.sql.catalog.iceberg': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.iceberg.type': 'hadoop',
        'spark.sql.catalog.iceberg.warehouse': 's3a://lakehouse/warehouse',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
        'spark.hadoop.fs.s3a.access.key': 'admin',
        'spark.hadoop.fs.s3a.secret.key': 'password',
        'spark.hadoop.fs.s3a.path.style.access': 'true',
        'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    },
    jars='/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar',
    dag=dag
)