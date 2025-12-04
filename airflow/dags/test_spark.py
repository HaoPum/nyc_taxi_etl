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

# run_spark = SparkSubmitOperator(
#     task_id='run_spark_job',
#     application='/opt/airflow/dags/spark_jobs/test_sparkjob.py',
#     conn_id='spark_default',
#     spark_binary='/opt/bitnami/spark/bin/spark-submit',
#     conf={'spark.master': 'spark://spark-master:7077'},
#     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
#     dag=dag
# )


reference_taxi_zones = SparkSubmitOperator(
    task_id='realtime_cdc_processing',
    application='/opt/airflow/dags/spark_jobs/test.py',
    conn_id='spark_default',
    conf={
        # 'spark.master': 'spark://spark-master:7077',
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
    jars='/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/airflow/jars/kafka-clients-3.4.1.jar,/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/airflow/jars/commons-pool2-2.11.1.jar',
    dag=dag
)

reference_taxi_zones