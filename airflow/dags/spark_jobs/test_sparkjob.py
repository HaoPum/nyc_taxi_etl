from airflow import DAG
from pyspark.sql import SparkSession
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def create_spark_session():
    return SparkSession.builder \
        .appName("Test Spark") \
        .master("local") \
        .config("spark.sql.adaptive.enable","true") \
        .config("spark.sql.adaptive.coalescePartitions.enacle","true") \
        .getOrCreate()

if __name__ == "__main__":
    try:
        spark = create_spark_session()
    except ImportError:
        print("Spark failed")