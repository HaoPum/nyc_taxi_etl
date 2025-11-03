import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, year, month, dayofmonth
    from pyspark.sql.types import *
except ImportError:
    print("PySpark not available in this environment")
    # For linting purposes when PySpark is not installed

spark = SparkSession.builder \
        .appName("NYC Taxi to Iceberg ETL") \
        .master("local") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


data = spark.sql("""
        SELECT *
        FROM iceberg.nyc_taxi.trips 
        WHERE 
            YEAR(pickup_datetime) = 2002
            OR
            YEAR(pickup_datetime) = 2008
            OR
            YEAR(pickup_datetime) = 2009
    """).show()

outpath = '/opt/airflow/data/raw/yellow_tripdata_2025-11.parquet'
df = spark.read.parquet(outpath)

df_2002 = df.filter(year(df.tpep_pickup_datetime) == 2002)
df_2008 = df.filter(year(df.tpep_pickup_datetime) == 2008)
df_2009 = df.filter(year(df.tpep_pickup_datetime) == 2009)

df_2002.show()
df_2008.show()
df_2009.show()