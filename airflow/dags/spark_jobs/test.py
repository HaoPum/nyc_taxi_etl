
import sys
import os

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
except ImportError:
    print("PySpark not available in this environment")

def create_spark_session():
    """Create Spark session with streaming and Iceberg configuration"""
    return SparkSession.builder \
        .appName("Real-time CDC Processor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hadoop") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.warehouse.dir", "s3a://lakehouse/warehouse") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres-airflow:5432/hive_metastore") \
        .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver") \
        .config("javax.jdo.option.ConnectionUserName", "airflow") \
        .config("javax.jdo.option.ConnectionPassword", "airflow") \
        .config("datanucleus.autoCreateSchema", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()


def demand_prediction_features_data(spark):
    df = spark.table("iceberg.ml.demand_prediction_features").show()
    sql = """
        SELECT 
            CORR(target_demand, temperature_celsius) as demand_temp_corr
        FROM
            iceberg.ml.demand_prediction_features
    """
    print(f"df_demand_temp_corr data:")
    df_demand_temp_corr = spark.sql(sql).show()


    sql_null = """
        SELECT location_id, prediction_hour, target_demand 
        FROM
            iceberg.ml.demand_prediction_features
        WHERE 
            target_demand IS NULL OR temperature_celsius IS NULL
    """

    print(f"df_demand_temp_corr_null data:")
    df_demand_temp_corr_null = spark.sql(sql_null).show()

    print("--- Analyzing iceberg.ml.demand_prediction_features table ---")

    # 1. Check for data variation
    stats_sql = """
        SELECT
            -- Statistics for target_demand
            min(target_demand) as min_demand,
            max(target_demand) as max_demand,
            avg(target_demand) as avg_demand,
            stddev(target_demand) as stddev_demand,
            count(CASE WHEN target_demand IS NULL THEN 1 END) as null_demand_count,

            -- Statistics for temperature_celsius
            min(temperature_celsius) as min_temp,
            max(temperature_celsius) as max_temp,
            avg(temperature_celsius) as avg_temp,
            stddev(temperature_celsius) as stddev_temp,
            count(CASE WHEN temperature_celsius IS NULL THEN 1 END) as null_temp_count,
            
            count(*) as total_rows
        FROM iceberg.ml.demand_prediction_features
    """
    print("Calculating statistics for correlation columns:")
    spark.sql(stats_sql).show()
    


def main():
    """Main function""" 
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Starting ML feature engineering pipeline...")
        demand_prediction_features_data(spark)
        
        print("ML feature engineering pipeline completed successfully!")
    except Exception as e:
        print(f"Error in ML feature engineering pipeline: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
