"""
Spark Batch Processor
Processes historical movie viewing data from MinIO
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
from pyspark.sql.functions import hour, date_format, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from minio import Minio
from minio.error import S3Error
import yaml
import os
import json

def load_config():
    """Load configuration from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def setup_minio_client(config):
    """Setup MinIO client"""
    minio_config = config['minio']
    return Minio(
        minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )

def create_spark_session(config):
    """Create Spark session with MinIO S3 support"""
    spark_config = config['spark']
    
    spark = SparkSession.builder \
        .appName(spark_config['app_name'] + "_Batch") \
        .master(spark_config['master']) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['minio']['endpoint']}") \
        .config("spark.hadoop.fs.s3a.access.key", config['minio']['access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config['minio']['secret_key']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    return spark

def read_batch_data_from_minio(spark, config):
    """Read batch data from MinIO"""
    bucket = config['minio']['bucket']
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("movie_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("rating", IntegerType(), True)
    ])
    
    s3_path = f"s3a://{bucket}/batch/events/"
    
    try:
        df = spark.read.json(s3_path, schema=schema)
        return df
    except Exception as e:
        print(f"Error reading from MinIO: {e}")
        return None

def process_batch_analytics(df):
    """Perform batch analytics on movie viewing data"""
    
    # Viewing statistics by user
    user_stats = df.groupBy("user_id").agg(
        count("*").alias("total_events"),
        count("movie_id").alias("movies_watched"),
        avg("duration").alias("avg_duration")
    )
    
    # Popular movies
    popular_movies = df.filter(col("event_type") == "view") \
        .groupBy("movie_id") \
        .agg(count("*").alias("view_count"))
    
    # Ratings statistics
    ratings_stats = df.filter(col("rating").isNotNull()) \
        .groupBy("movie_id") \
        .agg(
            avg("rating").alias("avg_rating"),
            count("*").alias("rating_count")
        )
    
    # Time-based analysis
    df_with_time = df.withColumn("ts", to_timestamp(col("timestamp"))) \
        .withColumn("hour", hour(col("ts")))
    hourly_stats = df_with_time.groupBy("hour") \
        .agg(count("*").alias("event_count"))
    
    return {
        'user_stats': user_stats,
        'popular_movies': popular_movies,
        'ratings_stats': ratings_stats,
        'hourly_stats': hourly_stats
    }

def write_to_clickhouse(df, table_name, config):
    """Write DataFrame to ClickHouse"""
    clickhouse_config = config['clickhouse']
    
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:clickhouse://{clickhouse_config['host']}:8123/{clickhouse_config['database']}") \
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
        .option("dbtable", table_name) \
        .option("user", clickhouse_config['user']) \
        .option("password", clickhouse_config['password']) \
        .mode("overwrite") \
        .save()

def main():
    """Main batch processing function"""
    config = load_config()
    
    spark = create_spark_session(config)
    print("Spark session created")
    
    # Read batch data
    df = read_batch_data_from_minio(spark, config)
    if df is None or df.count() == 0:
        print("No data found in MinIO. Please ensure data is loaded.")
        return
    
    print(f"Loaded {df.count()} events from MinIO")
    
    # Process analytics
    results = process_batch_analytics(df)
    
    # Write results to ClickHouse
    print("Writing batch results to ClickHouse...")
    write_to_clickhouse(results['user_stats'], "batch_user_stats", config)
    write_to_clickhouse(results['popular_movies'], "batch_popular_movies", config)
    write_to_clickhouse(results['ratings_stats'], "batch_ratings_stats", config)
    write_to_clickhouse(results['hourly_stats'], "batch_hourly_stats", config)
    
    print("Batch processing completed")
    spark.stop()

if __name__ == "__main__":
    main()
