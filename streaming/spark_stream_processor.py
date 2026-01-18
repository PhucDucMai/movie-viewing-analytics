"""
Spark Streaming Processor
Processes real-time movie viewing events from Kafka
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, window, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import yaml
import os

def load_config():
    """Load configuration from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_spark_session(config):
    """Create Spark session for streaming"""
    spark_config = config['spark']
    
    spark = SparkSession.builder \
        .appName(spark_config['app_name'] + "_Streaming") \
        .master(spark_config['master']) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/streaming-checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_stream_from_kafka(spark, config):
    """Read stream from Kafka"""
    kafka_config = config['kafka']
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
        .option("subscribe", kafka_config['topic']) \
        .option("startingOffsets", "latest") \
        .load()
    
    return df

def parse_json_event(df):
    """Parse JSON events from Kafka"""
    from pyspark.sql.functions import from_json
    
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("movie_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("rating", IntegerType(), True)
    ])
    
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    return parsed_df

def process_streaming_analytics(df):
    """Process real-time analytics on streaming data"""
    
    # Real-time viewing count by movie
    movie_views = df.filter(col("event_type") == "view") \
        .groupBy(
            window(current_timestamp(), "1 minute"),
            col("movie_id")
        ) \
        .agg(count("*").alias("view_count"))
    
    # Real-time user activity
    user_activity = df.groupBy(
            window(current_timestamp(), "1 minute"),
            col("user_id")
        ) \
        .agg(count("*").alias("activity_count"))
    
    # Real-time event type distribution
    event_distribution = df.groupBy(
            window(current_timestamp(), "1 minute"),
            col("event_type")
        ) \
        .agg(count("*").alias("event_count"))
    
    # Real-time ratings
    ratings_stream = df.filter(col("rating").isNotNull()) \
        .groupBy(
            window(current_timestamp(), "5 minutes"),
            col("movie_id")
        ) \
        .agg(
            avg("rating").alias("avg_rating"),
            count("*").alias("rating_count")
        )
    
    return {
        'movie_views': movie_views,
        'user_activity': user_activity,
        'event_distribution': event_distribution,
        'ratings_stream': ratings_stream
    }

def write_to_clickhouse_stream(df, table_name, config):
    """Write streaming DataFrame to ClickHouse"""
    clickhouse_config = config['clickhouse']
    
    query = df.writeStream \
        .outputMode("update") \
        .foreachBatch(lambda batch_df, batch_id: write_batch_to_clickhouse(
            batch_df, table_name, config
        )) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    return query

def write_batch_to_clickhouse(batch_df, table_name, config):
    """Write batch to ClickHouse"""
    try:
        clickhouse_config = config['clickhouse']
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:clickhouse://{clickhouse_config['host']}:8123/{clickhouse_config['database']}") \
            .option("driver", "ru.yandex.clickhouse.ClickHouseDriver") \
            .option("dbtable", table_name) \
            .option("user", clickhouse_config['user']) \
            .option("password", clickhouse_config['password']) \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to ClickHouse: {e}")

def main():
    """Main streaming processing function"""
    config = load_config()
    
    spark = create_spark_session(config)
    print("Spark streaming session created")
    
    # Read stream from Kafka
    kafka_df = read_stream_from_kafka(spark, config)
    print("Reading from Kafka...")
    
    # Parse events
    events_df = parse_json_event(kafka_df)
    
    # Process analytics
    results = process_streaming_analytics(events_df)
    
    # Start streaming queries
    print("Starting streaming queries...")
    queries = []
    
    queries.append(write_to_clickhouse_stream(
        results['movie_views'], "stream_movie_views", config
    ))
    queries.append(write_to_clickhouse_stream(
        results['user_activity'], "stream_user_activity", config
    ))
    queries.append(write_to_clickhouse_stream(
        results['event_distribution'], "stream_event_distribution", config
    ))
    queries.append(write_to_clickhouse_stream(
        results['ratings_stream'], "stream_ratings", config
    ))
    
    # Wait for termination
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        for query in queries:
            query.stop()
    
    spark.stop()

if __name__ == "__main__":
    main()
