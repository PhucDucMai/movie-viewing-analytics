"""
ClickHouse Client Utilities
Helper functions for interacting with ClickHouse database
"""

import clickhouse_connect
import yaml
import os

def load_config():
    """Load configuration from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def create_client(config=None):
    """Create ClickHouse client connection"""
    if config is None:
        config = load_config()
    
    clickhouse_config = config['clickhouse']
    
    client = clickhouse_connect.get_client(
        host=clickhouse_config['host'],
        port=int(clickhouse_config['port']),
        database=clickhouse_config['database'],
        username=clickhouse_config['user'],
        password=clickhouse_config['password']
    )
    
    return client

def initialize_schema(client):
    """Initialize database schema and tables"""
    
    # Create database if not exists
    client.command("CREATE DATABASE IF NOT EXISTS movie_analytics")
    
    # Batch layer tables
    client.command("""
        CREATE TABLE IF NOT EXISTS batch_user_stats
        (
            user_id Int32,
            total_events UInt64,
            movies_watched UInt64,
            avg_duration Float64
        )
        ENGINE = MergeTree()
        ORDER BY user_id
    """)
    
    client.command("""
        CREATE TABLE IF NOT EXISTS batch_popular_movies
        (
            movie_id Int32,
            view_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY movie_id
    """)
    
    client.command("""
        CREATE TABLE IF NOT EXISTS batch_ratings_stats
        (
            movie_id Int32,
            avg_rating Float64,
            rating_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY movie_id
    """)
    
    client.command("""
        CREATE TABLE IF NOT EXISTS batch_hourly_stats
        (
            hour Int32,
            event_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY hour
    """)
    
    # Streaming layer tables
    client.command("""
        CREATE TABLE IF NOT EXISTS stream_movie_views
        (
            window_start DateTime,
            window_end DateTime,
            movie_id Int32,
            view_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (window_start, movie_id)
    """)
    
    client.command("""
        CREATE TABLE IF NOT EXISTS stream_user_activity
        (
            window_start DateTime,
            window_end DateTime,
            user_id Int32,
            activity_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (window_start, user_id)
    """)
    
    client.command("""
        CREATE TABLE IF NOT EXISTS stream_event_distribution
        (
            window_start DateTime,
            window_end DateTime,
            event_type String,
            event_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (window_start, event_type)
    """)
    
    client.command("""
        CREATE TABLE IF NOT EXISTS stream_ratings
        (
            window_start DateTime,
            window_end DateTime,
            movie_id Int32,
            avg_rating Float64,
            rating_count UInt64
        )
        ENGINE = MergeTree()
        ORDER BY (window_start, movie_id)
    """)
    
    print("ClickHouse schema initialized successfully")

def query_data(client, query):
    """Execute query and return results"""
    result = client.query(query)
    return result.result_rows

def main():
    """Initialize ClickHouse schema"""
    config = load_config()
    client = create_client(config)
    initialize_schema(client)

if __name__ == "__main__":
    main()
