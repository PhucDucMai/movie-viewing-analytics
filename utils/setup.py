"""
Setup Script
Initialize system components and upload sample data
"""

from minio import Minio
from minio.error import S3Error
import yaml
import os
import json
from serving.clickhouse_client import initialize_schema, create_client

def load_config():
    """Load configuration from config file"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def setup_minio(config):
    """Setup MinIO bucket and upload sample data"""
    minio_config = config['minio']
    
    client = Minio(
        minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )
    
    bucket_name = minio_config['bucket']
    
    # Create bucket if not exists
    try:
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    except S3Error as e:
        if e.code == 'BucketAlreadyOwnedByYou':
            print(f"Bucket {bucket_name} already exists")
        else:
            raise
    
    # Upload sample data if exists
    sample_data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'sample_events.json')
    if os.path.exists(sample_data_path):
        try:
            client.fput_object(
                bucket_name,
                "batch/events/sample_events.json",
                sample_data_path
            )
            print(f"Uploaded sample data to MinIO")
        except Exception as e:
            print(f"Error uploading sample data: {e}")
    else:
        print(f"Sample data not found at {sample_data_path}")

def setup_clickhouse(config):
    """Initialize ClickHouse schema"""
    client = create_client(config)
    initialize_schema(client)

def main():
    """Main setup function"""
    print("Setting up system components...")
    config = load_config()
    
    print("1. Setting up MinIO...")
    setup_minio(config)
    
    print("2. Setting up ClickHouse...")
    setup_clickhouse(config)
    
    print("Setup completed!")

if __name__ == "__main__":
    main()
