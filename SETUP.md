# Setup Guide

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Java 8+ (required for Spark)

## Installation Steps

### 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Infrastructure Services

Start Kafka, MinIO, and ClickHouse using Docker Compose:

```bash
docker-compose -f docker/docker-compose.yml up -d
```

Wait for all services to be ready (about 30-60 seconds).

### 3. Generate Sample Data

Generate sample batch data:

```bash
python data/generate_sample_data.py
```

This creates `data/sample_events.json` with 10,000 sample events.

### 4. Initialize System

Run the setup script to initialize MinIO buckets and ClickHouse schema:

```bash
python utils/setup.py
```

### 5. Upload Data to MinIO

If you generated sample data, upload it to MinIO for batch processing:

```bash
# Using MinIO client (if installed)
mc cp data/sample_events.json local/movie-data/batch/events/

# Or use Python script in utils/setup.py
```

### 6. Start Data Ingestion

Start Kafka producer to simulate real-time events:

```bash
python ingestion/kafka_producer.py
```

This will continuously generate and send events to Kafka.

### 7. Run Batch Processing

Process historical data from MinIO:

```bash
python batch/spark_batch_processor.py
```

### 8. Run Streaming Processing

In a separate terminal, start Spark streaming:

```bash
python streaming/spark_stream_processor.py
```

### 9. Launch Dashboard

Start the Streamlit dashboard:

```bash
streamlit run dashboard/app.py
```

Access the dashboard at: http://localhost:8501

## Service URLs

- **Kafka**: localhost:9092
- **MinIO Console**: http://localhost:9001 (login: minioadmin/minioadmin)
- **ClickHouse**: localhost:8123
- **Streamlit Dashboard**: http://localhost:8501

## Troubleshooting

### Kafka Connection Issues

Ensure Kafka is running:
```bash
docker ps | grep kafka
```

### MinIO Connection Issues

Check MinIO is accessible:
```bash
curl http://localhost:9000
```

### ClickHouse Connection Issues

Verify ClickHouse is running:
```bash
curl http://localhost:8123
```

### Spark Issues

Ensure Java is installed:
```bash
java -version
```

If issues persist, check logs:
```bash
docker logs kafka
docker logs minio
docker logs clickhouse
```
