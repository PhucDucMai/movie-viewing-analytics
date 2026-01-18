# Architecture Documentation

## Lambda Architecture Overview

This project implements Lambda Architecture for processing movie viewing behavior data. Lambda Architecture provides both batch and real-time processing capabilities.

## Architecture Layers

### 1. Ingestion Layer (Kafka)

**Purpose**: Collect and buffer real-time movie viewing events

**Components**:
- Kafka Producer (`ingestion/kafka_producer.py`)
- Topic: `movie-viewing-events`

**Data Format**:
```json
{
  "event_id": "evt_1234567890_1234",
  "user_id": 123,
  "movie_id": 456,
  "event_type": "view|pause|resume|complete|rating",
  "timestamp": "2024-01-01T12:00:00",
  "duration": 3600,
  "rating": 5
}
```

### 2. Batch Layer (Spark Batch + MinIO)

**Purpose**: Process historical data with high accuracy

**Components**:
- Spark Batch Processor (`batch/spark_batch_processor.py`)
- MinIO Object Storage
- Processes data from MinIO S3-compatible storage

**Processing Tasks**:
- User statistics (total events, movies watched, avg duration)
- Popular movies ranking
- Movie ratings analysis
- Hourly activity patterns

**Output**: Writes to ClickHouse tables:
- `batch_user_stats`
- `batch_popular_movies`
- `batch_ratings_stats`
- `batch_hourly_stats`

### 3. Speed Layer (Spark Streaming)

**Purpose**: Process real-time events with low latency

**Components**:
- Spark Streaming Processor (`streaming/spark_stream_processor.py`)
- Reads from Kafka topics
- Micro-batch processing (10-second windows)

**Processing Tasks**:
- Real-time movie view counts
- User activity monitoring
- Event type distribution
- Real-time ratings aggregation

**Output**: Writes to ClickHouse tables:
- `stream_movie_views`
- `stream_user_activity`
- `stream_event_distribution`
- `stream_ratings`

### 4. Serving Layer (ClickHouse)

**Purpose**: Fast query engine for analytics

**Database**: ClickHouse (columnar OLAP database)

**Schema**:
- Separate tables for batch and streaming results
- Optimized for analytical queries
- MergeTree engine for fast aggregations

### 5. Presentation Layer (Streamlit)

**Purpose**: Visualize analytics results

**Components**:
- Streamlit Dashboard (`dashboard/app.py`)

**Features**:
- Batch analytics visualization
- Real-time streaming metrics
- System overview dashboard
- Interactive charts and tables

## Data Flow

```
Data Source → Kafka → ┌─→ Spark Streaming → ClickHouse ──┐
                      │                                   │
                      └─→ MinIO → Spark Batch → ClickHouse┘
                                                           │
                                                           ↓
                                                    Streamlit Dashboard
```

## Technology Choices

### Kafka
- High-throughput message queue
- Scalable and fault-tolerant
- Real-time event streaming

### MinIO
- S3-compatible object storage
- Cost-effective for batch storage
- Easy integration with Spark

### Spark
- Unified engine for batch and streaming
- Distributed processing
- Rich SQL and DataFrame APIs

### ClickHouse
- Columnar database optimized for analytics
- Fast aggregations
- Low-latency queries

### Streamlit
- Rapid dashboard development
- Interactive visualizations
- Easy deployment

## Scalability Considerations

- **Kafka**: Horizontal scaling via partitions
- **Spark**: Distributed processing across clusters
- **ClickHouse**: Sharding and replication
- **MinIO**: Distributed object storage

## Performance Characteristics

- **Batch Layer**: High accuracy, higher latency (minutes to hours)
- **Speed Layer**: Lower latency (seconds), near-real-time results
- **Serving Layer**: Sub-second query response for analytics

## Future Enhancements

- Machine learning recommendations
- Anomaly detection
- Advanced time-series analysis
- Multi-region deployment
- Data quality monitoring
