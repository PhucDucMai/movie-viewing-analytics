# Movie Viewing Behavior Analysis & Monitoring
## Lambda Architecture Implementation

This project implements a Lambda Architecture system for analyzing and monitoring movie viewing behavior using Netflix/MovieLens dataset patterns.

## Architecture Overview

```
┌─────────────┐
│  Data Source│
└──────┬──────┘
       │
       v
┌─────────────┐
│   Kafka     │  ← Ingestion Layer
│  (Real-time)│
└──┬──────┬───┘
   │      │
   │      └─────────┐
   │                │
   v                v
┌─────────┐    ┌─────────────┐
│  MinIO  │    │ Spark Stream│  ← Speed Layer
│ (Batch) │    │  (Real-time)│
└────┬────┘    └──────┬──────┘
     │                │
     v                │
┌─────────┐           │
│ Spark   │           │
│ Batch   │  ←───────┘  ← Batch Layer
└────┬────┘
     │
     v
┌─────────────┐
│  ClickHouse │  ← Serving Layer
│  (Database) │
└──────┬──────┘
       │
       v
┌─────────────┐
│  Streamlit  │  ← Presentation Layer
│  Dashboard  │
└─────────────┘
```

## Technology Stack

- **Kafka**: Real-time data ingestion
- **MinIO**: Object storage for batch data
- **Spark Batch**: Batch processing of historical data
- **Spark Streaming**: Real-time stream processing
- **ClickHouse**: Columnar database for analytics
- **Streamlit**: Web dashboard for visualization

## Project Structure

```
.
├── data/                  # Sample data files
├── ingestion/             # Kafka producer code
├── batch/                 # Spark batch processing
├── streaming/             # Spark streaming processing
├── serving/               # ClickHouse utilities
├── dashboard/             # Streamlit application
├── config/                # Configuration files
├── docker/                # Docker compose setup
└── requirements.txt       # Python dependencies
```

## Getting Started

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Start infrastructure (Kafka, MinIO, ClickHouse):
```bash
docker-compose -f docker/docker-compose.yml up -d
```

3. Run batch processing:
```bash
python batch/spark_batch_processor.py
```

4. Run streaming processing:
```bash
python streaming/spark_stream_processor.py
```

5. Start dashboard:
```bash
streamlit run dashboard/app.py
```

## Data Flow

1. **Ingestion**: Kafka producer reads movie viewing events
2. **Batch Layer**: Spark processes historical data from MinIO
3. **Speed Layer**: Spark Streaming processes real-time events from Kafka
4. **Serving Layer**: Both layers write results to ClickHouse
5. **Visualization**: Streamlit dashboard reads from ClickHouse
