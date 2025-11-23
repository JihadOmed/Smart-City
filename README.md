#  Smart City Real-Time Data Pipeline (Kafka + Spark + GCP)

This project implements a **real-time Smart City data platform** using:

- Python data generators (simulated IoT & city data)
- Apache Kafka for streaming
- Apache Spark Structured Streaming for processing
- Google Cloud Storage (GCS) as the data lake
- Docker and docker-compose for local infrastructure

It is inspired by the “Smart City Data Engineering” project on YouTube, but uses **Google Cloud Platform (GCP)** instead of AWS for the storage layer.

The goal is to demonstrate end-to-end **Data Engineering skills**: ingestion, streaming, processing, and cloud-based storage in a realistic scenario.

---

## Concept

The project simulates a **Smart City** where different systems continuously stream data:

- **Vehicles** moving from London to Birmingham  
- **GPS devices** reporting position and speed  
- **Traffic cameras** sending snapshots and locations  
- **Weather stations** sending weather conditions  
- **Emergency services** reporting incidents  

Each system writes events into **Kafka topics**, which are then consumed by **Spark Structured Streaming** and written into **GCS** in Parquet format. This creates a real-time data lake that can later be queried by BigQuery, used by ML models, or visualized in BI tools.

---

## Architecture

High-level architecture:

```text
     Python Producers (main.py)
   ───────────────────────────────
   Vehicle / GPS / Weather / Traffic / Emergency
                   │
                   ▼
         ┌───────────────────┐
         │   Kafka Broker    │
         │  (with ZooKeeper) │
         └─────────┬─────────┘
                   │
            Streaming Reads
                   ▼
       ┌────────────────────────┐
       │ Spark Structured       │
       │   Streaming (spark-city.py)
       └─────────┬──────────────┘
                 │
         Writes Parquet to
                 ▼
      ┌────────────────────────┐
      │ Google Cloud Storage   │
      │   (Data Lake – Bronze) │
      └────────────────────────┘
