# Kafka Ecosystem Project (Kafka Streams + Kafka Connect)

This repository demonstrates a complete Kafka-based data pipeline using **Apache Kafka**, **Kafka Streams**, and **Kafka Connect** with Spring Boot in Gradle and supporting infrastructure.

It includes real-world ingestion patterns such as:
- File/Excel-based ingestion into Kafka topics
- HTTP API-based ingestion (Mockaroo API)
- Stream processing and transformation using Kafka Streams

---

## Repository Overview

This project is built to demonstrate end-to-end event streaming architecture using Kafka:

1. **Kafka Connect integration (`kafka_connect` repo)**
2. **Kafka Producers (Spring Boot / REST APIs)**
3. **Kafka Streams processing application**
4. **Topic-based data flow and transformation pipelines**

---

##  Key Components

### 1. Kafka Connect Setup

The `kafka_connect` project demonstrates how Kafka Connect is used to ingest external data sources into Kafka topics.

#### 🔹 Features

- File/Excel data ingestion into Kafka topics
- Configurable source connectors
- REST-based connector management
- Connector lifecycle handling (create, update, delete)

#### 🔹 Example Use Case

Data stored in Excel is periodically or batch-read and pushed into Kafka topics using a Kafka Connect Source Connector.

#### 🔹 Key Configurations

Typical connector configuration includes:

- `connector.class`
- `tasks.max`
- `topic` (destination Kafka topic)
- `file.path` or file input source configuration
- `poll.interval.ms`
- `key.converter`
- `value.converter`

This enables automated ingestion of structured data into Kafka without writing custom producer code.
(Screenshot in postman included)
---

### 2. HTTP API Data Ingestion (Mockaroo)

This project also demonstrates ingestion of external HTTP data sources using REST APIs.

#### 🔹 Example Pipeline

```
Kafka Topic (Raw Data)
        ↓
Kafka Streams Processing
        ↓
Transformed Topic (Processed Data)


##  End-to-End Data Flow

---

### Pipeline 1: HTTP API → Kafka → Streams

```
Mockaroo API (HTTP)
   ↓
Spring Boot Producer Service
   ↓
Kafka Topic (t-person-address-http)
   ↓
Kafka Streams Processing
   ↓
Destination Topic (processed-person-data)
```

---

## Technologies Used

- Java 17+
- Spring Boot
- Apache Kafka
- Kafka Streams
- Kafka Connect
- Docker
- Gradle
- REST APIs (Mockaroo)

---

##  Running Kafka and Kafka Connect with Docker

Start Kafka and Kafka Connect ecosystem locally using Docker:

```bash
docker compose up -d
```

Stop services:

```bash
docker compose down
```
