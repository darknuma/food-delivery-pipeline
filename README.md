# Food Delivery Data Platform

Production-style case study for a real-time food delivery pipeline that turns operational delivery events into queryable business metrics.

The system simulates order, courier, merchant, and review activity, streams the events through Kafka, persists operational state in Cassandra, lands immutable raw data in S3, transforms orders with Spark, and publishes analytics-ready tables to Snowflake.

## Executive Summary

Food delivery platforms need two different data paths at the same time:

- Low-latency operational state for orders and couriers.
- Durable analytical history for fulfillment, delivery-time, revenue, and customer-experience reporting.

This project implements both paths from the same event stream. Kafka decouples producers from downstream consumers, Cassandra serves operational lookups, S3 stores the bronze and silver data lake layers, Spark handles batch transformations, Snowflake serves warehouse tables, and Prefect/Soda provide orchestration and validation hooks.

## Problem Framing

The pipeline is designed around common food delivery business questions:

- What percentage of orders are fulfilled, cancelled, or delayed?
- How long do successful deliveries take?
- Which days have the highest order volume and revenue?
- How many merchants and customers are active?
- Are ETA assumptions drifting from actual delivery behavior?

The key engineering challenge is preserving high-volume event history while still supporting operational consumers that need current order and courier state.

## Architecture

```text
Synthetic event generator
    |
    v
Kafka raw event topics
    |-------------------------> Cassandra
    |                             Operational order and courier tables
    |
    v
S3 bronze parquet
    |
    v
Spark silver transforms
    |
    v
S3 silver parquet
    |
    v
Snowflake silver tables
    |
    v
Snowflake gold metrics
```

## Design Decisions

| Decision | Implementation | Engineering Reason |
| --- | --- | --- |
| Kafka as the ingestion backbone | Four raw topics for orders, couriers, merchants, and reviews | Separates event production from operational and analytical consumers. |
| Cassandra for operational state | `food_delivery.orders` and `food_delivery.courier` tables | Supports fast key-based lookups for order and courier timelines. |
| S3 as the data lake | Bronze and silver Parquet paths | Keeps immutable event history separate from serving databases. |
| Spark for silver processing | `processing/write_to_silver.py` | Handles schema enforcement, nested item flattening, filtering, and derived columns. |
| Snowflake for analytics | `ORDERS`, `ORDER_ITEMS`, and gold metric tables | Provides BI-friendly warehouse tables after lake processing. |
| Prefect for orchestration | `processing/prefect_pipeline.py` and `deploy_flow.py` | Models the ETL as a recoverable workflow with task boundaries. |
| Soda for validation hooks | `validation/*.yml` | Gives the project a place to enforce data quality before publishing metrics. |

## Data Flow

1. `delivery_producer.py` generates synthetic events from cleaned restaurant data.
2. Events are published to Kafka topics:
   - `food-delivery-orders-raw`
   - `food-delivery-couriers-raw`
   - `food-delivery-merchants-raw`
   - `food-delivery-reviews-raw`
3. `kafka_to_cassandra_consumer.py` writes order and courier records to Cassandra.
4. `order_consumer.py` buffers Kafka events and writes Parquet files to S3 bronze.
5. `write_to_silver.py` reads bronze orders, validates core fields, casts types, flattens delivery location, calculates delivery duration, marks delayed orders, and explodes order items.
6. `write_to_snowflake.py` loads silver data into Snowflake with staging tables and merge logic.
7. `silver_to_gold.py` derives fulfillment, delivery-time, volume, revenue, and ETA metrics.

## Reliability Controls

- Kafka producer uses keyed messages, retries, and `acks="all"` for stronger delivery guarantees.
- Consumers manually commit offsets after processing batches.
- S3 writes use Parquet to reduce storage size and improve downstream scan performance.
- Silver processing filters records with missing required identifiers or invalid order totals.
- Snowflake loading writes through staging tables before merging into final tables.
- Prefect tasks isolate Kafka checks, Spark transforms, Snowflake loads, and validation steps.

## Data Model

### Operational Cassandra Tables

Defined in `cassandra/scripts/cassandra_setup.cql`:

- `food_delivery.orders`
- `food_delivery.courier`

These tables are partitioned by entity identifiers and clustered by event timestamp for timeline-style reads.

### Lake Paths

```text
s3://numa-delivery/bronze/<topic>/<yyyy>/<mm>/<dd>/<hh>/<mmss>.parquet
s3://numa-delivery/silver/food-delivery-orders/
s3://numa-delivery/silver/food-delivery-order_items/
```

### Snowflake Tables

Silver:

- `DELIVERY.SILVER.ORDERS`
- `DELIVERY.SILVER.ORDER_ITEMS`

Gold:

- `DELIVERY.GOLD.FULFILLMENT_METRICS`
- `DELIVERY.GOLD.DELIVERY_TIME_METRICS`
- `DELIVERY.GOLD.ORDER_VOLUME_METRICS`
- `DELIVERY.GOLD.ETA_ACCURACY_METRICS`

## Local Runbook

Create a `.env` file:

```env
AWS_ACCESS_KEY=your_aws_access_key
AWS_SECRET_KEY=your_aws_secret_key
AWS_REGION=us-east-2
S3_BUCKET=numa-delivery

SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
```

Start the full stack:

```bash
docker compose up --build
```

Useful service endpoints:

- Kafka broker: `localhost:9092`
- Cassandra: `localhost:9042`
- Kafdrop: `http://localhost:9001`
- Prefect: `http://localhost:4200`
- Spark/Jupyter container port: `localhost:8888`

Run individual services:

```bash
docker compose up -d zookeeper kafka-broker-1 kafka-setup cassandra kafdrop
docker compose up producer
docker compose up cassandra-consumer
docker compose up s3-consumer
```

Run processing jobs from the processing container:

```bash
python write_to_silver.py
python write_to_snowflake.py
python silver_to_gold.py
python prefect_pipeline.py
```

## Validation and Tests

Install Python dependencies:

```bash
pip install -r data_ingestion/requirement.txt
```

Run tests:

```bash
pytest
```

Coverage areas include event generation, Kafka producer behavior, S3 operations, ETL logic, Snowflake loading, and integration-oriented flows.

Soda validation files live in `validation/` and are wired into the Prefect flow:

- `silver_checks.yml`
- `gold_checks.yml`
- `snowflake_checks.yml`

## Production Gaps I Would Close Next

- Parameterize hard-coded S3, Snowflake, and account settings.
- Complete Soda checks with concrete schema, freshness, volume, and business-rule assertions.
- Add CI for unit tests, linting, Docker image builds, and integration smoke tests.
- Add Terraform for S3, IAM, Snowflake, and Kubernetes resources.
- Finish Kubernetes manifests for Kafka, consumers, and Prefect workers.
- Add dead-letter topics for malformed events and failed sink writes.
- Add observability dashboards for Kafka lag, consumer throughput, S3 write volume, Spark job duration, and Snowflake merge failures.
- Review and harden `silver_to_gold.py` before production execution.

## Repository Map

```text
food-delivery-pipeline/
|-- assets/                  # Source restaurant data and project notes
|-- cassandra/               # Cassandra image and schema setup
|-- data_ingestion/          # Kafka producers, consumers, schemas, event generator
|-- deployments/             # Kubernetes work in progress
|-- init_setup/              # Data cleaning and scraping utilities
|-- kafka/                   # Kafka image and topic setup scripts
|-- processing/              # Spark, Snowflake, Prefect, and metric jobs
|-- tests/                   # Unit and integration tests
|-- validation/              # Soda check files
`-- docker-compose.yml       # Local development stack
```
