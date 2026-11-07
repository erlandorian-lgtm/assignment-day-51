# Kafka + PySpark Structured Streaming

A small end-to-end streaming pipeline that publishes transaction events to Kafka, validates them with PySpark Structured Streaming, and routes valid and invalid records to separate Kafka topics.

## Architecture

```text
Python producer
      |
      v
Kafka: transactions
      |
      v
PySpark Structured Streaming
      |--------------------------|
      v                          v
transactions_valid        transactions_dlq
      |
      +--> Console: running total
      |
      +--> Console: 1-minute tumbling-window counts
```

## Project structure

```text
assignment-day-51/
├── producer/
│   └── producer.py
├── streaming/
│   └── spark_streaming_job.py
├── screenshots/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Requirements

- Docker and Docker Compose
- Python 3.10+
- A Python virtual environment (recommended)

## Run the project

### 1. Start Kafka and Spark

From the project root:

```bash
docker compose up -d
```

### 2. Install the producer dependency

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Start the Spark streaming job

Run this in one terminal and leave it running:

```bash
docker compose exec spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /opt/app/streaming/spark_streaming_job.py
```

### 4. Start the producer

In a second terminal:

```bash
source .venv/bin/activate
python producer/producer.py
```

The producer sends 25 events to the `transactions` topic, including controlled examples of late events, invalid amounts, invalid sources, invalid timestamps, and duplicate events.

### 5. Inspect the output topics

Valid transactions:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic transactions_valid \
  --from-beginning
```

Invalid transactions (DLQ):

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic transactions_dlq \
  --from-beginning
```

The Spark console also prints the cumulative valid transaction count and completed 1-minute window counts.

## Validation rules

| Rule | Behavior |
|---|---|
| Mandatory fields | `user_id`, `amount`, and `timestamp` must be present |
| Type parsing | JSON is parsed using an explicit Spark schema; values that fail parsing become null |
| Amount | Must be between 1 and 10,000,000 |
| Source | Must be `mobile`, `web`, or `pos` |
| Late events | Event timestamps more than 3 minutes behind processing time are marked late |
| Duplicate events | Valid records are deduplicated by `user_id` and `timestamp` within the 3-minute watermark |

## Notes and limitations

- The watermark is set to 3 minutes on the event-time `timestamp` column.
- Late-event routing uses an explicit comparison against the current processing time so the late event can be included in the DLQ. The watermark is separately used by the 1-minute event-time window aggregation.
- Spark's `dropDuplicatesWithinWatermark()` silently drops duplicate records. Duplicates are not emitted to the DLQ.
- The running total and window counts are calculated from valid records before deduplication, so duplicate events can contribute to those aggregates.
- JSON values that cannot be parsed into the declared schema become null. They are caught by mandatory-field validation, but are not separately labelled as type errors.
- Append-mode window aggregation emits a window only after the watermark passes its end. If the producer stops, a later event may be needed to advance the watermark and flush the last window.

## Reset the local test environment

To remove Kafka data and Spark checkpoints stored in Docker volumes:

```bash
docker compose down -v
docker compose up -d
```

This removes the Docker volumes, not the source files.

## Screenshots

Add your actual producer and Spark console screenshots to the `screenshots/` folder after running the project. See [screenshots/README.md](screenshots/README.md).
