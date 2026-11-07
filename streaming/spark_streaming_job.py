import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "broker:19092")
INPUT_TOPIC = "transactions"
VALID_OUTPUT_TOPIC = "transactions_valid"
DLQ_TOPIC = "transactions_dlq"


def get_spark_session():
    return (
        SparkSession.builder
        .appName("TransactionsStreaming")
        .getOrCreate()
    )


def get_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("source", StringType(), True),
    ])


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = get_schema()

    parsed_stream = (
        raw_stream
        .select(F.col("value").cast("string").alias("json_val"))
        .select(
            F.col("json_val"),
            F.from_json(F.col("json_val"), schema).alias("data"),
        )
        .select(F.col("json_val"), "data.*")
    )

    # Event-time watermark for deduplication and window aggregation.
    parsed_stream = parsed_stream.withWatermark("timestamp", "3 minutes")

    # Explicit late-event check allows late records to be routed to the DLQ.
    parsed_stream = parsed_stream.withColumn(
        "is_late",
        F.col("timestamp")
        < F.current_timestamp() - F.expr("INTERVAL 3 MINUTES"),
    )

    validated_data = parsed_stream.withColumn(
        "mandatory_valid",
        F.col("user_id").isNotNull()
        & F.col("amount").isNotNull()
        & F.col("timestamp").isNotNull(),
    )

    validated_data = validated_data.withColumn(
        "amount_valid",
        F.col("amount").between(1, 10_000_000),
    )

    validated_data = validated_data.withColumn(
        "valid_source",
        F.col("source").isin("mobile", "web", "pos"),
    )

    validated_data = validated_data.withColumn(
        "is_valid",
        F.col("mandatory_valid")
        & F.col("amount_valid")
        & F.col("valid_source")
        & ~F.coalesce(F.col("is_late"), F.lit(False)),
    )

    validated_data = validated_data.withColumn(
        "error_reason",
        F.when(~F.col("mandatory_valid"), "Missing mandatory field")
        .when(~F.col("amount_valid"), "Invalid amount")
        .when(~F.col("valid_source"), "Invalid source")
        .when(F.coalesce(F.col("is_late"), F.lit(False)), "Late event")
        .otherwise(F.lit(None).cast("string")),
    )

    valid_data = validated_data.filter(F.col("is_valid"))

    # Deduplicate valid events within the configured event-time watermark.
    deduplicated_valid_data = valid_data.dropDuplicatesWithinWatermark(
        ["user_id", "timestamp"]
    )

    invalid_data = validated_data.filter(~F.col("is_valid"))

    valid_kafka_output = deduplicated_valid_data.selectExpr(
        "to_json(struct(*)) AS value"
    )
    dlq_kafka_output = invalid_data.selectExpr(
        "to_json(struct(*)) AS value"
    )

    valid_query = (
        valid_kafka_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", VALID_OUTPUT_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/transactions_valid")
        .outputMode("append")
        .start()
    )

    dlq_query = (
        dlq_kafka_output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", "/tmp/checkpoints/transactions_dlq")
        .outputMode("append")
        .start()
    )

    # Aggregations are separate streaming queries to avoid chaining
    # multiple stateful operations in a single query.
    running_total = (
        valid_data
        .groupBy()
        .agg(F.count("*").alias("running_total"))
        .withColumn("timestamp", F.current_timestamp())
        .select("timestamp", "running_total")
    )

    running_total_query = (
        running_total.writeStream
        .format("console")
        .outputMode("complete")
        .option("checkpointLocation", "/tmp/checkpoints/running_total")
        .start()
    )

    window_transactions = (
        valid_data
        .groupBy(F.window("timestamp", "1 minute"))
        .agg(F.count("*").alias("transaction_count"))
    )

    window_query = (
        window_transactions.writeStream
        .format("console")
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/window_transactions")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
