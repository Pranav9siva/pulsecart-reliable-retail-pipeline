from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType,
)

from pulsecart.config import LAKE_ROOT, ensure_project_dirs


TOPIC = "retail.order_events"
SPARK_VERSION = "3.5.6"
KAFKA_CONNECTOR = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"

PAYLOAD_SCHEMA = StructType([
    StructField("source_row_id", StringType(), True),
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
])
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("schema_version", IntegerType(), True),
    StructField("event_time", StringType(), True),
    StructField("source", StringType(), True),
    StructField("replay_run_id", StringType(), True),
    StructField("payload", PAYLOAD_SCHEMA, True),
])


def start_stream(bootstrap_servers: str = "localhost:29092", topic: str = TOPIC) -> None:
    ensure_project_dirs()
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("PulseCart-order-stream")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", KAFKA_CONNECTOR)
        .getOrCreate()
    )

    run_root = LAKE_ROOT / "streaming" / topic.replace(".", "_")
    checkpoint_root = LAKE_ROOT / "checkpoints" / topic.replace(".", "_")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    decoded = (
        raw.select(
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("value").cast("string").alias("raw_message"),
        )
        .withColumn("event", F.from_json("raw_message", EVENT_SCHEMA))
        .select("topic", "partition", "offset", "raw_message", "event.*")
        .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("quantity_int", F.expr("try_cast(payload.quantity as int)"))
        .withColumn("unit_price_decimal", F.expr("try_cast(payload.unit_price as decimal(18, 4))"))
    )

    errors = F.concat_ws("; ",
        F.when(F.col("event_id").isNull() | (F.length("event_id") == 0), "missing event id"),
        F.when(F.col("schema_version") != 1, "unsupported schema version"),
        F.when(F.col("event_ts").isNull(), "invalid event time"),
        F.when(F.col("payload.invoice_no").isNull() | (F.length("payload.invoice_no") == 0), "missing invoice number"),
        F.when(F.col("payload.stock_code").isNull() | (F.length("payload.stock_code") == 0), "missing stock code"),
        F.when(F.col("quantity_int").isNull(), "invalid quantity"),
        F.when(F.col("quantity_int") == 0, "zero quantity"),
        F.when(F.col("unit_price_decimal").isNull(), "invalid unit price"),
        F.when(F.col("unit_price_decimal") < 0, "negative unit price"),
    )
    classified = decoded.withColumn("validation_error", F.when(F.length(errors) > 0, errors))

    valid = (
        classified.filter(F.col("validation_error").isNull())
        .withWatermark("event_ts", "2 days")
        .dropDuplicates(["event_id"])
        .select(
            "event_id", "schema_version", "event_ts", "source", "replay_run_id",
            F.col("payload.source_row_id").alias("source_row_id"),
            F.trim(F.col("payload.invoice_no")).alias("invoice_no"),
            F.trim(F.col("payload.stock_code")).alias("stock_code"),
            F.trim(F.col("payload.description")).alias("description"),
            F.col("quantity_int").alias("quantity"),
            F.col("unit_price_decimal").alias("unit_price"),
            F.trim(F.col("payload.customer_id")).alias("customer_id"),
            F.initcap(F.trim(F.col("payload.country"))).alias("country"),
            ((F.col("quantity_int") < 0) | F.upper(F.col("payload.invoice_no")).startswith("C")).alias("is_return"),
            (F.col("quantity_int") * F.col("unit_price_decimal")).cast("decimal(20, 4)").alias("line_revenue_gbp"),
            F.to_date("event_ts").alias("event_date"),
            "topic", "partition", "offset",
        )
    )
    invalid = classified.filter(F.col("validation_error").isNotNull()).select(
        "topic", "partition", "offset", "event_id", "event_time", "source", "replay_run_id",
        "validation_error", "raw_message",
    )

    valid_query = (
        valid.writeStream.format("parquet").outputMode("append")
        .option("path", str(run_root / "valid_orders"))
        .option("checkpointLocation", str(checkpoint_root / "valid_orders"))
        .partitionBy("event_date")
        .trigger(processingTime="5 seconds")
        .start()
    )
    quarantine_query = (
        invalid.writeStream.format("parquet").outputMode("append")
        .option("path", str(run_root / "quarantine"))
        .option("checkpointLocation", str(checkpoint_root / "quarantine"))
        .trigger(processingTime="5 seconds")
        .start()
    )
    print(f"Streaming {topic} from {bootstrap_servers}; stop with Ctrl+C")
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()

