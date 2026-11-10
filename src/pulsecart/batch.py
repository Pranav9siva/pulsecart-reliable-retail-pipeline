from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from pulsecart.config import LAKE_ROOT, SOURCE_CSV, ensure_project_dirs


SOURCE_SCHEMA = StructType([
    StructField("source_row_id", StringType(), True),
    StructField("source_sheet", StringType(), True),
    StructField("source_row_number", LongType(), True),
    StructField("invoice_no", StringType(), True),
    StructField("stock_code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("country", StringType(), True),
])


def spark_session(app_name: str = "PulseCart") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _write(df: DataFrame, relative_path: str, partition_by: str | None = None) -> None:
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(str(LAKE_ROOT / relative_path))


def run_batch() -> dict[str, str]:
    ensure_project_dirs()
    if not SOURCE_CSV.exists():
        raise FileNotFoundError(f"Canonical source CSV not found: {SOURCE_CSV}. Run `pulsecart extract` first.")

    spark = spark_session("PulseCart-batch")
    try:
        source = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(SOURCE_SCHEMA)
            .csv(str(SOURCE_CSV))
        )
        bronze = source.withColumn("_ingested_at", F.current_timestamp())
        _write(bronze, "bronze/online_retail_ii")

        typed = (
            bronze
            .withColumn("invoice_no", F.trim(F.col("invoice_no")))
            .withColumn("stock_code", F.trim(F.col("stock_code")))
            .withColumn("description", F.trim(F.col("description")))
            .withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("country", F.initcap(F.trim(F.col("country"))))
            .withColumn("invoice_ts", F.to_timestamp("invoice_date", "yyyy-MM-dd HH:mm:ss"))
            .withColumn("quantity", F.expr("try_cast(quantity as int)"))
            .withColumn("unit_price", F.expr("try_cast(unit_price as decimal(18, 4))"))
        )
        reasons = F.concat_ws("; ",
            F.when(F.col("source_row_id").isNull() | (F.length("source_row_id") == 0), "missing source row id"),
            F.when(F.col("invoice_no").isNull() | (F.length("invoice_no") == 0), "missing invoice number"),
            F.when(F.col("stock_code").isNull() | (F.length("stock_code") == 0), "missing stock code"),
            F.when(F.col("invoice_ts").isNull(), "invalid invoice timestamp"),
            F.when(F.col("quantity").isNull(), "invalid quantity"),
            F.when(F.col("quantity") == 0, "zero quantity"),
            F.when(F.col("unit_price").isNull(), "invalid unit price"),
            F.when(F.col("unit_price") < 0, "negative unit price"),
        )
        typed = typed.withColumn("validation_error", F.when(F.length(reasons) > 0, reasons))
        rejected = typed.filter(F.col("validation_error").isNotNull())
        accepted = typed.filter(F.col("validation_error").isNull())
        _write(rejected, "quarantine/batch")

        lines = (
            accepted
            .withColumn("is_return", (F.col("quantity") < 0) | F.upper("invoice_no").startswith("C"))
            .withColumn("line_revenue_gbp", (F.col("quantity") * F.col("unit_price")).cast("decimal(20, 4)"))
            .withColumn("order_date", F.to_date("invoice_ts"))
            .withColumn("order_month", F.date_format("invoice_ts", "yyyy-MM"))
            .select(
                "source_row_id", "source_sheet", "source_row_number", "invoice_no", "stock_code",
                "description", "quantity", "invoice_ts", "unit_price", "line_revenue_gbp",
                "customer_id", "country", "is_return", "order_date", "order_month",
            )
        )
        _write(lines, "silver/order_lines", "order_month")

        product_window = Window.partitionBy("stock_code").orderBy(F.col("invoice_ts").desc(), F.col("description").desc())
        products = (
            lines.filter(F.col("description").isNotNull())
            .withColumn("_rank", F.row_number().over(product_window))
            .filter(F.col("_rank") == 1)
            .select(F.col("stock_code").alias("product_id"), "description")
        )
        _write(products, "gold/dim_product")

        customer_window = Window.partitionBy("customer_id").orderBy(F.col("invoice_ts").desc(), F.col("country").desc())
        customers = (
            lines.filter(F.col("customer_id").isNotNull() & (F.length("customer_id") > 0))
            .withColumn("_rank", F.row_number().over(customer_window))
            .filter(F.col("_rank") == 1)
            .select("customer_id", "country")
        )
        _write(customers, "gold/dim_customer")

        daily_sales = (
            lines.groupBy("order_date", "country")
            .agg(
                F.countDistinct(F.when(~F.col("is_return"), F.col("invoice_no"))).alias("order_count"),
                F.countDistinct(F.when(F.col("is_return"), F.col("invoice_no"))).alias("return_invoice_count"),
                F.sum(F.when((~F.col("is_return")) & (F.col("quantity") > 0), F.col("line_revenue_gbp")).otherwise(F.lit(0))).alias("gross_sales_gbp"),
                F.sum(F.when(F.col("is_return"), F.col("line_revenue_gbp")).otherwise(F.lit(0))).alias("return_value_gbp"),
                F.sum("line_revenue_gbp").alias("net_revenue_gbp"),
                F.sum(F.when(F.col("quantity") > 0, F.col("quantity")).otherwise(F.lit(0))).alias("units_sold"),
                F.sum(F.when(F.col("quantity") < 0, -F.col("quantity")).otherwise(F.lit(0))).alias("units_returned"),
                F.count("*").alias("line_count"),
            )
        )
        _write(daily_sales, "gold/mart_daily_sales")

        top_products = (
            lines.groupBy("stock_code", "description")
            .agg(
                F.sum("quantity").alias("net_units"),
                F.sum("line_revenue_gbp").alias("net_revenue_gbp"),
                F.count("*").alias("line_count"),
            )
        )
        _write(top_products, "gold/mart_product_performance")

        customer_summary = (
            lines.filter(F.col("customer_id").isNotNull() & (F.length("customer_id") > 0))
            .groupBy("customer_id", "country")
            .agg(
                F.countDistinct(F.when(~F.col("is_return"), F.col("invoice_no"))).alias("order_count"),
                F.sum("line_revenue_gbp").alias("net_revenue_gbp"),
                F.sum("quantity").alias("net_units"),
            )
        )
        _write(customer_summary, "gold/mart_customer_summary")

        summary = {
            "bronze": str(LAKE_ROOT / "bronze" / "online_retail_ii"),
            "silver_order_lines": str(LAKE_ROOT / "silver" / "order_lines"),
            "quarantine": str(LAKE_ROOT / "quarantine" / "batch"),
            "gold": str(LAKE_ROOT / "gold"),
        }
        print(f"Batch pipeline complete. Gold marts written to {summary['gold']}")
        return summary
    finally:
        spark.stop()

