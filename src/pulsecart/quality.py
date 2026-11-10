from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import functions as F

from pulsecart.batch import spark_session
from pulsecart.config import LAKE_ROOT, REPORT_ROOT, ensure_project_dirs


MAX_QUARANTINE_RATE = 0.10


def run_quality_checks() -> dict:
    ensure_project_dirs()
    spark = spark_session("PulseCart-quality")
    try:
        bronze_path = str(LAKE_ROOT / "bronze" / "online_retail_ii")
        silver_path = str(LAKE_ROOT / "silver" / "order_lines")
        quarantine_path = str(LAKE_ROOT / "quarantine" / "batch")
        bronze = spark.read.parquet(bronze_path)
        silver = spark.read.parquet(silver_path)
        quarantine = spark.read.parquet(quarantine_path)

        source_count = bronze.count()
        accepted_count = silver.count()
        rejected_count = quarantine.count()
        duplicate_source_ids = (
            bronze.groupBy("source_row_id").count().filter(F.col("count") > 1).count()
        )
        duplicate_silver_ids = (
            silver.groupBy("source_row_id").count().filter(F.col("count") > 1).count()
        )
        null_required = silver.filter(
            F.col("source_row_id").isNull()
            | F.col("invoice_no").isNull()
            | F.col("stock_code").isNull()
            | F.col("invoice_ts").isNull()
            | F.col("quantity").isNull()
            | F.col("unit_price").isNull()
        ).count()
        quarantine_rate = rejected_count / source_count if source_count else 0.0

        checks = {
            "source_reconciles_to_accepted_plus_quarantine": source_count == accepted_count + rejected_count,
            "source_row_ids_are_unique": duplicate_source_ids == 0,
            "accepted_row_ids_are_unique": duplicate_silver_ids == 0,
            "accepted_rows_have_required_fields": null_required == 0,
            "quarantine_rate_below_10_percent": quarantine_rate <= MAX_QUARANTINE_RATE,
        }
        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_rows": source_count,
            "accepted_rows": accepted_count,
            "quarantined_rows": rejected_count,
            "quarantine_rate": round(quarantine_rate, 6),
            "duplicate_source_ids": duplicate_source_ids,
            "duplicate_accepted_ids": duplicate_silver_ids,
            "accepted_rows_missing_required_fields": null_required,
            "checks": checks,
            "status": "passed" if all(checks.values()) else "failed",
        }
        report_path = REPORT_ROOT / "quality_report.json"
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        if not all(checks.values()):
            raise RuntimeError(f"Data quality checks failed. See {report_path}")
        return report
    finally:
        spark.stop()

