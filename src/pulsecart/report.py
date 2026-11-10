from __future__ import annotations

import json
from datetime import datetime, timezone

import duckdb

from pulsecart.config import LAKE_ROOT, REPORT_ROOT, ensure_project_dirs


def _parquet_glob(relative: str) -> str:
    path = (LAKE_ROOT / relative).as_posix()
    return path.rstrip("/") + "/**/*.parquet"


def build_reports() -> dict:
    ensure_project_dirs()
    daily_path = _parquet_glob("gold/mart_daily_sales")
    product_path = _parquet_glob("gold/mart_product_performance")
    customer_path = _parquet_glob("gold/mart_customer_summary")
    connection = duckdb.connect()
    try:
        daily = connection.execute(
            "SELECT order_date, country, order_count, return_invoice_count, gross_sales_gbp, "
            "return_value_gbp, net_revenue_gbp, units_sold, units_returned, line_count "
            f"FROM read_parquet('{daily_path}') ORDER BY order_date, country"
        ).df()
        products = connection.execute(
            "SELECT stock_code, description, net_units, net_revenue_gbp, line_count "
            f"FROM read_parquet('{product_path}') ORDER BY net_revenue_gbp DESC NULLS LAST LIMIT 100"
        ).df()
        customer_metrics = connection.execute(
            "SELECT count(*) AS known_customers, sum(order_count) AS customer_orders, "
            "sum(net_revenue_gbp) AS known_customer_net_revenue_gbp "
            f"FROM read_parquet('{customer_path}')"
        ).fetchone()
        overall = connection.execute(
            "SELECT sum(order_count) AS order_count, sum(net_revenue_gbp) AS net_revenue_gbp, "
            "sum(units_sold) AS units_sold, sum(units_returned) AS units_returned "
            f"FROM read_parquet('{daily_path}')"
        ).fetchone()
    finally:
        connection.close()

    daily.to_csv(REPORT_ROOT / "daily_sales.csv", index=False)
    products.to_csv(REPORT_ROOT / "top_products.csv", index=False)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "order_count": int(overall[0] or 0),
        "net_revenue_gbp": float(overall[1] or 0),
        "units_sold": int(overall[2] or 0),
        "units_returned": int(overall[3] or 0),
        "known_customers": int(customer_metrics[0] or 0),
        "known_customer_orders": int(customer_metrics[1] or 0),
        "known_customer_net_revenue_gbp": float(customer_metrics[2] or 0),
        "daily_sales_rows": len(daily),
        "top_products_included": len(products),
    }
    (REPORT_ROOT / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Reports written to {REPORT_ROOT}")
    return summary

