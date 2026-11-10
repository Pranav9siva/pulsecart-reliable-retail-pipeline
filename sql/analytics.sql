-- Run these examples in DuckDB from the project root. The report command
-- already executes the same analyses and writes CSV/JSON files to data/reports.

SELECT order_date,
       country,
       order_count,
       gross_sales_gbp,
       return_value_gbp,
       net_revenue_gbp
FROM read_parquet('data/lake/gold/mart_daily_sales/**/*.parquet')
ORDER BY order_date, country;

SELECT stock_code,
       description,
       net_units,
       net_revenue_gbp
FROM read_parquet('data/lake/gold/mart_product_performance/**/*.parquet')
ORDER BY net_revenue_gbp DESC
LIMIT 20;

SELECT customer_id,
       country,
       order_count,
       net_revenue_gbp
FROM read_parquet('data/lake/gold/mart_customer_summary/**/*.parquet')
ORDER BY net_revenue_gbp DESC
LIMIT 20;

