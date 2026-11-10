from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(os.environ.get("PULSECART_HOME", Path(__file__).resolve().parents[1])).resolve()
REPORT_ROOT = PROJECT_ROOT / "data" / "reports"


@st.cache_data(ttl=60)
def load_reports(report_root: str) -> tuple[pd.DataFrame, pd.DataFrame, dict, dict]:
    root = Path(report_root)
    sales = pd.read_csv(root / "daily_sales.csv", parse_dates=["order_date"])
    products = pd.read_csv(root / "top_products.csv")
    quality = json.loads((root / "quality_report.json").read_text(encoding="utf-8"))
    summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))

    numeric_columns = (
        "order_count", "return_invoice_count", "gross_sales_gbp", "return_value_gbp",
        "net_revenue_gbp", "units_sold", "units_returned", "line_count",
    )
    for column in numeric_columns:
        sales[column] = pd.to_numeric(sales[column], errors="coerce").fillna(0)
    products["net_units"] = pd.to_numeric(products["net_units"], errors="coerce").fillna(0)
    products["net_revenue_gbp"] = pd.to_numeric(products["net_revenue_gbp"], errors="coerce").fillna(0)
    products["line_count"] = pd.to_numeric(products["line_count"], errors="coerce").fillna(0)
    return sales, products, quality, summary


st.set_page_config(
    page_title="PulseCart Retail Analytics",
    page_icon="🛒",
    layout="wide",
)

st.title("PulseCart | Retail Performance")
st.caption("Historical UCI Online Retail II transactions • GBP • Refreshed from the latest pipeline reports")

required_reports = (
    REPORT_ROOT / "daily_sales.csv",
    REPORT_ROOT / "top_products.csv",
    REPORT_ROOT / "quality_report.json",
    REPORT_ROOT / "summary.json",
)
missing_reports = [path.name for path in required_reports if not path.exists()]
if missing_reports:
    st.warning("Pipeline reports are not available yet. Run the pulsecart_retail_batch DAG, then refresh this page.")
    st.caption("Missing: " + ", ".join(missing_reports))
    st.stop()

sales, products, quality, summary = load_reports(str(REPORT_ROOT))
if sales.empty:
    st.info("The daily sales report is empty. Run the batch pipeline to create report data.")
    st.stop()

minimum_date = sales["order_date"].min().date()
maximum_date = sales["order_date"].max().date()
countries = sorted(sales["country"].dropna().astype(str).unique().tolist())

st.sidebar.header("Filters")
date_selection = st.sidebar.date_input(
    "Order date range",
    value=(minimum_date, maximum_date),
    min_value=minimum_date,
    max_value=maximum_date,
)
selected_countries = st.sidebar.multiselect("Country", countries, default=countries)

if isinstance(date_selection, (tuple, list)) and len(date_selection) == 2:
    start_date, end_date = date_selection
else:
    start_date = end_date = date_selection

filtered = sales.loc[
    sales["order_date"].dt.date.between(start_date, end_date)
    & sales["country"].isin(selected_countries)
].copy()

st.caption(f"Showing {start_date:%d %b %Y} to {end_date:%d %b %Y} across {len(selected_countries):,} countries")
if filtered.empty:
    st.info("No rows match these filters. Adjust the date range or select one or more countries.")
else:
    net_revenue = float(filtered["net_revenue_gbp"].sum())
    order_count = int(filtered["order_count"].sum())
    units_sold = int(filtered["units_sold"].sum())
    units_returned = int(filtered["units_returned"].sum())
    return_rate = (units_returned / units_sold * 100) if units_sold else 0.0

    metric_cols = st.columns(4)
    metric_cols[0].metric("Net revenue", f"£{net_revenue:,.2f}")
    metric_cols[1].metric("Orders", f"{order_count:,}")
    metric_cols[2].metric("Units sold", f"{units_sold:,}")
    metric_cols[3].metric("Units returned", f"{units_returned:,}", f"{return_rate:.1f}% of units sold")

    st.subheader("Daily net revenue")
    daily = (
        filtered.groupby("order_date", as_index=False)
        .agg(net_revenue_gbp=("net_revenue_gbp", "sum"))
        .sort_values("order_date")
    )
    st.line_chart(daily.set_index("order_date")["net_revenue_gbp"], y_label="Net revenue (£)")

    left, right = st.columns([1.2, 1])
    with left:
        st.subheader("Revenue by country")
        country_sales = (
            filtered.groupby("country", as_index=False)
            .agg(net_revenue_gbp=("net_revenue_gbp", "sum"), order_count=("order_count", "sum"))
            .sort_values("net_revenue_gbp", ascending=False)
            .head(15)
        )
        st.bar_chart(country_sales.set_index("country")["net_revenue_gbp"], y_label="Net revenue (£)")
    with right:
        st.subheader("Top products overall")
        product_view = products.head(10).rename(columns={
            "stock_code": "Stock code",
            "description": "Product",
            "net_units": "Net units",
            "net_revenue_gbp": "Net revenue (£)",
            "line_count": "Lines",
        })
        st.dataframe(product_view, hide_index=True, use_container_width=True)

    with st.expander("Top 15 countries detail"):
        st.dataframe(country_sales, hide_index=True, use_container_width=True)

st.divider()
st.subheader("Pipeline quality")
quality_status = str(quality.get("status", "unknown")).upper()
if quality_status == "PASSED":
    st.success(f"Quality checks {quality_status.lower()} — source-to-output reconciliation is complete.")
else:
    st.error(f"Quality checks {quality_status.lower()} — inspect data/reports/quality_report.json.")

quality_cols = st.columns(4)
quality_cols[0].metric("Source rows", f"{int(quality.get('source_rows', 0)):,}")
quality_cols[1].metric("Accepted", f"{int(quality.get('accepted_rows', 0)):,}")
quality_cols[2].metric("Quarantined", f"{int(quality.get('quarantined_rows', 0)):,}")
quality_cols[3].metric("Quarantine rate", f"{float(quality.get('quarantine_rate', 0)):.3%}")

last_generated = summary.get("generated_at_utc", "not recorded")
st.caption(f"Reports generated at {last_generated} • Source: UCI Online Retail II")


