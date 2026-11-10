from __future__ import annotations

import csv
import json
import os
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

from pulsecart.config import SOURCE_CSV, ensure_project_dirs


TOPIC = "retail.order_events"


def replay_events(
    limit: int = 5_000,
    batch_size: int = 500,
    interval: float = 0.5,
    run_id: str | None = None,
    bootstrap_servers: str = "localhost:29092",
    topic: str = TOPIC,
) -> dict:
    ensure_project_dirs()
    if not SOURCE_CSV.exists():
        raise FileNotFoundError(f"Canonical source CSV not found: {SOURCE_CSV}. Run `pulsecart extract` first.")
    if batch_size < 1 or interval < 0 or limit < 0:
        raise ValueError("batch_size must be positive; interval and limit cannot be negative")

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    producer = KafkaProducer(
        bootstrap_servers=[value.strip() for value in bootstrap_servers.split(",")],
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
    )
    sent = 0
    try:
        with SOURCE_CSV.open("r", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            pending = 0
            for row in reader:
                if limit and sent >= limit:
                    break
                event = {
                    "event_id": row["source_row_id"],
                    "schema_version": 1,
                    "event_time": row["invoice_date"],
                    "source": "uci_online_retail_ii_historical_replay",
                    "replay_run_id": run_id,
                    "payload": {
                        "source_row_id": row["source_row_id"],
                        "invoice_no": row["invoice_no"],
                        "stock_code": row["stock_code"],
                        "description": row["description"],
                        "quantity": row["quantity"],
                        "invoice_date": row["invoice_date"],
                        "unit_price": row["unit_price"],
                        "customer_id": row["customer_id"],
                        "country": row["country"],
                    },
                }
                producer.send(topic, key=event["event_id"], value=event)
                sent += 1
                pending += 1
                if pending >= batch_size:
                    producer.flush()
                    pending = 0
                    print(f"Run {run_id}: published {sent:,} events")
                    if interval:
                        time.sleep(interval)
            producer.flush()
    finally:
        producer.close()

    result = {
        "topic": topic,
        "run_id": run_id,
        "bootstrap_servers": bootstrap_servers,
        "events_published": sent,
        "limit": limit or "all",
    }
    print(json.dumps(result, indent=2))
    return result

