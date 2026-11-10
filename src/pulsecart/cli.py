from __future__ import annotations

import argparse

from pulsecart.batch import run_batch
from pulsecart.quality import run_quality_checks
from pulsecart.replay import replay_events
from pulsecart.report import build_reports
from pulsecart.source import extract
from pulsecart.stream import start_stream


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="pulsecart", description="PulseCart retail pipeline commands")
    commands = result.add_subparsers(dest="command", required=True)

    extract_command = commands.add_parser("extract", help="download/read UCI workbook and create canonical CSV")
    extract_command.add_argument("--force", action="store_true", help="rebuild the canonical CSV")

    commands.add_parser("batch", help="build bronze, silver, quarantine, and gold Parquet datasets")
    commands.add_parser("quality", help="run data quality and reconciliation checks")
    commands.add_parser("report", help="create CSV and JSON analytics outputs with DuckDB")

    replay = commands.add_parser("replay", help="publish historical source rows as JSON events to Kafka")
    replay.add_argument("--limit", type=int, default=5_000, help="maximum events; use 0 for all source rows")
    replay.add_argument("--batch-size", type=int, default=500)
    replay.add_argument("--interval", type=float, default=0.5, help="seconds to pause between published batches")
    replay.add_argument("--run-id", default=None)
    replay.add_argument("--bootstrap-servers", default=None)
    replay.add_argument("--topic", default="retail.order_events")

    stream = commands.add_parser("stream", help="process Kafka JSON events into Parquet with PySpark")
    stream.add_argument("--bootstrap-servers", default=None)
    stream.add_argument("--topic", default="retail.order_events")

    return result


def main() -> None:
    args = parser().parse_args()
    if args.command == "extract":
        extract(force=args.force)
    elif args.command == "batch":
        run_batch()
    elif args.command == "quality":
        run_quality_checks()
    elif args.command == "report":
        build_reports()
    elif args.command == "replay":
        import os
        replay_events(
            limit=args.limit,
            batch_size=args.batch_size,
            interval=args.interval,
            run_id=args.run_id,
            bootstrap_servers=args.bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            topic=args.topic,
        )
    elif args.command == "stream":
        import os
        start_stream(
            bootstrap_servers=args.bootstrap_servers or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            topic=args.topic,
        )


if __name__ == "__main__":
    main()

