from __future__ import annotations

import os
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = Path(os.environ.get("PULSECART_HOME", PACKAGE_ROOT)).resolve()
DATA_ROOT = PROJECT_ROOT / "data"
RAW_ROOT = DATA_ROOT / "raw"
SOURCE_ROOT = DATA_ROOT / "source"
EVENT_ROOT = DATA_ROOT / "events" / "incoming"
LAKE_ROOT = DATA_ROOT / "lake"
REPORT_ROOT = DATA_ROOT / "reports"

SOURCE_URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
SOURCE_ZIP = RAW_ROOT / "uci_online_retail_ii.zip"
SOURCE_XLSX = RAW_ROOT / "online_retail_II.xlsx"
SOURCE_CSV = SOURCE_ROOT / "online_retail_ii.csv"
MANIFEST = SOURCE_ROOT / "extraction_manifest.json"


def ensure_project_dirs() -> None:
    for path in (RAW_ROOT, SOURCE_ROOT, EVENT_ROOT, LAKE_ROOT, REPORT_ROOT):
        path.mkdir(parents=True, exist_ok=True)

