from __future__ import annotations

import hashlib
import json
import shutil
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pulsecart.config import SOURCE_URL, SOURCE_XLSX, SOURCE_ZIP, SOURCE_CSV, MANIFEST, ensure_project_dirs


COLUMN_MAP = {
    "invoiceno": "invoice_no",
    "invoice": "invoice_no",
    "stockcode": "stock_code",
    "description": "description",
    "quantity": "quantity",
    "invoicedate": "invoice_date",
    "price": "unit_price",
    "unitprice": "unit_price",
    "customerid": "customer_id",
    "country": "country",
}
CANONICAL_COLUMNS = [
    "source_row_id", "source_sheet", "source_row_number", "invoice_no", "stock_code",
    "description", "quantity", "invoice_date", "unit_price", "customer_id", "country",
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _get_workbook() -> Path:
    if SOURCE_XLSX.exists():
        return SOURCE_XLSX
    if not SOURCE_ZIP.exists():
        print(f"Downloading the UCI source archive to {SOURCE_ZIP}")
        request = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "PulseCart-data-pipeline/1.0"})
        with urllib.request.urlopen(request, timeout=60) as response, SOURCE_ZIP.open("wb") as output:
            shutil.copyfileobj(response, output)

    expected_name = "online_retail_ii.xlsx"
    with zipfile.ZipFile(SOURCE_ZIP) as archive:
        member = next((name for name in archive.namelist() if Path(name).name.lower() == expected_name), None)
        if member is None:
            raise FileNotFoundError(f"{expected_name} was not present in the UCI archive")
        with archive.open(member) as source, SOURCE_XLSX.open("wb") as output:
            shutil.copyfileobj(source, output)
    return SOURCE_XLSX


def _normalize_sheet(frame: pd.DataFrame, sheet: str) -> pd.DataFrame:
    normalized = {}
    for column in frame.columns:
        key = "".join(character for character in str(column).lower() if character.isalnum())
        if key in COLUMN_MAP:
            normalized[column] = COLUMN_MAP[key]
    frame = frame.rename(columns=normalized)
    missing = set(COLUMN_MAP.values()) - set(frame.columns)
    if missing:
        raise ValueError(f"Unexpected workbook schema in {sheet}: missing {sorted(missing)}")

    frame = frame[["invoice_no", "stock_code", "description", "quantity", "invoice_date", "unit_price", "customer_id", "country"]].copy()
    for column in frame.columns:
        frame[column] = frame[column].astype("string").str.strip()
        frame[column] = frame[column].replace({"<NA>": None, "nan": None, "NaN": None})
    dates = pd.to_datetime(frame["invoice_date"], errors="coerce")
    frame["invoice_date"] = dates.dt.strftime("%Y-%m-%d %H:%M:%S").where(dates.notna(), None)
    frame.insert(0, "source_row_number", range(2, len(frame) + 2))
    frame.insert(0, "source_sheet", sheet)
    frame.insert(0, "source_row_id", [f"{sheet}:{row}" for row in range(2, len(frame) + 2)])
    return frame[CANONICAL_COLUMNS]


def extract(force: bool = False) -> dict:
    ensure_project_dirs()
    if SOURCE_CSV.exists() and MANIFEST.exists() and not force:
        manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
        print(f"Using cached extraction: {manifest['total_rows']:,} rows")
        return manifest

    workbook = _get_workbook()
    excel = pd.ExcelFile(workbook, engine="openpyxl")
    sheet_counts: dict[str, int] = {}
    with SOURCE_CSV.open("w", encoding="utf-8", newline="") as output:
        for index, sheet in enumerate(excel.sheet_names):
            frame = pd.read_excel(excel, sheet_name=sheet, dtype="string")
            canonical = _normalize_sheet(frame, sheet)
            canonical.to_csv(output, index=False, header=index == 0)
            sheet_counts[sheet] = len(canonical)
            print(f"Extracted {sheet}: {len(canonical):,} rows")

    manifest = {
        "dataset": "Online Retail II",
        "publisher": "UCI Machine Learning Repository",
        "creator": "Daqing Chen",
        "doi": "10.24432/C5CG6D",
        "license": "CC BY 4.0",
        "source_url": "https://archive.ics.uci.edu/dataset/502/online+retail+ii",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "workbook_sha256": _sha256(workbook),
        "workbook_bytes": workbook.stat().st_size,
        "csv_sha256": _sha256(SOURCE_CSV),
        "sheet_rows": sheet_counts,
        "total_rows": sum(sheet_counts.values()),
        "columns": CANONICAL_COLUMNS,
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote canonical CSV: {SOURCE_CSV} ({manifest['total_rows']:,} rows)")
    return manifest

