"""Job-title extraction + mapping CSV generator for the MCP layer.

Two-step flow (MCP "extract-then-confirm"):
  1. Caller invokes the tool WITHOUT a mapping. We extract distinct DSP titles
     after the vendor fallback chain, return them + the Amazon catalog. Claude
     produces the mapping in its conversation turn.
  2. Caller re-invokes WITH the mapping dict. We write a 2-column CSV
     (DSP Job Title | Amazon Job Title) to the audit inbox.

Vendor fallback chains (per row, when the primary field is blank):
  - ADP:    Job Title Description -> Department Description
  - Paycom: Position -> Business_Title -> Job_Title_Description -> Department_Desc
"""
from __future__ import annotations

import io
import os
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

from utils.audit_utils import find_header_and_data

CATALOG_PATH = Path(__file__).parent.parent / "templates" / "amazon_job_titles.csv"

ADP_FALLBACK_COLUMNS = ["Job Title Description", "Department Description"]
PAYCOM_FALLBACK_COLUMNS = [
    "Position",
    "Business_Title",
    "Job_Title_Description",
    "Department_Desc",
]


def _norm(s) -> str:
    if s is None:
        return ""
    try:
        if pd.isna(s):
            return ""
    except (TypeError, ValueError):
        pass
    out = re.sub(r"\s+", " ", str(s)).strip()
    return "" if out.lower() == "nan" else out


def _find_column(df: pd.DataFrame, target: str):
    t = _norm(target).lower()
    for col in df.columns:
        if _norm(col).lower() == t:
            return col
    return None


def load_amazon_catalog() -> list[dict]:
    df = pd.read_csv(CATALOG_PATH, dtype=str).fillna("")
    df = df[df["Job Title"].str.strip() != ""]
    return df.to_dict(orient="records")


def extract_distinct_titles(content: bytes, filename: str, vendor: str) -> list[str]:
    df, _, _ = find_header_and_data(content, filename)
    chain = ADP_FALLBACK_COLUMNS if vendor.lower() == "adp" else PAYCOM_FALLBACK_COLUMNS

    cols: list[str] = []
    for target in chain:
        actual = _find_column(df, target)
        if actual and actual in df.columns and actual not in cols:
            cols.append(actual)

    if not cols:
        return []

    titles: set[str] = set()
    for _, row in df[cols].iterrows():
        for c in cols:
            v = _norm(row[c])
            if v:
                titles.add(v)
                break

    return sorted(titles, key=str.lower)


def write_mapping_csv(
    mapping: dict[str, str],
    vendor: str,
    out_dir: str,
) -> tuple[str, int]:
    """Writes the 2-column CSV. Returns (out_path, row_count)."""
    rows = [
        {"DSP Job Title": _norm(k), "Amazon Job Title": _norm(v)}
        for k, v in mapping.items()
        if _norm(k)
    ]
    df = pd.DataFrame(rows, columns=["DSP Job Title", "Amazon Job Title"])
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = os.path.join(out_dir, f"{vendor.lower()}_job_title_mapping_{stamp}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path, len(df)
