import io
import pandas as pd

# =========================================================
# ADP FIT/SIT Sanity Check (MCP core port)
#
# Ported verbatim from the Streamlit module
# ../../apps/adp/fit_sit_sanity.py (root repo, sidebar entry
# "ADP - FIT/SIT Sanity Check"). The ONLY difference is I/O shape: this version
# takes (content: bytes, filename: str) and exposes
# run_adp_fit_sit_sanity() returning (xlsx_bytes, csv_bytes, summary_dict).
# The fill logic is kept in sync with the Streamlit version -- fix bugs in BOTH.
#
# - Input: single ADP FIT/SIT export (.csv / .xlsx)
# - Fills blanks in three columns with hardcoded Uzio defaults:
#     1) Dependents                          -> 0
#     2) Non-Resident Alien                  -> No
#     3) State Marital Status Description    -> Single
# - Everything else is handled downstream by the API.
# =========================================================

DEFAULTS = {
    "Dependents": "0",
    "Non-Resident Alien": "No",
    "State Marital Status Description": "Single",
}


def _is_blank(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and pd.isna(v):
        return True
    s = str(v).strip()
    return s == "" or s.lower() == "nan"


def _read_file(content: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    else:
        df = pd.read_excel(io.BytesIO(content), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _find_col(df: pd.DataFrame, target: str) -> str:
    """Exact match first, then case-insensitive."""
    if target in df.columns:
        return target
    target_lower = target.casefold()
    for c in df.columns:
        if c.casefold() == target_lower:
            return c
    return ""


def run_adp_fit_sit_sanity(content: bytes, filename: str = "adp_fit_sit.xlsx"):
    """Fill the three FIT/SIT blank-default columns and return artifacts.

    Returns (xlsx_bytes, csv_bytes, summary_dict). The xlsx has three sheets
    (Summary, Changes, Corrected_Source); the csv is the Corrected_Source as
    plain UTF-8 (NO BOM) for API ingestion. summary_dict is JSON-serializable.
    """
    df = _read_file(content, filename)

    # Resolve column names (defensive -- should be exact)
    resolved: dict[str, str] = {}
    missing: list[str] = []
    for target in DEFAULTS:
        col = _find_col(df, target)
        if col:
            resolved[target] = col
        else:
            missing.append(target)

    if missing:
        raise ValueError(
            "Could not find required column(s) in the file: " + ", ".join(missing)
        )

    df_fixed = df.copy()

    # Pick out the ID + name columns for the change log (best-effort)
    id_col = _find_col(df, "Associate ID")
    first_col = _find_col(df, "Legal First Name")
    last_col = _find_col(df, "Legal Last Name")

    change_rows: list[dict] = []
    fill_counts: dict[str, int] = {t: 0 for t in DEFAULTS}

    for idx, row in df.iterrows():
        for target, default in DEFAULTS.items():
            col = resolved[target]
            if _is_blank(row.get(col)):
                df_fixed.at[idx, col] = default
                fill_counts[target] += 1

                emp_id = str(row.get(id_col, "")).strip() if id_col else ""
                fname = str(row.get(first_col, "")).strip() if first_col else ""
                lname = str(row.get(last_col, "")).strip() if last_col else ""
                emp_name = f"{fname} {lname}".strip()

                change_rows.append({
                    "Associate ID": emp_id,
                    "Employee Name": emp_name,
                    "Column": target,
                    "Filled With": default,
                })

    changes_df = pd.DataFrame(
        change_rows,
        columns=["Associate ID", "Employee Name", "Column", "Filled With"],
    )

    summary_df = pd.DataFrame({
        "Metric": [
            "Total rows",
            "Rows with at least one blank filled",
            "Dependents blanks filled",
            "Non-Resident Alien blanks filled",
            "State Marital Status Description blanks filled",
            "Total blanks filled",
        ],
        "Value": [
            len(df),
            changes_df["Associate ID"].nunique() if not changes_df.empty else 0,
            fill_counts["Dependents"],
            fill_counts["Non-Resident Alien"],
            fill_counts["State Marital Status Description"],
            sum(fill_counts.values()),
        ],
    })

    # Stringify everything to keep long numeric strings (e.g. amounts, IDs)
    # from being emitted in exponential notation in either output.
    df_fixed_clean = df_fixed.fillna("").astype(str)
    df_fixed_clean = df_fixed_clean.replace({"nan": "", "NaN": "", "None": ""})

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        changes_df.to_excel(writer, sheet_name="Changes", index=False)
        df_fixed_clean.to_excel(writer, sheet_name="Corrected_Source", index=False)

    # Bare UTF-8 (NO BOM). Downstream APIs match the first header literally; a
    # utf-8-sig BOM smuggles U+FEFF in front of it and the column lookup silently
    # misses. Excel users should open the XLSX export instead.
    csv_bytes = df_fixed_clean.to_csv(index=False).encode("utf-8")

    summary = {
        "metrics": {row["Metric"]: int(row["Value"]) for _, row in summary_df.iterrows()},
        "changes": changes_df.to_dict("records"),
    }
    return out.getvalue(), csv_bytes, summary
