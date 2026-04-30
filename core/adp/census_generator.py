"""ADP -> Uzio Census Template Generator (ported from apps/adp/census_generator.py).

Public entry point: run_adp_census_generation(file_content, filename, fix_options=None)
returns (xlsm_bytes, summary_dict).
"""
import io
import re
import pandas as pd

from utils.audit_utils import (
    generate_uzio_template,
    inject_into_uzio_template,
    resolve_uzio_template_path,
)

ADP_GENERATOR_FIELD_MAP = {
    'Employee ID': ['Associate ID'],
    'First Name': ['Legal First Name'],
    'Last Name': ['Legal Last Name'],
    'Middle Initial': ['Legal Middle Name'],
    'Suffix': ['Generation Suffix Code'],
    'Employment Status': ['Position Status'],
    'Employment Type': ['Worker Category Description'],
    'Hire Date': ['Hire/Rehire Date'],
    'Original Hire Date': ['Hire Date'],
    'Termination Date': ['Termination Date'],
    'Termination Reason': ['Termination Reason Description'],
    'Pay Type': ['Regular Pay Rate Description'],
    'Annual Salary': ['Annual Salary'],
    'Hourly Pay Rate': ['Regular Pay Rate Amount'],
    'Working Hours': ['Standard Hours'],
    'Job Title': ['Job Title Description'],
    'Department': ['Department Description'],
    'Work Email': ['Work Contact: Work Email'],
    'Personal Email': ['Personal Contact: Personal Email'],
    'SSN': ['Tax ID (SSN)'],
    'DOB': ['Birth Date'],
    'Gender': ['Gender / Sex (Self-ID)'],
    'Tobacco User': ['Tobacco User'],
    'FLSA Classification': ['FLSA Description'],
    'Address Line 1': ['Primary Address: Address Line 1'],
    'Address Line 2': ['Primary Address: Address Line 2'],
    'City': ['Primary Address: City'],
    'Zip': ['Primary Address: Zip / Postal Code'],
    'State': ['Primary Address: State / Territory Code'],
    'Mailing Address Line 1': ['Legal / Preferred Address: Address Line 1'],
    'Mailing Address Line 2': ['Legal / Preferred Address: Address Line 2'],
    'Mailing City': ['Legal / Preferred Address: City'],
    'Mailing Zip': ['Legal / Preferred Address: Zip / Postal Code'],
    'Mailing State': ['Legal / Preferred Address: State / Territory Code'],
    'Reports To ID': ['Reports To Associate ID'],
    'Work Location': ['Location Description'],
}


def _norm_colname(c: str) -> str:
    if c is None:
        return ""
    c = str(c).replace("\n", " ").replace("\r", " ").replace(" ", " ")
    c = re.sub(r'\(.*?\)', '', c)
    c = re.sub(r"\s+", " ", c).strip()
    c = c.replace("*", "").strip('"').strip("'")
    return c.lower()


def _read_source(content: bytes, filename: str) -> pd.DataFrame:
    is_csv = str(filename).lower().endswith('.csv')
    bio = io.BytesIO(content)
    if is_csv:
        try:
            return pd.read_csv(bio, dtype=str)
        except UnicodeDecodeError:
            bio.seek(0)
            return pd.read_csv(bio, dtype=str, encoding='latin1')
    return pd.read_excel(bio, dtype=str)


def _check_duplicate_columns(content: bytes, filename: str):
    bio = io.BytesIO(content)
    try:
        if str(filename).lower().endswith('.csv'):
            df_h = pd.read_csv(bio, header=None, nrows=1)
        else:
            df_h = pd.read_excel(bio, header=None, nrows=1)
    except Exception:
        return None
    if df_h.empty:
        return None
    headers = [str(h).strip() for h in df_h.iloc[0].tolist() if pd.notna(h) and str(h).strip()]
    seen, dupes = set(), []
    for h in headers:
        if h in seen and h not in dupes:
            dupes.append(h)
        seen.add(h)
    return dupes or None


def _resolve_field_map(df_columns):
    """Normalize columns and pick the first vendor-name alias that exists."""
    norm_cols = set(df_columns)
    resolved = {}
    for std_name, vendor_cols in ADP_GENERATOR_FIELD_MAP.items():
        match = None
        for vc in vendor_cols:
            n = _norm_colname(vc)
            if n in norm_cols:
                match = n
                break
        # Streamlit code preserves the first alias even when missing — preserve that
        # so generate_uzio_template can detect "missing" via the .columns check.
        resolved[std_name] = match if match else _norm_colname(vendor_cols[0])
    return resolved


def run_adp_census_generation(file_content: bytes, filename: str, fix_options: dict | None = None,
                              template_path: str | None = None) -> tuple[bytes, dict]:
    """Generate a Uzio Census Template from an ADP Census export.

    Returns (xlsm_bytes, summary). Raises on hard errors (missing template, duplicate
    headers, unreadable file).
    """
    fix_options = fix_options or {}

    dupes = _check_duplicate_columns(file_content, filename)
    if dupes:
        raise ValueError(
            f"Duplicate column headers in source file: {dupes}. "
            "Pandas cannot process duplicate headers — clean the source and retry."
        )

    df_adp = _read_source(file_content, filename)
    original_columns = list(df_adp.columns)
    df_adp.columns = [_norm_colname(c) for c in df_adp.columns]

    resolved_field_map = _resolve_field_map(df_adp.columns)

    df_uzio = generate_uzio_template(df_adp, resolved_field_map, fix_options=fix_options)

    # Pull Job Title and Work Location verbatim from source (no per-row mapping UI in MCP context).
    src_job_col = resolved_field_map.get('Job Title')
    src_loc_col = resolved_field_map.get('Work Location')
    src_dept_col = resolved_field_map.get('Department')
    if src_job_col and src_job_col in df_adp.columns:
        df_uzio['Job Title'] = df_adp[src_job_col].fillna("").astype(str).str.strip().values[: len(df_uzio)]
    if src_loc_col and src_loc_col in df_adp.columns:
        df_uzio['Work Location'] = df_adp[src_loc_col].fillna("").astype(str).str.strip().values[: len(df_uzio)]
    if src_dept_col and src_dept_col in df_adp.columns:
        df_uzio['Department'] = df_adp[src_dept_col].fillna("").astype(str).str.strip().values[: len(df_uzio)]

    # Inject into the .xlsm template
    tpl_path = template_path or resolve_uzio_template_path()
    if not tpl_path:
        raise FileNotFoundError(
            "Uzio_Census_Template.xlsm not found. Place it under audit_fast_api/templates/ "
            "or in the parent project's templates/ folder."
        )
    wb = inject_into_uzio_template(df_uzio.reset_index(drop=True), template_path=tpl_path)
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    fix_logs_df = df_uzio.attrs.get('fix_logs', pd.DataFrame())
    summary = {
        "rows_in_source": len(df_adp),
        "rows_in_uzio_output": len(df_uzio),
        "applied_toggles": {k: bool(v) for k, v in fix_options.items() if v},
        "auto_fix_count": int(len(fix_logs_df)) if isinstance(fix_logs_df, pd.DataFrame) else 0,
        "auto_fix_log_preview": (
            fix_logs_df.head(50).to_dict(orient="records")
            if isinstance(fix_logs_df, pd.DataFrame) and not fix_logs_df.empty
            else []
        ),
        "unmapped_standard_fields": [
            std for std, col in resolved_field_map.items() if col not in df_adp.columns
        ],
        "source_columns_seen": original_columns,
    }
    return out.getvalue(), summary
