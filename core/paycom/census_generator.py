"""Paycom -> Uzio Census Template Generator (ported from apps/paycom/census_generator.py).

Public entry point: run_paycom_census_generation(file_content, filename, fix_options=None)
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

PAYCOM_GENERATOR_FIELD_MAP = {
    'Employee ID': ['Employee_Code'],
    'First Name': ['Legal_Firstname'],
    'Last Name': ['Legal_Lastname'],
    'Middle Initial': ['Legal_Middle_Name'],
    'Employment Status': ['Employee_Status'],
    'Employment Type': ['DOL_Status'],
    'Hire Date': ['Most_Recent_Hire_Date'],
    'Original Hire Date': ['Hire_Date'],
    'Termination Date': ['Termination_Date'],
    'Termination Reason': ['Termination_Reason'],
    'Pay Type': ['Pay_Type'],
    'Annual Salary': ['Annual_Salary'],
    'Hourly Pay Rate': ['Rate_1'],
    'Working Hours': ['Scheduled_Pay_Period_Hours'],
    'Job Title': ['Position'],
    'Department': ['Department_Desc'],
    'Work Email': ['Work_Email'],
    'Personal Email': ['Personal_Email'],
    'Phone Number': ['Primary_Phone'],
    'SSN': ['SS_Number'],
    'DOB': ['Birth_Date_(MM/DD/YYYY)'],
    'Gender': ['Gender'],
    'Tobacco User': ['Tobacco_User'],
    'FLSA Classification': ['Exempt_Status'],
    'Address Line 1': ['Primary_Address_Line_1'],
    'Address Line 2': ['Primary_Address_Line_2'],
    'City': ['Primary_City/Municipality'],
    'Zip': ['Primary_Zip/Postal_Code'],
    'State': ['Primary_State/Province'],
    'Mailing Address Line 1': ['Mailing_Address_Line_1'],
    'Mailing Address Line 2': ['Mailing_Address_Line_2'],
    'Mailing City': ['Mailing_City/Municipality'],
    'Mailing Zip': ['Mailing_Zip/Postal_Code'],
    'Mailing State': ['Mailing_State/Province'],
    'License Number': ['DriversLicense'],
    'License Expiration Date': ['DLExpirationDate'],
    'Work Location': ['Work_Location'],
}


def _norm_colname(c: str) -> str:
    if c is None:
        return ""
    c = str(c).replace("\n", " ").replace("\r", " ").replace(" ", " ")
    c = c.replace("’", "'").replace("“", '"').replace("”", '"')
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
    norm_cols = set(df_columns)
    resolved = {}
    for std_name, vendor_cols in PAYCOM_GENERATOR_FIELD_MAP.items():
        match = None
        for vc in vendor_cols:
            n = _norm_colname(vc)
            if n in norm_cols:
                match = n
                break
        resolved[std_name] = match if match else _norm_colname(vendor_cols[0])
    return resolved


def run_paycom_census_generation(file_content: bytes, filename: str, fix_options: dict | None = None,
                                 template_path: str | None = None) -> tuple[bytes, dict]:
    """Generate a Uzio Census Template from a Paycom Census export.

    Returns (xlsm_bytes, summary).
    """
    fix_options = fix_options or {}

    dupes = _check_duplicate_columns(file_content, filename)
    if dupes:
        raise ValueError(
            f"Duplicate column headers in source file: {dupes}. "
            "Pandas cannot process duplicate headers — clean the source and retry."
        )

    df_paycom = _read_source(file_content, filename)
    original_columns = list(df_paycom.columns)
    df_paycom.columns = [_norm_colname(c) for c in df_paycom.columns]

    resolved_field_map = _resolve_field_map(df_paycom.columns)

    df_uzio = generate_uzio_template(df_paycom, resolved_field_map, fix_options=fix_options)

    src_job_col = resolved_field_map.get('Job Title')
    src_loc_col = resolved_field_map.get('Work Location')
    src_dept_col = resolved_field_map.get('Department')
    if src_job_col and src_job_col in df_paycom.columns:
        df_uzio['Job Title'] = df_paycom[src_job_col].fillna("").astype(str).str.strip().values[: len(df_uzio)]
    if src_loc_col and src_loc_col in df_paycom.columns:
        df_uzio['Work Location'] = df_paycom[src_loc_col].fillna("").astype(str).str.strip().values[: len(df_uzio)]
    if src_dept_col and src_dept_col in df_paycom.columns:
        df_uzio['Department'] = df_paycom[src_dept_col].fillna("").astype(str).str.strip().values[: len(df_uzio)]

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
        "rows_in_source": len(df_paycom),
        "rows_in_uzio_output": len(df_uzio),
        "applied_toggles": {k: bool(v) for k, v in fix_options.items() if v},
        "auto_fix_count": int(len(fix_logs_df)) if isinstance(fix_logs_df, pd.DataFrame) else 0,
        "auto_fix_log_preview": (
            fix_logs_df.head(50).to_dict(orient="records")
            if isinstance(fix_logs_df, pd.DataFrame) and not fix_logs_df.empty
            else []
        ),
        "unmapped_standard_fields": [
            std for std, col in resolved_field_map.items() if col not in df_paycom.columns
        ],
        "source_columns_seen": original_columns,
    }
    return out.getvalue(), summary
