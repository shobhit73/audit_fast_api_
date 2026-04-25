import pandas as pd
import io
import re
import numpy as np
from datetime import datetime, date

# --- Constants from the original utils ---
UZIO_RAW_MAPPING = {
    'Employee ID*': 'Employee ID',
    'Employee First Name*': 'First Name',
    'Employee Last Name*': 'Last Name',
    'Employee Middle Initial': 'Middle Initial',
    'Employee Suffix': 'Suffix',
    'Employment Status*': 'Employment Status',
    'Date of Hire*': 'Hire Date',
    'Original DOH': 'Original Hire Date',
    'Termination Date': 'Termination Date',
    'Termination Reason': 'Termination Reason',
    'Employment Type*': 'Employment Type',
    'Pay Type*': 'Pay Type',
    'Annual Salary(Digits)**': 'Annual Salary',
    'Hourly Pay Rate**': 'Hourly Pay Rate',
    'Working Hours per Week(Digits)**': 'Working Hours',
    'Job Title': 'Job Title',
    'Department': 'Department',
    'Official Email*': 'Work Email',
    'Personal Email': 'Personal Email',
    'Phone Number(Digits)': 'Phone Number',
    'Employee SSN': 'SSN',
    'Employee Date of Birth*': 'DOB',
    'Employee Gender*': 'Gender',
    'Employee Tobacco usage in last 12 months': 'Tobacco User',
    'FLSA Classification': 'FLSA Classification',
    'Employee Address Line 1': 'Address Line 1',
    'Employee Address Line 2': 'Address Line 2',
    'City*': 'City',
    'Zipcode*': 'Zip',
    'State(Abbreviation)*': 'State',
    'Mailing Address Line 1': 'Mailing Address Line 1',
    'Mailing Address Line 2': 'Mailing Address Line 2',
    'Mailing City': 'Mailing City',
    'Mailing Zipcode': 'Mailing Zip',
    'Mailing State(Abbreviation)': 'Mailing State',
    'Reporting Manager ID': 'Reports To ID',
    'Work Location': 'Work Location',
    'License Number*': 'License Number',
    'License Expiration Date': 'License Expiration Date'
}

HOURLY_ONLY_JOB_TITLES = {
    "driver", "lead driver", "walker", "helper", "driver-lite", "driver-step van",
    "driver-unscheduled", "ddu dedicated", "ddu shared", "delivery associate",
    "delivery associates", "driver -major appliance"
}

# --- Core Utility Functions ---

def clean_money_val(x):
    if pd.isna(x) or x == "": return 0.0
    s = str(x).strip().replace("$", "").replace("%", "").replace(",", "")
    s = s.replace("(", "-").replace(")", "")
    try: return float(s)
    except: return 0.0

def norm_colname(c: str) -> str:
    if c is None: return ""
    c = str(c).replace("\n", " ").replace("\r", " ").replace("\u00A0", " ")
    c = c.replace("’", "'").replace("“", '"').replace("”", '"')
    c = re.sub(r'\(.*?\)', '', c)
    c = re.sub(r"\s+", " ", c).strip().replace("*", "")
    return c.strip('"').strip("'")

def norm_blank(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return ""
    if isinstance(x, str) and x.strip().lower() in {"", "nan", "none", "null"}: return ""
    return x

def normalize_id(x):
    if pd.isna(x) or x is None: return ""
    s = str(x).strip()
    if s.endswith(".0"): s = s[:-2]
    return s.lstrip("0") if s != "0" else "0"

def try_parse_date(x):
    x = norm_blank(x)
    if x == "": return ""
    try:
        ts = pd.to_datetime(x, errors='coerce')
        return ts.strftime("%m/%d/%Y") if pd.notna(ts) else str(x)
    except: return str(x)

def format_pay_date(date_val):
    if pd.isna(date_val) or str(date_val).strip() in ["", "nan", "NaT"]: return "Unknown"
    try:
        dt = pd.to_datetime(date_val)
        return dt.strftime('%Y-%m-%d')
    except: return str(date_val).strip()

def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    norm_cols = [norm_colname(c).casefold() for c in df.columns]
    seen, to_keep = set(), []
    for i, nc in enumerate(norm_cols):
        if nc not in seen:
            seen.add(nc)
            to_keep.append(i)
    return df.iloc[:, to_keep]

def safe_val(df, idx, col):
    if idx is None or col not in df.columns: return ""
    val = df.loc[idx, col]
    return val.iloc[0] if isinstance(val, pd.Series) else val

def norm_ssn_canonical(x):
    x = norm_blank(x)
    if x == "": return ""
    s = re.sub(r"\D", "", str(x).strip().replace("-", "").replace(" ", "").replace(".0", ""))
    return s.zfill(9)[-9:] if s else ""

def find_header_and_data(file_content, filename):
    file_io = io.BytesIO(file_content)
    if filename.lower().endswith('.csv'):
        df_peek = pd.read_csv(file_io, header=None, nrows=50)
        header_idx = 0
        for i, row in df_peek.iterrows():
            row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
            if any(k in row_str for k in ["employee id", "employee name", "associate id"]):
                header_idx = i
                break
        file_io.seek(0)
        df = pd.read_csv(file_io, header=header_idx)
        header_top = df_peek.iloc[header_idx - 1].tolist() if header_idx > 0 else None
    else:
        xls = pd.ExcelFile(file_io)
        sheet = xls.sheet_names[1] if len(xls.sheet_names) > 1 and "criteria" in xls.sheet_names[0].lower() else xls.sheet_names[0]
        df_peek = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=50)
        header_idx = 0
        for i, row in df_peek.iterrows():
            row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
            if any(k in row_str for k in ["employee id", "employee name", "associate id"]):
                header_idx = i
                break
        df = pd.read_excel(xls, sheet_name=sheet, header=header_idx)
        header_top = df_peek.iloc[header_idx - 1].tolist() if header_idx > 0 else None
    return df, header_top, filename

def read_uzio_raw_file(content):
    try:
        df = pd.read_excel(io.BytesIO(content), sheet_name='Employee Details', header=3)
        df.columns = [str(c).strip() for c in df.columns]
        norm_mapping = {norm_colname(k).casefold(): v for k, v in UZIO_RAW_MAPPING.items()}
        df.columns = [norm_mapping.get(norm_colname(c).casefold(), c) for c in df.columns]
        if 'Employee ID' in df.columns:
            df['Employee ID'] = df['Employee ID'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        return df
    except Exception as e:
        print(f"Error reading Uzio Raw File: {e}")
        return None

def is_hourly_only_job_title(jt_val: str) -> bool:
    jt = jt_val.strip().lower()
    return jt in HOURLY_ONLY_JOB_TITLES or jt.endswith("driver")
