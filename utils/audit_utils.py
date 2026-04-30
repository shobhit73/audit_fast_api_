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

def smart_read_df(content, filename="", sheet_name=None, header='infer', required_columns=None, fallback_columns=None, **kwargs):
    """
    Robustly reads Excel or CSV from bytes. 
    If sheet_name is provided and it's an Excel file, it reads that sheet.
    If required_columns is provided, it scans for the header row.
    Falls back to CSV if Excel reading fails or if the extension is .csv.
    """
    import io
    file_io = io.BytesIO(content)
    is_csv = str(filename).lower().endswith('.csv')
    
    if not is_csv:
        # Try Excel
        try:
            xls = pd.ExcelFile(file_io)
            # Use provided sheet or search all sheets
            sheets = [sheet_name] if sheet_name and sheet_name in xls.sheet_names else xls.sheet_names
            
            for sheet in sheets:
                if required_columns:
                    df_peek = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=50)
                    header_row_idx = None
                    for idx, row in df_peek.iterrows():
                        row_vals = [str(v).strip().lower() for v in row.values if pd.notna(v)]
                        if all(any(col.lower() in v for v in row_vals) for col in required_columns):
                            header_row_idx = idx
                            break
                    
                    if header_row_idx is not None:
                        return pd.read_excel(xls, sheet_name=sheet, header=header_row_idx, **kwargs)
                    
                    # Try fallback columns
                    if fallback_columns:
                        for idx, row in df_peek.iterrows():
                            row_vals = [str(v).strip().lower() for v in row.values if pd.notna(v)]
                            if all(any(col.lower() in v for v in row_vals) for col in fallback_columns):
                                return pd.read_excel(xls, sheet_name=sheet, header=idx, **kwargs)
                else:
                    return pd.read_excel(xls, sheet_name=sheet, header=header, **kwargs)
        except Exception:
            pass

    # Try CSV
    try:
        file_io.seek(0)
        if required_columns:
            # We need to peek to find the header
            wrapper = io.TextIOWrapper(file_io, encoding='utf-8', errors='replace')
            header_row_idx = None
            for i, line in enumerate(wrapper):
                line_lower = line.lower()
                if all(col.lower() in line_lower for col in required_columns):
                    header_row_idx = i
                    break
                if i > 100: break
            
            if header_row_idx is not None:
                file_io.seek(0)
                return pd.read_csv(file_io, header=header_row_idx, **kwargs)
            
            if fallback_columns:
                file_io.seek(0)
                wrapper = io.TextIOWrapper(file_io, encoding='utf-8', errors='replace')
                for i, line in enumerate(wrapper):
                    line_lower = line.lower()
                    if all(col.lower() in line_lower for col in fallback_columns):
                        header_row_idx = i
                        break
                    if i > 100: break
                if header_row_idx is not None:
                    file_io.seek(0)
                    return pd.read_csv(file_io, header=header_row_idx, **kwargs)

        file_io.seek(0)
        return pd.read_csv(file_io, header=header, **kwargs)
    except Exception:
        return pd.DataFrame()


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
    """Reads Uzio raw census file, supporting both Excel (with sheet logic) and CSV."""
    try:
        # Try Excel with specific sheet and header row
        df = smart_read_df(content, sheet_name='Employee Details', header=3)
        if df.empty:
            # Try CSV fallback (no sheet name)
            df = smart_read_df(content, header=3)
            
        if df.empty:
            # Try without header skip if still empty
            df = smart_read_df(content)

        df.columns = [str(c).strip() for c in df.columns]
        norm_mapping = {norm_colname(k).casefold(): v for k, v in UZIO_RAW_MAPPING.items()}
        df.columns = [norm_mapping.get(norm_colname(c).casefold(), c) for c in df.columns]
        
        if 'Employee ID' in df.columns:
            df['Employee ID'] = df['Employee ID'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
        return df
    except Exception as e:
        import sys
        sys.stderr.write(f"Error reading Uzio Raw File: {e}\n")
        return pd.DataFrame()

def is_hourly_only_job_title(jt_val: str) -> bool:
    jt = jt_val.strip().lower()
    return jt in HOURLY_ONLY_JOB_TITLES or jt.endswith("driver")

def get_identity_match_map(df_uzio, df_vendor, uzio_id_col, vendor_id_col, uzio_ssn_col, vendor_ssn_col):
    """Maps Uzio employee IDs to vendor IDs via SSN matching."""
    uz_ssn_map = {norm_ssn_canonical(row[uzio_ssn_col]): str(row[uzio_id_col]).strip()
                  for _, row in df_uzio.iterrows() if norm_ssn_canonical(row.get(uzio_ssn_col, ""))}
    result = {}
    for _, row in df_vendor.iterrows():
        ssn = norm_ssn_canonical(row.get(vendor_ssn_col, ""))
        vid = str(row[vendor_id_col]).strip()
        if ssn and ssn in uz_ssn_map:
            result[uz_ssn_map[ssn]] = vid
    return result

def norm_id(x):
    if pd.isna(x): return ""
    s = str(x).strip()
    if s.endswith(".0"): s = s[:-2]
    return s.lstrip("0")

def normalize_space_and_case(x):
    if not x: return ""
    return re.sub(r"\s+", " ", str(x).strip()).lower()

def as_float_or_none(x):
    try: return float(str(x).replace(",", "").replace("$", "").strip())
    except: return None

def detect_duplicate_ssns(df, ssn_col):
    """Identifies duplicate SSNs in a dataframe."""
    df = df.copy()
    df['__norm_ssn'] = df[ssn_col].apply(norm_ssn_canonical)
    dupes = df[df['__norm_ssn'] != ""][df.duplicated(subset=['__norm_ssn'], keep=False)]
    return dupes

def normalize_paytype_text(x):
    return str(norm_blank(x)).strip().lower()

def paytype_bucket(pt_norm: str) -> str:
    if "salary" in pt_norm or "salaried" in pt_norm: return "salaried"
    if "hourly" in pt_norm or "hour" in pt_norm: return "hourly"
    return "other"

def normalize_reason_text(x):
    s = str(norm_blank(x)).strip().replace("\u00A0", " ")
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')
    s = re.sub(r"\s+", " ", s).strip()
    return s.strip('"').strip("'").lower()

def is_termination_reason_field(f: str) -> bool:
    return "termination reason" in norm_colname(f).lower()

def is_employment_status_field(f: str) -> bool:
    return any(k in norm_colname(f).lower() for k in ["employment status", "position status", "worker status"])

def status_contains_any(s: str, needles: list) -> bool:
    s = str(s).lower()
    return any(n in s for n in needles)

def uzio_is_active(s: str) -> bool:
    return "active" in str(s).lower()

def uzio_is_terminated(s: str) -> bool:
    return any(x in str(s).lower() for x in ["terminated", "inactive", "quit", "resign"])

ALLOWED_TERM_REASONS = {
    "quit without notice", "no reason given", "misconduct", "abandoned job",
    "advancement (better job with higher pay)", "no-show (never started employment)",
    "performance", "personal", "scheduling conflicts (schedules don't work)",
    "attendance", "reduction in force", "reorganization", "mutual agreement",
    "import created action", "advancement", "no-show", "management", "layoff"
}
