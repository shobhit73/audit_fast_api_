import pandas as pd
import re
import numpy as np
import io
from datetime import datetime, date
from utils.audit_utils import (
    read_uzio_raw_file, norm_colname, norm_blank, try_parse_date, ensure_unique_columns, 
    safe_val, normalize_id, norm_ssn_canonical, is_hourly_only_job_title, detect_duplicate_ssns,
    get_identity_match_map, norm_id, normalize_paytype_text, paytype_bucket, 
    normalize_reason_text, is_termination_reason_field, is_employment_status_field, 
    status_contains_any, uzio_is_active, uzio_is_terminated, ALLOWED_TERM_REASONS
)

# --- Production Mappings ---
ADP_FIELD_MAP = {
    'Employee ID': 'Associate ID',
    'First Name': 'Legal First Name',
    'Last Name': 'Legal Last Name',
    'Middle Initial': 'Legal Middle Name',
    'Suffix': 'Generation Suffix Code',
    'Employment Status': 'Position Status',
    'Employment Type': 'Worker Category Description',
    'Hire Date': 'Hire/Rehire Date',
    'Original Hire Date': 'Hire Date',
    'Termination Date': 'Termination Date',
    'Termination Reason': 'Termination Reason Description',
    'Pay Type': 'Regular Pay Rate Description',
    'Annual Salary': 'Annual Salary',
    'Hourly Pay Rate': 'Regular Pay Rate Amount',
    'Working Hours': 'Standard Hours',
    'Job Title': 'Job Title Description',
    'Department': 'Department Description',
    'Work Email': 'Work Contact: Work Email',
    'Personal Email': 'Personal Contact: Personal Email',
    'SSN': 'Tax ID (SSN)',
    'DOB': 'Birth Date',
    'Gender': 'Gender / Sex (Self-ID)',
    'Tobacco User': 'Tobacco User',
    'FLSA Classification': 'FLSA Description',
    'Address Line 1': 'Primary Address: Address Line 1',
    'Address Line 2': 'Primary Address: Address Line 2',
    'City': 'Primary Address: City',
    'Zip': 'Primary Address: Zip / Postal Code',
    'State': 'Primary Address: State / Territory Code',
    'Mailing Address Line 1': 'Legal / Preferred Address: Address Line 1',
    'Mailing Address Line 2': 'Legal / Preferred Address: Address Line 2',
    'Mailing City': 'Legal / Preferred Address: City',
    'Mailing Zip': 'Legal / Preferred Address: Zip / Postal Code',
    'Mailing State': 'Legal / Preferred Address: State / Territory Code',
    'Reports To ID': 'Reports To Associate ID',
    'Protected Veteran Status': 'Protected Veteran Status',
    'EEO Job Category': 'EEOC Job Classification',
    'Ethnicity': 'Race Description',
    'SOC Code': 'SOC Code',
    'Work Location': 'Location Description'
}

NUMERIC_KEYWORDS = {"salary", "rate", "hours", "amount"}
DATE_KEYWORDS = {"date", "dob", "birth", "doh", "hire"}
SSN_KEYWORDS = {"ssn", "tax id"}
ZIP_KEYWORDS = {"zip", "zipcode", "postal"}
GENDER_KEYWORDS = {"gender"}
PHONE_KEYWORDS = {"phone"}
MIDDLE_INITIAL_KEYWORDS = {"middle initial"}
JOB_TITLE_KEYWORDS = {"job title", "position title"}
VETERAN_KEYWORDS = {"veteran"}
EMP_STATUS_TOKENS = {"active", "terminated", "retired"}

JOB_TITLE_MAPPINGS = {
    "admin": "administrator",
    "management": "manager",
    "dsp owner": "owner"
}

# --- Production Normalization Helpers ---

def norm_gender(x):
    s = str(norm_blank(x)).strip().casefold()
    if "female" in s or "woman" in s: return "female"
    if "male" in s or "man" in s: return "male"
    return s

def norm_middle_initial(x):
    s = str(norm_blank(x)).strip()
    m = re.search(r"[A-Za-z]", s)
    return m.group(0).casefold() if m else ""

def norm_zip_first5(x):
    s = re.sub(r"[^\d]", "", str(norm_blank(x)).strip())
    if not s: return ""
    return s.zfill(5)[:5]

def norm_ssn_9digits(x):
    return norm_ssn_canonical(x)

def norm_veteran_status(x):
    s = str(norm_blank(x)).strip().casefold()
    if not s: return ""
    if "decline to self-identify" in s or "decline to answer" in s: return "decline to answer"
    if "not a protected veteran" in s: return "not a protected veteran"
    if "protected veteran" in s and "not" not in s: return "protected veteran"
    return s

def norm_job_title(x):
    s = str(norm_blank(x)).strip().casefold()
    if not s: return ""
    return JOB_TITLE_MAPPINGS.get(s, s)

def norm_value(x, field_name: str):
    f = norm_colname(field_name).lower()
    x = norm_blank(x)
    if x == "": return ""

    if any(k in f for k in MIDDLE_INITIAL_KEYWORDS): return norm_middle_initial(x)
    if any(k in f for k in GENDER_KEYWORDS): return norm_gender(x)
    if any(k in f for k in VETERAN_KEYWORDS): return norm_veteran_status(x)
    if any(k in f for k in JOB_TITLE_KEYWORDS): return norm_job_title(x)
    if any(k in f for k in SSN_KEYWORDS): return norm_ssn_9digits(x)
    if any(k in f for k in ZIP_KEYWORDS): return norm_zip_first5(x)
    if any(k in f for k in DATE_KEYWORDS): return try_parse_date(x)
    
    if any(k in f for k in NUMERIC_KEYWORDS):
        try: return float(str(x).replace(",", "").replace("$", ""))
        except: return re.sub(r"\s+", " ", str(x).strip()).casefold()

    return re.sub(r"\s+", " ", str(x).strip()).casefold()

def cleanse_uzio_value_for_field(field_name: str, uz_val):
    if norm_blank(uz_val) == "": return uz_val
    s = str(uz_val).strip().casefold()
    if (s in EMP_STATUS_TOKENS) and ("status" not in norm_colname(field_name).lower()):
        return ""
    return uz_val

def is_pay_type_field(f): return "pay type" in norm_colname(f).lower()
def is_employment_type_field(f): return "employment type" in norm_colname(f).lower()
def is_annual_salary_field(f): return "annual salary" in norm_colname(f).lower()
def is_hourly_rate_field(f): return "hourly pay rate" in norm_colname(f).lower() or "hourly rate" in norm_colname(f).lower()

def normalize_paytype_for_compare(x):
    s = normalize_paytype_text(x)
    if s in {"salary", "salaried"}: return "salaried"
    if s in {"hourly", "hour"}: return "hourly"
    return s

def normalize_employment_type(x):
    s = str(norm_blank(x)).strip().lower()
    if s in {"full time", "fulltime", "full-time", "ft"}: return "full time"
    if s in {"part time", "parttime", "part-time", "pt"}: return "part time"
    if s in {"seasonal", "temporary", "temp"}: return "seasonal"
    return s

def deduplicate_adp(df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    col_map = {c: c.lower() for c in df.columns}
    status_col = next((c for c, l in col_map.items() if "position status" in l), None)
    term_date_col = next((c for c, l in col_map.items() if "termination date" in l), None)
    start_date_col = next((c for c, l in col_map.items() if "position start date" in l), None)
    loc_desc_col = next((c for c, l in col_map.items() if "work location description" in l), None)
    license_id_col = next((c for c, l in col_map.items() if "license/certification id" in l), None)
    
    if not status_col: return df.drop_duplicates(subset=[key_col], keep="first")
        
    def pick_best_idx(group):
        if len(group) <= 1: return group.index[0]
        
        def get_date_val(row, col):
            if not col or pd.isna(row[col]): return pd.Timestamp.min
            try: return pd.to_datetime(str(row[col]).strip())
            except: return pd.Timestamp.min

        group_work = group.copy()
        group_work['__norm_status'] = group_work[status_col].astype(str).str.lower().str.strip()
        group_work['__has_license'] = group_work[license_id_col].apply(lambda x: 1 if norm_blank(x) != "" else 0) if license_id_col else 0
        
        actives = group_work[group_work['__norm_status'] == 'active']
        terms = group_work[group_work['__norm_status'] == 'terminated']
        others = group_work[(group_work['__norm_status'] != 'active') & (group_work['__norm_status'] != 'terminated')]
        
        if not actives.empty:
            actives['__sort_date'] = actives.apply(lambda r: get_date_val(r, start_date_col), axis=1)
            if loc_desc_col:
                actives['__has_loc'] = actives[loc_desc_col].apply(lambda x: 1 if norm_blank(x) != "" else 0)
                return actives.sort_values(by=['__has_loc', '__has_license', '__sort_date'], ascending=[False, False, False]).index[0]
            return actives.sort_values(by=['__has_license', '__sort_date'], ascending=[False, False]).index[0]

        if not terms.empty:
            use_start = not term_date_col or (terms[term_date_col].apply(norm_blank) == "").any()
            terms['__sort_date'] = terms.apply(lambda r: get_date_val(r, start_date_col if use_start else term_date_col), axis=1)
            return terms.sort_values(by=['__has_license', '__sort_date'], ascending=[False, False]).index[0]

        if not others.empty:
            others['__sort_date'] = others.apply(lambda r: get_date_val(r, start_date_col), axis=1)
            return others.sort_values(by=['__has_license', '__sort_date'], ascending=[False, False]).index[0]

        return group_work.index[0]

    return df.loc[df.groupby(key_col, group_keys=False).apply(pick_best_idx)].copy()

# --- Main Audit Function ---

def run_adp_census_audit(uzio_content, adp_content):
    uzio = read_uzio_raw_file(uzio_content)
    if uzio.empty: return [{"Status": "Error", "Message": "Uzio file is empty or invalid."}]

    try: adp = pd.read_excel(io.BytesIO(adp_content), dtype=str)
    except: adp = pd.read_csv(io.BytesIO(adp_content), dtype=str)
    
    adp = ensure_unique_columns(adp)
    adp.columns = [norm_colname(c) for c in adp.columns]
    
    UZIO_KEY = 'Employee ID'
    ADP_KEY = norm_colname(ADP_FIELD_MAP.get('Employee ID', 'Associate ID'))
    
    uzio[UZIO_KEY] = uzio[UZIO_KEY].apply(norm_id)
    adp[ADP_KEY] = adp[ADP_KEY].apply(norm_id)
    
    adp = deduplicate_adp(adp, ADP_KEY)
    uzio = uzio.drop_duplicates(subset=[UZIO_KEY], keep="first").copy()
    
    uz_to_adp_id_map = get_identity_match_map(
        uzio, adp, uzio_id_col=UZIO_KEY, vendor_id_col=ADP_KEY,
        uzio_ssn_col='SSN', vendor_ssn_col=next((c for c in adp.columns if "Tax ID" in c or "SSN" in c), None)
    )
    
    uzio_keys = set(uzio[UZIO_KEY].dropna())
    adp_keys = set(adp[ADP_KEY].dropna())
    uzio_idx = uzio.set_index(UZIO_KEY, drop=False)
    adp_idx = adp.set_index(ADP_KEY, drop=False)
    
    mapped_fields = [f for f in ADP_FIELD_MAP.keys() if f != UZIO_KEY]
    uz_to_adp = {k: norm_colname(v) for k, v in ADP_FIELD_MAP.items()}
    
    rows = []
    flsa_rows = []
    dq_rows = []
    adp_keys_processed = set()

    for uz_id in sorted(uzio_keys):
        adp_id = uz_to_adp_id_map.get(uz_id, uz_id)
        adp_exists = adp_id in adp_idx.index
        if adp_exists: adp_keys_processed.add(adp_id)

        fname = str(norm_blank(uzio_idx.at[uz_id, 'First Name']) or "")
        lname = str(norm_blank(uzio_idx.at[uz_id, 'Last Name']) or "")
        emp_name = f"{fname} {lname}".strip()
        
        # Pay Bucket & FLSA Context
        uz_pay_raw = str(norm_blank(uzio_idx.at[uz_id, 'Pay Type']) or "")
        flsa_raw = str(norm_blank(uzio_idx.at[uz_id, 'FLSA Classification']) or "")
        emp_pay_bucket = paytype_bucket(normalize_paytype_text(uz_pay_raw))
        flsa_norm = normalize_paytype_text(flsa_raw)
        
        adp_pay_type = safe_val(adp_idx, adp_id, uz_to_adp.get('Pay Type', "")) if adp_exists else ""
        adp_flsa = safe_val(adp_idx, adp_id, uz_to_adp.get('FLSA Classification', "")) if adp_exists else ""
        adp_job = safe_val(adp_idx, adp_id, uz_to_adp.get('Job Title', "")) if adp_exists else ""

        # --- FLSA Issues Detection ---
        all_issues = []
        if emp_pay_bucket == "hourly" and "exempt" in flsa_norm and "non" not in flsa_norm:
            all_issues.append("Hourly employee classified as Exempt (Uzio Internal)")
        elif emp_pay_bucket == "salaried" and "non-exempt" in flsa_norm:
            all_issues.append("Salaried employee classified as Non-Exempt (Uzio Internal)")
        
        if adp_exists:
            uz_pt_canon = normalize_paytype_for_compare(uz_pay_raw)
            adp_pt_canon = normalize_paytype_for_compare(adp_pay_type)
            if uz_pt_canon != adp_pt_canon and adp_pt_canon != "":
                all_issues.append(f"Pay Type Mismatch (Uzio: {uz_pay_raw} vs ADP: {adp_pay_type})")
            
            uz_flsa_canon = normalize_paytype_text(flsa_raw)
            adp_flsa_canon = normalize_paytype_text(adp_flsa)
            if uz_flsa_canon != adp_flsa_canon and adp_flsa_canon != "":
                all_issues.append(f"FLSA Mismatch (Uzio: {flsa_raw} vs ADP: {adp_flsa})")

        if all_issues:
            flsa_rows.append({
                "Employee ID": uz_id, "Employee Name": emp_name,
                "Pay Type (Uzio)": uz_pay_raw, "Pay Type (ADP)": adp_pay_type,
                "FLSA Classification (Uzio)": flsa_raw, "FLSA Classification (ADP)": adp_flsa,
                "Job Title (Uzio)": safe_val(uzio_idx, uz_id, 'Job Title'), 
                "Job Title (ADP)": adp_job, "Issue": "; ".join(all_issues)
            })

        for field in mapped_fields:
            adp_col = uz_to_adp.get(field, "")
            uz_val_raw = safe_val(uzio_idx, uz_id, field)
            uz_val = cleanse_uzio_value_for_field(field, uz_val_raw)
            adp_val = safe_val(adp_idx, adp_id, adp_col) if adp_exists else ""
            
            uz_n = norm_value(uz_val, field)
            adp_n = norm_value(adp_val, field)
            
            if not adp_exists: status = "Employee ID Not Found in ADP"
            elif adp_col not in adp.columns: status = "Column Missing in ADP Sheet"
            elif field not in uzio.columns: status = "Column Missing in Uzio Sheet"
            else:
                if is_pay_type_field(field):
                    uz_pt = normalize_paytype_for_compare(uz_val)
                    adp_pt = normalize_paytype_for_compare(adp_val)
                    if (uz_pt == adp_pt) or (uz_pt == "" and adp_pt == ""): status = "Data Match"
                    elif uz_pt == "" and adp_pt != "": status = "Value missing in Uzio (ADP has value)"
                    elif uz_pt != "" and adp_pt == "": status = "Value missing in ADP (Uzio has value)"
                    else: status = "Data Mismatch"
                elif is_employment_status_field(field):
                    adp_is_term_or_ret = any(x in adp_n.lower() for x in ["term", "retir", "deceased", "layoff"])
                    if (uz_n == adp_n) or (uz_n == "" and adp_n == ""): status = "Data Match"
                    elif (uzio_is_active(uz_n) and "leave" in adp_n.lower()): status = "Data Match"
                    elif (uzio_is_terminated(uz_n) and adp_is_term_or_ret): status = "Data Match"
                    else:
                        if uzio_is_active(uz_n): status = "Active in Uzio"
                        elif uzio_is_terminated(uz_n): status = "Terminated in Uzio"
                        elif uz_n == "" and not adp_is_term_or_ret: status = "Active in ADP"
                        elif uz_n == "" and adp_is_term_or_ret: status = "Terminated in ADP"
                        else: status = "Data Mismatch"
                elif is_termination_reason_field(field):
                    uz_r = normalize_reason_text(uz_val)
                    adp_r = normalize_reason_text(adp_val)
                    if (uz_r == adp_r) or (uz_r == "" and adp_r == ""): status = "Data Match"
                    elif (uz_r == "other" and adp_r in ALLOWED_TERM_REASONS): status = "Data Match"
                    elif ("voluntary termination of employment" in uz_r and "voluntary" in adp_r): status = "Data Match"
                    elif ("involuntary termination of employment" in uz_r and ("involuntary" in adp_r or "layoff" in adp_r)): status = "Data Match"
                    else: status = "Data Mismatch"
                elif is_employment_type_field(field):
                    uz_et = normalize_employment_type(uz_val)
                    adp_et = normalize_employment_type(adp_val)
                    if (uz_et == adp_et) or (uz_et == "" and adp_et == ""): status = "Data Match"
                    elif uz_et == "" and adp_et != "": status = "Value missing in Uzio (ADP has value)"
                    elif uz_et != "" and adp_et == "": status = "Value missing in ADP (Uzio has value)"
                    else: status = "Data Mismatch"
                else:
                    if (uz_n == adp_n) or (uz_n == "" and adp_n == ""): status = "Data Match"
                    elif uz_n == "" and adp_n != "": status = "Value missing in Uzio (ADP has value)"
                    elif uz_n != "" and adp_n == "": status = "Value missing in ADP (Uzio has value)"
                    else: status = "Data Mismatch"
                    
                    if status in ["Value missing in Uzio (ADP has value)", "Data Mismatch"]:
                        if emp_pay_bucket == "hourly" and is_annual_salary_field(field): status = "Data Match"
                        elif emp_pay_bucket == "salaried" and is_hourly_rate_field(field): status = "Data Match"
            
            rows.append({
                "Employee ID": uz_id, "Employee Name": emp_name, "Field": field,
                "Uzio Value": uz_val_raw, "ADP Value": adp_val, "Status": status
            })

    # Data Quality & Missing in Uzio
    adp_status_col = next((c for c in adp.columns if any(x in c.lower() for x in ["position status", "employment status"])), None)
    adp_hire_col = next((c for c in adp.columns if "hire" in c.lower() and "date" in c.lower()), None)
    adp_fname_col = uz_to_adp.get('First Name', '')
    adp_lname_col = uz_to_adp.get('Last Name', '')

    active_missing_rows = []
    terminated_missing_rows = []
    
    for adp_id in adp_idx.index:
        # DQ: 00/00/0000 dates
        for col in adp.columns:
            val = adp_idx.at[adp_id, col]
            if pd.notna(val) and '00/00/0000' in str(val):
                dq_rows.append({
                    "Employee ID": adp_id, "Employee Name": f"{safe_val(adp_idx, adp_id, adp_fname_col)} {safe_val(adp_idx, adp_id, adp_lname_col)}".strip(),
                    "Column": col, "Invalid Value Found": str(val)
                })
        
        # Missing in Uzio
        if adp_id not in adp_keys_processed:
            status_val = safe_val(adp_idx, adp_id, adp_status_col)
            status_lower = status_val.lower()
            emp_name = f"{safe_val(adp_idx, adp_id, adp_fname_col)} {safe_val(adp_idx, adp_id, adp_lname_col)}".strip()
            hire_date = safe_val(adp_idx, adp_id, adp_hire_col)
            
            payload = {"Employee ID": adp_id, "Employee Name": emp_name, "Employment Status (ADP)": status_val, "Date of Hire (ADP)": hire_date}
            
            if any(x in status_lower for x in ["active", "leave"]):
                active_missing_rows.append(payload)
            elif any(x in status_lower for x in ["term", "retir", "inactive", "quit", "resign"]):
                terminated_missing_rows.append(payload)
            
            for field in mapped_fields:
                rows.append({
                    "Employee ID": adp_id, "Employee Name": emp_name, "Field": field,
                    "Uzio Value": "", "ADP Value": safe_val(adp_idx, adp_id, uz_to_adp.get(field, "")),
                    "Status": "Employee ID Not Found in Uzio"
                })

    # Anomalies
    df_rows = pd.DataFrame(rows)
    df_salaried_drivers = df_rows[
        (df_rows["Field"] == "Job Title") & 
        (df_rows["Status"] == "Data Match") & 
        (df_rows["ADP Value"].str.contains("driver|delivery", case=False, na=False))
    ].copy()
    
    # Filter for actually salaried ones
    salaried_ids = [r["Employee ID"] for r in rows if r["Field"] == "Pay Type" and "salary" in str(r["Uzio Value"]).lower()]
    df_salaried_drivers = df_salaried_drivers[df_salaried_drivers["Employee ID"].isin(salaried_ids)]

    hourly_rates = [r for r in rows if is_hourly_rate_field(r["Field"]) and r["Status"] != "Employee ID Not Found in ADP"]
    high_rates = []
    for r in hourly_rates:
        try:
            val = float(str(r["ADP Value"]).replace("$","").replace(",",""))
            if val > 60: high_rates.append({**r, "Rate": val})
        except: pass

    mismatches = [r for r in rows if r["Status"] != "Data Match"]
    
    return {
        "Summary_Metrics": [
            {"Metric": "Employees in Uzio", "Value": len(uzio_keys)},
            {"Metric": "Employees in ADP", "Value": len(adp_keys)},
            {"Metric": "Total Mismatches Found", "Value": len(mismatches)}
        ],
        "Mismatches_Only": mismatches,
        "Comparison_Detail_AllFields": rows,
        "FLSA_Compliance_Issues": flsa_rows,
        "Data_Quality_Issues": dq_rows,
        "Active_Missing_In_Uzio": active_missing_rows,
        "Terminated_Missing_In_Uzio": terminated_missing_rows,
        "Duplicate_SSN_Check": detect_duplicate_ssns(adp, ssn_col=next((c for c in adp.columns if "Tax ID" in c or "SSN" in c), ADP_KEY)).to_dict(orient="records"),
        "Salaried_Driver_Exceptions": df_salaried_drivers.to_dict(orient="records"),
        "High_Hourly_Rate_Anomalies": high_rates
    }
