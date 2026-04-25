import pandas as pd
import re
import numpy as np
from utils.audit_utils import (
    read_uzio_raw_file, norm_colname, norm_blank, try_parse_date, ensure_unique_columns, 
    safe_val, normalize_id, norm_ssn_canonical, is_hourly_only_job_title
)

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

def norm_value(x, field_name: str):
    f = norm_colname(field_name).lower()
    x = norm_blank(x)
    if x == "": return ""
    if "middle initial" in f:
        s = str(x).strip()
        m = re.search(r"[A-Za-z]", s)
        return m.group(0).casefold() if m else ""
    if "gender" in f:
        s = str(x).lower()
        if "female" in s: return "female"
        if "male" in s: return "male"
        return s
    if "date" in f or "dob" in f or "birth" in f: return try_parse_date(x)
    if any(k in f for k in ["salary", "rate", "hours", "amount"]):
        try: return float(str(x).replace(",", "").replace("$", ""))
        except: return str(x).lower()
    return str(x).lower().strip()

def run_adp_census_audit(uzio_content, adp_content):
    uzio = read_uzio_raw_file(uzio_content)
    adp = pd.read_excel(pd.io.common.BytesIO(adp_content), dtype=str)
    adp = ensure_unique_columns(adp)
    adp.columns = [norm_colname(c) for c in adp.columns]
    
    UZIO_KEY = 'Employee ID'
    ADP_KEY = norm_colname(ADP_FIELD_MAP.get('Employee ID', 'Associate ID'))
    
    uzio[UZIO_KEY] = uzio[UZIO_KEY].apply(normalize_id)
    adp[ADP_KEY] = adp[ADP_KEY].apply(normalize_id)
    
    uzio_idx = uzio.set_index(UZIO_KEY, drop=False)
    adp_idx = adp.set_index(ADP_KEY, drop=False)
    
    mapped_fields = [f for f in ADP_FIELD_MAP.keys() if f != UZIO_KEY]
    uz_to_adp = {k: norm_colname(v) for k, v in ADP_FIELD_MAP.items()}
    
    rows = []
    all_keys = sorted(set(uzio[UZIO_KEY]).union(set(adp[ADP_KEY])))
    
    for eid in all_keys:
        if not eid: continue
        uz_exists = eid in uzio_idx.index
        adp_exists = eid in adp_idx.index
        
        for field in mapped_fields:
            adp_col = uz_to_adp.get(field, "")
            uz_val = safe_val(uzio_idx, eid, field) if uz_exists and field in uzio_idx.columns else ""
            adp_val = safe_val(adp_idx, eid, adp_col) if adp_exists and adp_col in adp_idx.columns else ""
            
            uz_n = norm_value(uz_val, field)
            adp_n = norm_value(adp_val, field)
            
            if not adp_exists: status = "Employee ID Not Found in ADP"
            elif not uz_exists: status = "Employee ID Not Found in Uzio"
            elif uz_n == adp_n: status = "Data Match"
            else: status = "Data Mismatch"
            
            if status != "Data Match":
                rows.append({
                    "Employee ID": eid,
                    "Field": field,
                    "Uzio Value": uz_val,
                    "ADP Value": adp_val,
                    "Status": status
                })
                
    return rows
