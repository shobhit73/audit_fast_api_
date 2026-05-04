import pandas as pd
import io
import re
from datetime import datetime
from utils.audit_utils import (
    norm_blank, try_parse_date, normalize_space_and_case,
    as_float_or_none, get_identity_match_map, norm_id, safe_val,
    read_uzio_raw_file, ensure_unique_columns, norm_colname,
    is_hourly_only_job_title, norm_ssn_canonical, smart_read_df
)

# Hardcoded Mapping from production tool
PAYCOM_FIELD_MAP = {
    'Employee ID': 'Employee_Code',
    'First Name': 'Legal_Firstname',
    'Last Name': 'Legal_Lastname',
    'Middle Initial': 'Legal_Middle_Name',
    'Suffix': 'Legal_Employee_Suffix',
    'Employment Status': 'Employee_Status',
    'Employment Type': 'DOL_Status',
    'Hire Date': 'Most_Recent_Hire_Date',
    'Original Hire Date': 'Hire_Date',
    'Termination Date': 'Termination_Date',
    'Termination Reason': 'Termination_Reason',
    'Pay Type': 'Pay_Type',
    'Annual Salary': 'Annual_Salary',
    'Hourly Pay Rate': 'Rate_1',
    'Working Hours': 'Scheduled_Pay_Period_Hours',
    'Job Title': 'Position',
    'Department': 'Department_Desc',
    'Work Email': 'Work_Email',
    'Personal Email': 'Personal_Email',
    'Phone Number': 'Primary_Phone',
    'SSN': 'SS_Number',
    'DOB': 'Birth_Date_(MM/DD/YYYY)',
    'Gender': 'Gender',
    'Tobacco User': 'Tobacco_User',
    'FLSA Classification': 'Exempt_Status',
    'Address Line 1': 'Primary_Address_Line_1',
    'Address Line 2': 'Primary_Address_Line_2',
    'City': 'Primary_City/Municipality',
    'Zip': 'Primary_Zip/Postal_Code',
    'State': 'Primary_State/Province',
    'Mailing Address Line 1': 'Mailing_Address_Line_1',
    'Mailing Address Line 2': 'Mailing_Address_Line_2',
    'Mailing City': 'Mailing_City/Municipality',
    'Mailing Zip': 'Mailing_Zip/Postal_Code',
    'Mailing State': 'Mailing_State/Province',
    'License Number': 'DriversLicense',
    'License Expiration Date': 'DLExpirationDate',
    'Work Location': 'Work_Location',
    'Reports To ID': 'Supervisor_Primary_Code',
    'Ethnicity': 'EEO1_Ethnicity',
    'SOC Code': 'SOC_Code',
    'EEO Job Category': 'EEO1_Category'
}

def normalize_employment_type(x):
    s = normalize_space_and_case(x).replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if s in {"full time", "fulltime", "ft"}: return "full time"
    if s in {"part time", "parttime", "pt"}: return "part time"
    if s in {"seasonal", "temporary", "temp"}: return "seasonal"
    return s

def normalize_suffix(x):
    return re.sub(r"[^a-z0-9]", "", normalize_space_and_case(x))

def normalize_phone(x):
    s = str(norm_blank(x)).strip()
    if s.endswith(".0"): s = s[:-2]
    digits = re.sub(r"[^0-9]", "", s)
    if len(digits) == 11 and digits.startswith("1"): digits = digits[1:]
    return digits

def first_alpha_char(x):
    txt = str(norm_blank(x)).strip()
    m = re.search(r"[A-Za-z]", txt)
    return m.group(0).casefold() if m else ""

def canonical_pay_type(x):
    s = normalize_space_and_case(x)
    if "hour" in s: return "hourly"
    if "salar" in s: return "salaried"
    return s

def canonical_employment_status(x):
    s = normalize_space_and_case(x)
    if "on leave" in s or s in {"active", "activated"}: return "active"
    return s

def termination_reason_equal(uzio_val, paycom_val):
    uz, pc = normalize_space_and_case(uzio_val), normalize_space_and_case(paycom_val)
    if uz == "" and pc == "": return True
    if uz == "other": return True
    if ("involuntary" in uz) or ("involuntary" in pc): return ("involuntary" in uz) and ("involuntary" in pc)
    if ("voluntary" in uz) or ("voluntary" in pc): return ("voluntary" in uz) and ("voluntary" in pc)
    return uz == pc

def should_ignore_field_for_paytype(field_name: str, pay_type_canon: str) -> bool:
    f, pt = norm_colname(field_name).casefold(), (pay_type_canon or "").casefold()
    if pt == "hourly" and "annual salary" in f: return True
    if pt == "salaried" and (("hourly" in f and "rate" in f) or ("hours per week" in f) or ("working hours" in f)): return True
    return False

def normalized_compare(field_name: str, uzio_val, paycom_val) -> bool:
    f = norm_colname(field_name).casefold()
    if "termination reason" in f: return termination_reason_equal(uzio_val, paycom_val)
    if "employment status" in f: return canonical_employment_status(uzio_val) == canonical_employment_status(paycom_val)
    if "pay type" in f: return canonical_pay_type(uzio_val) == canonical_pay_type(paycom_val)
    if "employment type" in f: return normalize_employment_type(uzio_val) == normalize_employment_type(paycom_val)
    if ("middle" in f and "initial" in f): return first_alpha_char(uzio_val) == first_alpha_char(paycom_val)
    if "suffix" in f: return normalize_suffix(uzio_val) == normalize_suffix(paycom_val)
    if "ssn" in f: return re.sub(r"\D", "", str(uzio_val)).lstrip("0") == re.sub(r"\D", "", str(paycom_val)).lstrip("0")
    if "phone" in f: return normalize_phone(uzio_val).lstrip("0") == normalize_phone(paycom_val).lstrip("0")
    if "zip" in f: return re.sub(r"\D", "", str(uzio_val)).lstrip("0") == re.sub(r"\D", "", str(paycom_val)).lstrip("0")
    if any(k in f for k in ["date", "dob", "birth", "effective", "doh", "hire", "termination"]):
        return try_parse_date(uzio_val) == try_parse_date(paycom_val)
    if any(k in f for k in ["salary", "rate", "hours", "amount", "percent", "percentage", "digits"]):
        fa, fb = as_float_or_none(uzio_val), as_float_or_none(paycom_val)
        if fa is not None and fb is not None: return abs(fa - fb) <= 1e-9
    if "license" in f: return str(uzio_val).strip().lstrip("0") == str(paycom_val).strip().lstrip("0")
    return normalize_space_and_case(uzio_val) == normalize_space_and_case(paycom_val)

def run_paycom_census_audit(uzio_content, paycom_content):
    """Full production-grade Paycom census audit logic with 10-sheet output."""
    uzio = read_uzio_raw_file(uzio_content)
    paycom = smart_read_df(paycom_content, dtype=str)
    
    uzio = ensure_unique_columns(uzio)
    uzio.columns = [norm_colname(c) for c in uzio.columns]
    paycom = ensure_unique_columns(paycom)
    paycom.columns = [norm_colname(c) for c in paycom.columns]

    UZIO_KEY, PAYCOM_KEY = 'Employee ID', norm_colname(PAYCOM_FIELD_MAP['Employee ID'])
    uzio[UZIO_KEY] = uzio[UZIO_KEY].apply(norm_id)
    paycom[PAYCOM_KEY] = paycom[PAYCOM_KEY].apply(norm_id)
    
    # SSN Column discovery
    uzio_ssn_col = 'SSN'
    paycom_ssn_col = next((c for c in paycom.columns if any(k in c for k in ["Tax ID", "SSN"])), None)
    
    uz_to_pc_id_map = get_identity_match_map(uzio, paycom, UZIO_KEY, PAYCOM_KEY, uzio_ssn_col, paycom_ssn_col)
    
    # Data Quality / Duplicate SSN
    dupe_ssn_rows = []
    if uzio_ssn_col in uzio.columns:
        uz_dupes = uzio[uzio[uzio_ssn_col].apply(norm_ssn_canonical).duplicated(keep=False) & (uzio[uzio_ssn_col].apply(norm_ssn_canonical) != "")]
        for ssn, grp in uz_dupes.groupby(uzio[uzio_ssn_col].apply(norm_ssn_canonical)):
            dupe_ssn_rows.append({"Source": "Uzio", "SSN": ssn, "Employee IDs": ", ".join(grp[UZIO_KEY].astype(str)), "Issue": "Duplicate SSN in Uzio"})
    if paycom_ssn_col:
        pc_dupes = paycom[paycom[paycom_ssn_col].apply(norm_ssn_canonical).duplicated(keep=False) & (paycom[paycom_ssn_col].apply(norm_ssn_canonical) != "")]
        for ssn, grp in pc_dupes.groupby(paycom[paycom_ssn_col].apply(norm_ssn_canonical)):
            dupe_ssn_rows.append({"Source": "Paycom", "SSN": ssn, "Employee IDs": ", ".join(grp[PAYCOM_KEY].astype(str)), "Issue": "Duplicate SSN in Paycom"})

    # Main comparison
    rows, processed_pc = [], set()
    mapped_fields = [f for f in PAYCOM_FIELD_MAP if f != 'Employee ID']
    uzio_idx_map = {eid: i for i, eid in uzio[UZIO_KEY].items() if eid}
    paycom_idx_map = {eid: i for i, eid in paycom[PAYCOM_KEY].items() if eid}

    for uz_id, u_i in uzio_idx_map.items():
        pc_id = uz_to_pc_id_map.get(uz_id, uz_id)
        p_i = paycom_idx_map.get(pc_id)
        if p_i is not None: processed_pc.add(pc_id)
        
        emp_name = f"{safe_val(uzio, u_i, 'First Name')} {safe_val(uzio, u_i, 'Last Name')}".strip()
        emp_status = safe_val(uzio, u_i, 'Employment Status')
        emp_pay_type = canonical_pay_type(safe_val(uzio, u_i, 'Pay Type'))

        for field in mapped_fields:
            pc_col = norm_colname(PAYCOM_FIELD_MAP[field])
            uz_val = safe_val(uzio, u_i, field)
            pc_val = safe_val(paycom, p_i, pc_col) if p_i is not None else ""
            
            if p_i is None: status = "Employee ID Not Found in Paycom"
            elif pc_col not in paycom.columns: status = "Column Missing in Paycom Sheet"
            elif should_ignore_field_for_paytype(field, emp_pay_type): status = "Data Match"
            else:
                same = normalized_compare(field, uz_val, pc_val)
                if same: status = "Data Match"
                else:
                    uz_b, pc_b = norm_blank(uz_val), norm_blank(pc_val)
                    if "employment status" in field.lower() and pc_b != "":
                        uz_s, pc_s = canonical_employment_status(uz_b), canonical_employment_status(pc_b)
                        if "active" in uz_s: status = "Active in Uzio"
                        elif "term" in uz_s: status = "Terminated in Uzio"
                        else: status = "Data Mismatch"
                    elif not uz_b and pc_b: status = "Value missing in Uzio (Paycom has value)"
                    elif uz_b and not pc_b: status = "Value missing in Paycom (Uzio has value)"
                    else: status = "Data Mismatch"
            
            rows.append({
                "Employee ID": uz_id, "Employee Name": emp_name, "Field": field,
                "Employment Status": emp_status, "UZIO_Value": uz_val, "PAYCOM_Value": pc_val,
                "PAYCOM_SourceOfTruth_Status": status
            })

    # Paycom-only records
    remaining_pc = set(paycom_idx_map.keys()) - processed_pc
    for pc_id in remaining_pc:
        p_i = paycom_idx_map[pc_id]
        emp_name = f"{safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP['First Name']))} {safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP['Last Name']))}".strip()
        for field in mapped_fields:
            rows.append({
                "Employee ID": pc_id, "Employee Name": emp_name, "Field": field,
                "Employment Status": "Not in Uzio", "UZIO_Value": "", 
                "PAYCOM_Value": safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP[field])),
                "PAYCOM_SourceOfTruth_Status": "Employee ID Not Found in Uzio"
            })

    df_detail = pd.DataFrame(rows)
    
    # FLSA, Salaried Drivers, DQ, High Hourly, Hourly Zero Hours
    flsa_issues, dq_issues, active_missing, term_missing, salaried_drivers, high_rate, hourly_zero_hours = [], [], [], [], [], [], []
    
    # Logic for specialized sheets
    for uz_id, u_i in uzio_idx_map.items():
        # FLSA
        pt = canonical_pay_type(safe_val(uzio, u_i, 'Pay Type'))
        flsa = normalize_space_and_case(safe_val(uzio, u_i, 'FLSA Classification'))
        if (pt == "hourly" and "exempt" in flsa and "non" not in flsa) or (pt == "salaried" and "non" in flsa):
            flsa_issues.append({"Employee ID": uz_id, "Issue": f"Inconsistent Uzio FLSA ({flsa}) for Pay Type ({pt})"})
            
        # Hourly = 0 Hours validation (Check Uzio only)
        if pt == "hourly":
            wh_raw = safe_val(uzio, u_i, 'Working Hours')
            try:
                wh_val = float(str(wh_raw).replace(",", "").strip()) if str(wh_raw).strip() else 0.0
            except Exception:
                wh_val = 0.0
            
            if wh_val > 0:
                emp_name = f"{safe_val(uzio, u_i, 'First Name')} {safe_val(uzio, u_i, 'Last Name')}".strip()
                hourly_zero_hours.append({
                    "Employee ID": uz_id,
                    "Employee Name": emp_name,
                    "Pay Type (Uzio)": safe_val(uzio, u_i, 'Pay Type'),
                    "Working Hours (Uzio)": wh_raw,
                    "Issue": f"Hourly employee has {wh_raw} working hours. Must be 0."
                })
            
    for pc_id, p_i in paycom_idx_map.items():
        # DQ
        for col in paycom.columns:
            if '00/00/0000' in str(safe_val(paycom, p_i, col)):
                dq_issues.append({"Employee ID": pc_id, "Column": col, "Invalid Value Found": "00/00/0000"})
        # Salaried Drivers
        jt = str(safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP['Job Title']))).lower()
        pt = canonical_pay_type(safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP['Pay Type'])))
        if pt == "salaried" and is_hourly_only_job_title(jt):
            salaried_drivers.append({"Employee ID": pc_id, "Job Title": jt, "Comment": "Salaried driver role (conflict)"})
        # High Hourly
        if is_hourly_only_job_title(jt):
            rate = as_float_or_none(safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP['Hourly Pay Rate'])))
            if rate and rate > 100:
                high_rate.append({"Employee ID": pc_id, "Rate": rate, "Comment": "Exceeds $100/hr threshold"})

    # Missing in Uzio splits
    for pc_id in remaining_pc:
        p_i = paycom_idx_map[pc_id]
        st = canonical_employment_status(safe_val(paycom, p_i, norm_colname(PAYCOM_FIELD_MAP['Employment Status'])))
        rec = {"Employee ID": pc_id, "Status": st}
        if st == "active": active_missing.append(rec)
        else: term_missing.append(rec)

    # Summary
    uzio_emps_set = set(uzio[UZIO_KEY].dropna().map(str))
    paycom_emps_set = set(paycom[PAYCOM_KEY].dropna().map(str))

    metrics = [
        {"Metric": "Total UZIO Employees", "Value": len(uzio_emps_set)},
        {"Metric": "Total PAYCOM Employees", "Value": len(paycom_emps_set)},
        {"Metric": "Employees in both", "Value": len(uzio_emps_set & paycom_emps_set)},
        {"Metric": "Employees only in UZIO", "Value": len(uzio_emps_set - paycom_emps_set)},
        {"Metric": "Employees only in PAYCOM", "Value": len(paycom_emps_set - uzio_emps_set)},
        {"Metric": "Total UZIO Records", "Value": int(len(uzio))},
        {"Metric": "Total PAYCOM Records", "Value": int(len(paycom))},
        {"Metric": "Fields Compared", "Value": int(len(mapped_fields))},
        {"Metric": "Total Comparisons (field-level rows)", "Value": int(len(rows))},
        {"Metric": "FLSA Compliance Issues", "Value": len(flsa_issues)},
        {"Metric": "Active in Paycom but Missing in Uzio", "Value": len(active_missing)},
        {"Metric": "Terminated in Paycom but Missing in Uzio", "Value": len(term_missing)},
        {"Metric": "Data Quality Issues (00/00/0000)", "Value": len(dq_issues)},
        {"Metric": "Duplicate SSN Warnings", "Value": len(dupe_ssn_rows)},
        {"Metric": "Salaried Hourly-Only Exceptions", "Value": len(salaried_drivers)},
        {"Metric": "High Hourly Rate Anomalies (>$100/hr)", "Value": len(high_rate)},
    ]

    return {
        "Summary_Metrics": metrics,
        "Mismatches_Only": df_detail[df_detail["PAYCOM_SourceOfTruth_Status"] != "Data Match"].to_dict(orient="records"),
        "Comparison_Detail_AllFields": rows,
        "FLSA_Compliance_Issues": flsa_issues,
        "Data_Quality_Issues": dq_issues,
        "Active_Missing_In_Uzio": active_missing,
        "Terminated_Missing_In_Uzio": term_missing,
        "Duplicate_SSN_Check": dupe_ssn_rows,
        "Salaried_Driver_Exceptions": salaried_drivers,
        "High_Hourly_Rate_Anomalies": high_rate,
        "Hourly_Zero_Hours_Exceptions": hourly_zero_hours
    }
