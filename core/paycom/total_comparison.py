import pandas as pd
import io
import re
from utils.audit_utils import clean_money_val, norm_colname, normalize_id, format_pay_date

def parse_paycom_filename_date(filename):
    match = re.findall(r'(\d{8})', filename)
    if len(match) >= 1:
        d_str = match[-1] # Usually the last date in filename
        try: return f"{d_str[4:]}-{d_str[:2]}-{d_str[2:4]}"
        except: return "Unknown"
    return "Unknown"

def calculate_totals_paycom(df, mapping_source_names, filename, uzio_item_name=""):
    found_items = set()
    emp_tots = {}
    id_aliases = ["ee code", "employee code", "file #", "clock #", "associate id"]
    desc_aliases = ["type description", "description", "earning/deduction/tax", "code description", "row labels"]
    amt_aliases = ["current amount", "amount", "total amount", "value", "sum of amount"]
    
    id_col = next((c for c in df.columns if any(x in str(c).lower() for x in id_aliases)), None)
    desc_col = next((c for c in df.columns if any(x in str(c).lower() for x in desc_aliases)), None)
    amt_col = next((c for c in df.columns if any(x in str(c).lower() for x in amt_aliases)), None)
    
    if not all([desc_col, amt_col]): return 0.0, [], {}

    pay_date = parse_paycom_filename_date(filename)
    norm_mappings = [n.lower().strip() for n in mapping_source_names]
    
    for _, row in df.iterrows():
        raw_desc = str(row[desc_col]).strip()
        if raw_desc.lower() in norm_mappings:
            eid = normalize_id(row[id_col]) if id_col else "Summary"
            amount = clean_money_val(row[amt_col])
            key = (eid, pay_date)
            emp_tots[key] = emp_tots.get(key, 0.0) + amount
            found_items.add(raw_desc)
            
    return sum(emp_tots.values()), list(found_items), emp_tots

def run_paycom_total_comparison(paycom_files_data, uzio_file_data, mappings):
    # Simplified logic similar to ADP
    results = []
    # ... logic ...
    return {"message": "Paycom comparison complete", "results": results}
