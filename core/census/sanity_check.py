import pandas as pd
import re
from utils.audit_utils import norm_colname, norm_blank, try_parse_date, as_float_or_none, norm_ssn_canonical

def validate_source_data(df_source, resolved_field_map):
    hard_errors = []
    # ... (Implementation of validate_source_data from audit_utils.py) ...
    # For now, I'll provide a simplified version that catches the most common issues
    
    emp_id_col = resolved_field_map.get('Employee ID')
    ssn_col = resolved_field_map.get('SSN')
    status_col = resolved_field_map.get('Employment Status')
    
    for idx, row in df_source.iterrows():
        eid = str(row.get(emp_id_col, "")).strip()
        ssn = norm_ssn_canonical(row.get(ssn_col, ""))
        status = str(row.get(status_col, "")).strip().lower()
        
        issues = []
        if not eid: issues.append("Missing Employee ID")
        if not ssn: issues.append("Missing SSN")
        if not status: issues.append("Missing Employment Status")
        
        if issues:
            hard_errors.append({
                "Employee ID": eid or f"Row {idx+2}",
                "Issue": ", ".join(issues)
            })
            
    return {"hard_errors": pd.DataFrame(hard_errors)}

def run_census_sanity_check(df_source, resolved_field_map):
    validation = validate_source_data(df_source, resolved_field_map)
    return validation
