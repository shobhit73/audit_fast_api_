import pandas as pd
import io
import re
from utils.audit_utils import norm_blank, try_parse_date, norm_id, normalize_space_and_case

def _pivot_uzio_long_to_wide(df_long):
    uz = df_long.copy()
    wide = uz.pivot_table(index="employee_id", columns="withholding_field_key", values="withholding_field_value", aggfunc="first").reset_index()
    return wide

def run_paycom_withholding_audit(uzio_content, paycom_content, mapping_content):
    """Production-grade Paycom withholding audit logic."""
    uzio_long = pd.read_csv(io.BytesIO(uzio_content), dtype=str).fillna("")
    uzio_wide = _pivot_uzio_long_to_wide(uzio_long)
    
    paycom = pd.read_csv(io.BytesIO(paycom_content), dtype=str).fillna("")
    mapping = pd.read_excel(io.BytesIO(mapping_content), dtype=str)
    
    p_id_col = next((c for c in paycom.columns if "Employee_Code" in c or "EE Code" in c), paycom.columns[0])
    paycom[p_id_col] = paycom[p_id_col].apply(norm_id)
    uzio_wide["employee_id"] = uzio_wide["employee_id"].apply(norm_id)
    
    merged = pd.merge(paycom, uzio_wide, left_on=p_id_col, right_on="employee_id", how="outer")
    
    rows = []
    for _, row in merged.iterrows():
        eid = row.get(p_id_col) or row.get("employee_id")
        if not eid: continue
        
        for _, m_row in mapping.iterrows():
            uz_key = m_row.get("Uzio Field Key")
            pc_col = m_row.get("PayCom Column")
            if not uz_key or not pc_col or uz_key not in merged.columns or pc_col not in merged.columns: continue
            
            u_v, p_v = row[uz_key], row[pc_col]
            match = (normalize_space_and_case(u_v) == normalize_space_and_case(p_v))
            rows.append({
                "Employee": eid, "Field": uz_key, "UZIO_Value": u_v, "PAYCOM_Value": p_v,
                "Status": "Data Match" if match else "Data Mismatch"
            })
            
    return {"Comparison_Detail": rows}
