import pandas as pd
import io
import re
from utils.audit_utils import clean_money_val, norm_id, normalize_space_and_case, smart_read_df

def run_paycom_deduction_audit(uzio_content, paycom_content, mapping):
    """Production-grade Paycom deduction audit logic."""
    df_uzio = smart_read_df(uzio_content, dtype=str)
    df_paycom = smart_read_df(paycom_content)
    
    p_id_col = next((c for c in df_paycom.columns if any(x in c.lower() for x in ["ee code", "employee code"])), "EE Code")
    p_desc_col = next((c for c in df_paycom.columns if "description" in c.lower()), "Description")
    p_amt_col = next((c for c in df_paycom.columns if "amount" in c.lower()), "Amount")
    
    u_id_col = next((c for c in df_uzio.columns if "employee id" in c.lower()), "Employee Id")
    u_ded_col = next((c for c in df_uzio.columns if "deduction name" in c.lower()), "Deduction Name")
    u_amt_col = next((c for c in df_uzio.columns if "amount" in c.lower()), "Amount")

    # Map and Group Paycom
    paycom_data = []
    for _, row in df_paycom.iterrows():
        eid = norm_id(row.get(p_id_col))
        desc = str(row.get(p_desc_col, "")).strip()
        uz_name = mapping.get(desc) or mapping.get(desc.lower())
        if not uz_name: continue
        paycom_data.append({"ID": eid, "Deduction": uz_name, "Amount": clean_money_val(row.get(p_amt_col))})
    df_p = pd.DataFrame(paycom_data).groupby(["ID", "Deduction"])["Amount"].sum().reset_index()

    # Group Uzio
    uzio_data = []
    for _, row in df_uzio.iterrows():
        eid = norm_id(row.get(u_id_col))
        uzio_data.append({"ID": eid, "Deduction": str(row.get(u_ded_col, "")).strip(), "Amount": clean_money_val(row.get(u_amt_col))})
    df_u = pd.DataFrame(uzio_data).groupby(["ID", "Deduction"])["Amount"].sum().reset_index()

    merged = pd.merge(df_p, df_u, on=["ID", "Deduction"], how="outer", suffixes=("_P", "_U")).fillna(0)
    
    results = []
    for _, row in merged.iterrows():
        diff = abs(row["Amount_P"] - row["Amount_U"])
        results.append({
            "Employee ID": row["ID"], "Deduction Name": row["Deduction"],
            "Paycom Amount": row["Amount_P"], "Uzio Amount": row["Amount_U"],
            "Status": "Data Match" if diff < 0.01 else "Data Mismatch"
        })
        
    return {"Audit_Details": results}
