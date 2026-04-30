import pandas as pd
import io
import re
from utils.audit_utils import clean_money_val, norm_id, smart_read_df

def run_paycom_deduction_audit(uzio_content, paycom_content, mapping):
    """Production-grade Paycom deduction audit logic with Smart Header Search."""
    # Load Uzio with smart search
    df_uzio = smart_read_df(uzio_content, required_columns=["employee id", "deduction name"], fallback_columns=["employee id"], dtype=str)
    # Load Paycom with smart search
    df_paycom = smart_read_df(paycom_content, required_columns=["code", "amount"], dtype=str)
    
    if df_uzio.empty:
        return {"error": "Could not find valid Uzio data (missing 'Employee Id' or 'Deduction Name')."}
    if df_paycom.empty:
        return {"error": "Could not find valid Paycom data (missing 'Code' or 'Amount')."}

    # Flexible column detection
    p_id_col = next((c for c in df_paycom.columns if any(x in c.lower() for x in ["ee code", "employee code", "employee id", "associate id"])), df_paycom.columns[0])
    p_code_col = next((c for c in df_paycom.columns if "deduction code" in c.lower()), next((c for c in df_paycom.columns if "code" in c.lower() and "employee" not in c.lower()), "Code"))
    p_desc_col = next((c for c in df_paycom.columns if "deduction desc" in c.lower()), next((c for c in df_paycom.columns if "description" in c.lower()), "Description"))
    p_amt_col = next((c for c in df_paycom.columns if "amount" in c.lower() and "exempt" not in c.lower()), "Amount")
    
    u_id_col = next((c for c in df_uzio.columns if "employee id" in c.lower()), "Employee Id")
    u_ded_col = next((c for c in df_uzio.columns if "deduction name" in c.lower()), "Deduction Name")
    u_amt_col = next((c for c in df_uzio.columns if "employee amount" in c.lower()), next((c for c in df_uzio.columns if "amount" in c.lower()), "Amount"))

    # Map and Group Paycom
    # Support mapping by Description OR Code
    norm_mapping = {str(k).lower(): v for k, v in mapping.items()}
    
    paycom_data = []
    for _, row in df_paycom.iterrows():
        eid = norm_id(row.get(p_id_col))
        if not eid: continue
        
        raw_code = str(row.get(p_code_col, "")).strip()
        raw_desc = str(row.get(p_desc_col, "")).strip()
        
        # Try Description then Code for mapping
        uz_name = mapping.get(raw_desc) or mapping.get(raw_code) or norm_mapping.get(raw_desc.lower()) or norm_mapping.get(raw_code.lower())
        
        if not uz_name: continue
        
        paycom_data.append({
            "ID": eid, 
            "Deduction": uz_name, 
            "Amount": clean_money_val(row.get(p_amt_col)),
            "Raw_Code": raw_code
        })
        
    if not paycom_data:
        return {"error": "No deductions found after mapping. Please check your mapping JSON."}
        
    df_p = pd.DataFrame(paycom_data).groupby(["ID", "Deduction"]).agg({"Amount": "sum", "Raw_Code": "first"}).reset_index()

    # Group Uzio
    uzio_data = []
    for _, row in df_uzio.iterrows():
        eid = norm_id(row.get(u_id_col))
        if not eid: continue
        uzio_data.append({"ID": eid, "Deduction": str(row.get(u_ded_col, "")).strip(), "Amount": clean_money_val(row.get(u_amt_col))})
        
    if not uzio_data:
        return {"error": "No deduction data found in Uzio file."}
        
    df_u = pd.DataFrame(uzio_data).groupby(["ID", "Deduction"])["Amount"].sum().reset_index()

    merged = pd.merge(df_p, df_u, on=["ID", "Deduction"], how="outer", suffixes=("_P", "_U")).fillna(0)
    
    results = []
    for _, row in merged.iterrows():
        eid = row["ID"]
        ded = row["Deduction"]
        p_amt = row["Amount_P"]
        u_amt = row["Amount_U"]
        diff = abs(p_amt - u_amt)
        
        status = "Data Match" if diff < 0.01 else "Data Mismatch"
        if p_amt > 0 and u_amt == 0:
            status = "Missing in Uzio"
        elif u_amt > 0 and p_amt == 0:
            status = "Missing in Paycom"
            
        results.append({
            "Employee ID": eid, 
            "Deduction Name": ded,
            "Paycom Code": row.get("Raw_Code", ""),
            "Paycom Amount": p_amt, 
            "Uzio Amount": u_amt,
            "Difference": round(diff, 2),
            "Status": status
        })
        
    return {"Audit_Details": results}
