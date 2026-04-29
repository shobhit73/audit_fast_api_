import pandas as pd
import io
import re
from utils.audit_utils import get_identity_match_map, norm_ssn_canonical, clean_money_val, norm_colname

def norm_col(c):
    if c is None: return ""
    return str(c).strip().replace("\n", " ").strip()

def read_uzio_deduction(file_content):
    xls = pd.ExcelFile(io.BytesIO(file_content), engine='openpyxl')
    for sheet in xls.sheet_names:
        df_raw = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=20)
        header_row_idx = None
        for idx, row in df_raw.iterrows():
            row_vals = [str(v).strip().lower() for v in row.values if pd.notna(v)]
            if any("employee id" in v for v in row_vals) and any("deduction name" in v for v in row_vals):
                header_row_idx = idx
                break
        if header_row_idx is not None:
             df = pd.read_excel(xls, sheet_name=sheet, header=header_row_idx, dtype=str)
             df.columns = [norm_col(c) for c in df.columns]
             return df
    return None

def run_adp_deduction_audit(uzio_content, adp_content, mapping_dict):
    """
    Production-grade deduction audit logic.
    uzio_content: bytes
    adp_content: bytes
    mapping_dict: dict of {ADP_Deduction_Name: Uzio_Deduction_Name}
    """
    df_uzio = read_uzio_deduction(uzio_content)
    if df_uzio is None:
        return {"error": "Could not find 'Employee Id' and 'Deduction Name' in Uzio file."}

    xls_adp = pd.ExcelFile(io.BytesIO(adp_content), engine='openpyxl')
    adp_sheet = xls_adp.sheet_names[0]
    peek_df = pd.read_excel(xls_adp, sheet_name=adp_sheet, nrows=20, header=None)
    header_row_idx = 0
    for idx, row in peek_df.iterrows():
        row_str = " ".join([str(val).upper() for val in row.values])
        if "EMPLOYEE NAME" in row_str or "ASSOCIATE ID" in row_str:
            header_row_idx = idx
            break
    df_adp = pd.read_excel(xls_adp, sheet_name=adp_sheet, header=header_row_idx, dtype=str)
    
    df_uzio.columns = [norm_col(c) for c in df_uzio.columns]
    df_adp.columns = [norm_col(c) for c in df_adp.columns]

    mapping = {k.lower(): v for k, v in mapping_dict.items()}
    mapping.update(mapping_dict)

    adp_id_col = next((c for c in df_adp.columns if "associate" in c.lower() and "id" in c.lower()), None)
    adp_code_col = next((c for c in df_adp.columns if "deduction" in c.lower() and "code" in c.lower()), None)
    adp_amt_col = next((c for c in df_adp.columns if "amount" in c.lower() or "rate" in c.lower()), None)
    adp_desc_col = next((c for c in df_adp.columns if "deduction" in c.lower() and "description" in c.lower()), None)
    adp_pct_col = next((c for c in df_adp.columns if "deduction" in c.lower() and "%" in c.lower()), None)
    adp_ssn_col = next((c for c in df_adp.columns if "ssn" in c.lower() or "tax id" in c.lower()), None)

    uz_id_col = next((c for c in df_uzio.columns if "employee" in c.lower() and "id" in c.lower()), None)
    uz_ded_col = next((c for c in df_uzio.columns if "deduction" in c.lower() and "name" in c.lower()), None)
    uz_amt_col = next((c for c in df_uzio.columns if "amount" in c.lower() or "percent" in c.lower()), None)
    uz_ssn_col = next((c for c in df_uzio.columns if "ssn" in c.lower()), None)

    if not all([adp_id_col, adp_code_col, adp_amt_col]):
        return {"error": f"ADP Sheet missing required columns. Found: {list(df_adp.columns)}"}
    if not all([uz_id_col, uz_ded_col, uz_amt_col]):
        return {"error": f"Uzio Sheet missing required columns. Found: {list(df_uzio.columns)}"}

    uz_to_adp_id_map = {}
    if uz_ssn_col and adp_ssn_col:
        uz_to_adp_id_map = get_identity_match_map(df_uzio, df_adp, uzio_id_col=uz_id_col, vendor_id_col=adp_id_col, uzio_ssn_col=uz_ssn_col, vendor_ssn_col=adp_ssn_col)
    adp_to_uz_id_map = {v: k for k, v in uz_to_adp_id_map.items()}

    adp_records = []
    for _, row in df_adp.iterrows():
        emp_id = str(row[adp_id_col]).strip()
        raw_code = str(row[adp_code_col]).strip()
        raw_desc = str(row[adp_desc_col]).strip() if adp_desc_col else ""
        deduction_name = mapping.get(raw_desc, mapping.get(raw_desc.lower(), mapping.get(raw_code, mapping.get(raw_code.lower()))))
        if not deduction_name: continue
        amt = clean_money_val(row[adp_amt_col])
        if amt == 0.0 and adp_pct_col:
            pct_val = clean_money_val(row[adp_pct_col])
            if pct_val != 0.0: amt = pct_val
        match_id = adp_to_uz_id_map.get(emp_id, emp_id)
        adp_records.append({"Employee_ID": emp_id, "Deduction_Name": deduction_name, "ADP_Raw_Code": raw_code, "ADP_Description": raw_desc, "ADP_Amount": amt, "Key": f"{match_id}|{deduction_name}".lower()})
    
    df_adp_clean = pd.DataFrame(adp_records)
    if not df_adp_clean.empty:
        df_adp_clean = df_adp_clean.groupby(["Employee_ID", "Deduction_Name", "ADP_Raw_Code", "ADP_Description", "Key"], as_index=False)["ADP_Amount"].sum()

    uzio_records = []
    for _, row in df_uzio.iterrows():
        emp_id = str(row[uz_id_col]).strip()
        ded_name = str(row[uz_ded_col]).strip()
        amt = clean_money_val(row[uz_amt_col])
        uzio_records.append({"Uzio_Employee_ID": emp_id, "Uzio_Deduction_Name": ded_name, "Uzio_Amount": amt, "Key": f"{emp_id}|{ded_name}".lower()})
    
    df_uz_clean = pd.DataFrame(uzio_records)
    if not df_uz_clean.empty:
        df_uz_clean = df_uz_clean.groupby(["Uzio_Employee_ID", "Uzio_Deduction_Name", "Key"], as_index=False)["Uzio_Amount"].sum()

    merged = pd.merge(df_adp_clean, df_uz_clean, on="Key", how="outer")
    results = []
    for _, row in merged.iterrows():
        adp_val = row["ADP_Amount"] if pd.notna(row["ADP_Amount"]) else 0.0
        uz_val = row["Uzio_Amount"] if pd.notna(row["Uzio_Amount"]) else 0.0
        status = "Data Match" if abs(adp_val - uz_val) < 0.01 else "Data Mismatch"
        
        adp_id = row["Employee_ID"] if pd.notna(row["Employee_ID"]) else ""
        uz_id = row["Uzio_Employee_ID"] if pd.notna(row["Uzio_Employee_ID"]) else ""
        
        results.append({
            "Employee ID": uz_id if uz_id else adp_id,
            "Deduction": row["Uzio_Deduction_Name"] if pd.notna(row["Uzio_Deduction_Name"]) else row["ADP_Description"],
            "ADP Amount": adp_val,
            "Uzio Amount": uz_val,
            "Status": status
        })
    
    df_res = pd.DataFrame(results)
    
    # Summary tab: counts per deduction
    summary_data = {}
    if not df_res.empty:
        for ded, grp in df_res.groupby("Deduction"):
            summary_data[ded] = {"Total": len(grp), "Match": (grp["Status"] == "Data Match").sum(), "Mismatch": (grp["Status"] == "Data Mismatch").sum()}
    df_summary = pd.DataFrame([{"Metric": k, **v} for k, v in summary_data.items()])
    
    # Field_Summary_By_Status: pivot of Deduction x Status counts
    field_summary = []
    if not df_res.empty:
        pivot = df_res.groupby(["Deduction", "Status"]).size().unstack(fill_value=0).reset_index()
        field_summary = pivot.to_dict(orient="records")
    
    return {
        "Summary": df_summary.to_dict(orient="records") if not df_summary.empty else [],
        "Field_Summary_By_Status": field_summary,
        "Audit Details": results
    }
