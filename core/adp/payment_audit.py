import pandas as pd
import io
import re
from utils.audit_utils import smart_read_df

def norm_str(x):
    return str(x).strip() if pd.notna(x) else ""

def norm_digits(x):
    if pd.isna(x) or x is None: return ""
    if isinstance(x, (float, int)): return str(int(x))
    return re.sub(r"\D", "", str(x))

def norm_money(x):
    if pd.isna(x) or x is None: return 0.0
    if isinstance(x, (float, int)): return float(x)
    s = str(x).replace(",", "").replace("$", "").replace("%", "").strip()
    try: return float(s) if s else 0.0
    except: return 0.0

def normalize_account_type(t):
    if not t: return ""
    s = str(t).strip().lower()
    if "checking" in s or "ck" in s: return "Checking"
    if "savings" in s or "sv" in s: return "Savings"
    return str(t).strip()

def run_adp_payment_audit(uzio_content, adp_content):
    """Production-grade payment audit logic."""
    df_uzio = smart_read_df(uzio_content, header=1, dtype=str)
    df_uzio.columns = [str(c).strip().replace("\n", " ") for c in df_uzio.columns]
    
    uzio_map = {}
    for _, row in df_uzio.iterrows():
        emp_id = str(row.get("Employee ID") or row.get("EmpID") or "").strip()
        if not emp_id: continue
        if emp_id not in uzio_map: uzio_map[emp_id] = []
        acc = {
            "Routing": norm_digits(row.get("Routing Number")),
            "Account": norm_digits(row.get("Account Number")),
            "Type": normalize_account_type(row.get("Account Type")),
            "Percent": norm_money(row.get("Paycheck Percentage")),
            "Amount": norm_money(row.get("Paycheck Amount"))
        }
        if acc["Routing"] or acc["Account"]:
            if acc not in uzio_map[emp_id]: uzio_map[emp_id].append(acc)

    df_adp = smart_read_df(adp_content, dtype=str)
    adp_map = {}
    a_cols = {
        "EmpID": next((c for c in df_adp.columns if "ASSOCIATE ID" in c.upper()), "ASSOCIATE ID"),
        "Routing": next((c for c in df_adp.columns if "ROUTING NUMBER" in c.upper()), "ROUTING NUMBER"),
        "Account": next((c for c in df_adp.columns if "ACCOUNT NUMBER" in c.upper()), "ACCOUNT NUMBER"),
        "Deduction": next((c for c in df_adp.columns if "DEDUCTION" in c.upper()), "DEDUCTION"),
        "DepositType": next((c for c in df_adp.columns if "DEPOSIT TYPE" in c.upper()), "DEPOSIT TYPE"),
        "Percent": next((c for c in df_adp.columns if "DEPOSIT PERCENT" in c.upper()), "DEPOSIT PERCENT"),
        "Amount": next((c for c in df_adp.columns if "DEPOSIT AMOUNT" in c.upper()), "DEPOSIT AMOUNT")
    }

    for _, row in df_adp.iterrows():
        emp_id = str(row.get(a_cols["EmpID"]) or "").strip()
        if not emp_id: continue
        if emp_id not in adp_map: adp_map[emp_id] = []
        
        dep_type = str(row.get(a_cols["DepositType"])).strip()
        pct = norm_money(row.get(a_cols["Percent"])) if "Full" in dep_type or "Balance" in dep_type or "Partial %" in dep_type else 0.0
        amt = norm_money(row.get(a_cols["Amount"])) if "Partial" in dep_type and "%" not in dep_type else 0.0
        
        acc = {
            "Routing": norm_digits(row.get(a_cols["Routing"])),
            "Account": norm_digits(row.get(a_cols["Account"])),
            "Type": normalize_account_type(row.get(a_cols["Deduction"])),
            "Percent": pct if pct > 0 or "Full" in dep_type or "Balance" in dep_type else 0.0,
            "Amount": amt,
            "IsNet": "Full" in dep_type or "Balance" in dep_type
        }
        if acc["Routing"] or acc["Account"]: adp_map[emp_id].append(acc)

    # Simple 100% logic for single account net pay
    for eid, accs in adp_map.items():
        if len(accs) == 1 and accs[0]["IsNet"]: accs[0]["Percent"] = 100.0

    rows = []
    all_ids = set(uzio_map.keys()) | set(adp_map.keys())
    for eid in sorted(all_ids):
        u_accs = uzio_map.get(eid, [])
        a_accs = adp_map.get(eid, [])
        
        # Match by Account Number
        for u in u_accs:
            match = next((a for a in a_accs if a["Account"] == u["Account"]), None)
            status = "Data Match" if match and abs(u["Percent"] - match["Percent"]) < 0.01 and abs(u["Amount"] - match["Amount"]) < 0.01 else "Data Mismatch"
            rows.append({
                "Employee ID": eid,
                "Routing": u["Routing"],
                "Account": u["Account"],
                "Status": status if match else "Missing in ADP"
            })
    df_res = pd.DataFrame(rows)
    summary = []
    if not df_res.empty:
        counts = df_res["Status"].value_counts().reset_index()
        counts.columns = ["Status", "Count"]
        summary = counts.to_dict(orient="records")

    return {
        "Comparison_Detail": rows,
        "Summary": summary
    }
