import re
import pandas as pd
from utils.audit_utils import smart_read_df

STATUS_MATCH = "Data Match"
STATUS_MISMATCH = "Data Mismatch"
STATUS_VAL_MISSING_UZIO = "Value missing in Uzio (Paycom has value)"
STATUS_VAL_MISSING_PAYCOM = "Value missing in Paycom (Uzio has value)"
STATUS_MISSING_UZIO = "Employee ID Not Found in Uzio"
STATUS_MISSING_PAYCOM = "Employee ID Not Found in Paycom"

def norm_str(x):
    return str(x).strip() if x is not None and pd.notna(x) else ""

def norm_digits(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return ""
    if isinstance(x, (float, int)): return str(int(x))
    return re.sub(r"\D", "", str(x))

def norm_money(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return 0.0
    if isinstance(x, (float, int)): return float(x)
    s = str(x).replace(",", "").replace("$", "").strip()
    try: return float(s) if s else 0.0
    except: return 0.0

_TYPE_CODE_MAP = {"22": "checking", "32": "savings", "1": "checking", "2": "checking"}

def strip_type(t):
    if not t: return ""
    s = str(t).strip()
    if s.endswith(".0"): s = s[:-2]
    if s in _TYPE_CODE_MAP: return _TYPE_CODE_MAP[s]
    return s.lower().replace("account", "").replace("code: ", "").strip()

def _get_field_val(acc, field):
    mapping = {"Routing Number": "Routing", "Account Number": "Account",
               "Account Type": "Type", "Amount": "Amount", "Percent": "Percent"}
    val = acc.get(mapping.get(field, ""), "")
    return str(val) if val != "" else ""

def _compare_field(field, u_val, p_val):
    u_n = str(u_val).strip() if u_val else ""
    p_n = str(p_val).strip() if p_val else ""
    if u_n == "" and p_n == "": return STATUS_MATCH
    if u_n == "" and p_n != "": return STATUS_VAL_MISSING_UZIO
    if u_n != "" and p_n == "": return STATUS_VAL_MISSING_PAYCOM
    if field == "Account Type":
        return STATUS_MATCH if strip_type(u_n) == strip_type(p_n) else STATUS_MISMATCH
    if field in ("Amount", "Percent"):
        try:
            if abs(float(u_n) - float(p_n)) < 0.01: return STATUS_MATCH
        except: pass
        return STATUS_MISMATCH
    return STATUS_MATCH if u_n == p_n else STATUS_MISMATCH

def run_paycom_payment_audit(uzio_content, paycom_content):
    """
    Full production-grade Paycom payment audit.
    Matches 3 sheets: Summary, Field_Summary_By_Status, Comparison_Detail_AllFields
    """
    df_uzio = smart_read_df(uzio_content, header=1, dtype=str)
    df_uzio.columns = [str(c).strip() for c in df_uzio.columns]

    df_paycom = smart_read_df(paycom_content, dtype=str)
    df_paycom.columns = [str(c).strip() for c in df_paycom.columns]

    # --- Build Uzio map ---
    uzio_map = {}
    uzio_emp_names = {}
    for _, row in df_uzio.iterrows():
        emp_id = norm_str(row.get("Employee ID") or row.get("EmpID", ""))
        if not emp_id: continue
        name_str = norm_str(row.get("Full Name", ""))
        if emp_id not in uzio_emp_names: uzio_emp_names[emp_id] = name_str
        acc = {
            "Routing": norm_digits(row.get("Routing Number")),
            "Account": norm_digits(row.get("Account Number")),
            "Type": norm_str(row.get("Account Type")),
            "Percent": norm_money(row.get("Paycheck Percentage")),
            "Amount": norm_money(row.get("Paycheck Amount")),
            "Name": name_str
        }
        if acc["Routing"] or acc["Account"]:
            uzio_map.setdefault(emp_id, [])
            if acc not in uzio_map[emp_id]: uzio_map[emp_id].append(acc)

    # --- Build Paycom map (Wide → multiple dist columns) ---
    pc_empid_col = next((c for c in df_paycom.columns if "Employee_Code" in c or "Emp Code" in c), df_paycom.columns[0])
    pc_first_col = next((c for c in df_paycom.columns if "Firstname" in c or "First Name" in c), "")
    pc_last_col = next((c for c in df_paycom.columns if "Lastname" in c or "Last Name" in c), "")
    uzio_keys = set(uzio_map.keys())

    paycom_accounts = []
    for _, row in df_paycom.iterrows():
        raw_id = row.get(pc_empid_col)
        if pd.isna(raw_id): continue
        s_id = str(raw_id).strip().rstrip(".0")
        emp_id = s_id.zfill(4) if s_id.isdigit() and len(s_id) < 4 else s_id
        if emp_id not in uzio_keys:
            # try padding
            for w in [3, 4, 5]:
                if s_id.zfill(w) in uzio_keys: emp_id = s_id.zfill(w); break

        dist_entries = []
        total_dist_pct = 0.0
        total_dist_amt = 0.0

        for i in range(1, 9):
            prefix = f"Dist_{i}_"
            d_acc = norm_digits(row.get(f"{prefix}Acct_Code"))
            d_rout = norm_digits(row.get(f"{prefix}Rout_Code"))
            raw_amt = row.get(f"{prefix}Amount")
            d_amt = norm_money(raw_amt)
            d_pct = norm_money(row.get(f"{prefix}Percent")) if f"{prefix}Percent" in df_paycom.columns else 0.0

            if d_pct == 0.0:
                raw_str = str(raw_amt).strip() if raw_amt is not None else ""
                if "%" in raw_str:
                    try: d_pct = float(raw_str.replace("%","").replace(",","").strip())
                    except: d_pct = 0.0
                    d_amt = 0.0
                elif 0.01 < abs(d_amt) <= 1.0:
                    d_pct = round(d_amt * 100, 4); d_amt = 0.0

            if d_pct == 0.0 and d_amt == 0.0 and not d_acc and not d_rout: continue
            total_dist_pct += d_pct; total_dist_amt += d_amt
            if d_acc or d_rout:
                d_type = row.get(f"{prefix}Type_Code")
                dist_entries.append({"EmpID": emp_id, "Routing": d_rout, "Account": d_acc,
                    "Type": str(d_type) if d_type is not None else "", "Percent": d_pct, "Amount": d_amt, "IsNet": False})

        paycom_accounts.extend([d for d in dist_entries if d["Amount"] > 0 or d["Percent"] > 0 or d["Account"] or d["Routing"]])

        net_acc = norm_digits(row.get("Net_Acct_Code"))
        net_rout = norm_digits(row.get("Net_Rout_Code"))
        if net_acc or net_rout:
            net_pct = round(100.0 - total_dist_pct, 4) if total_dist_pct > 0 else (0.0 if total_dist_amt > 0 else 100.0)
            if net_pct > 0 or net_acc or net_rout:
                p_type = row.get("Net_Type_Code")
                paycom_accounts.append({"EmpID": emp_id, "Routing": net_rout, "Account": net_acc,
                    "Type": str(p_type) if p_type is not None else "", "Percent": net_pct, "Amount": 0.0, "IsNet": True})

    paycom_map = {}
    for item in paycom_accounts:
        eid = item["EmpID"]
        paycom_map.setdefault(eid, [])
        if item not in paycom_map[eid]: paycom_map[eid].append(item)

    FIELDS = ["Routing Number", "Account Number", "Account Type", "Amount", "Percent"]
    rows = []
    all_emps = set(uzio_emp_names.keys()) | set(paycom_map.keys())

    for emp_id in sorted(all_emps):
        u_accs = uzio_map.get(emp_id, [])
        p_accs = paycom_map.get(emp_id, [])
        emp_name = u_accs[0]["Name"] if u_accs else uzio_emp_names.get(emp_id, "")

        if not u_accs and p_accs:
            is_in_uzio = emp_id in uzio_emp_names
            for p in p_accs:
                for field in FIELDS:
                    rows.append({"Employee ID": emp_id, "Employee Name": emp_name,
                        "Paycom_Account_Class": "Net Account" if p.get("IsNet") else "Distribution Account",
                        "Field": field, "UZIO_Value": "", "Paycom_Value": _get_field_val(p, field),
                        "Paycom_SourceOfTruth_Status": STATUS_VAL_MISSING_UZIO if is_in_uzio else STATUS_MISSING_UZIO})
            continue

        if u_accs and not p_accs:
            for u in u_accs:
                for field in FIELDS:
                    rows.append({"Employee ID": emp_id, "Employee Name": u["Name"],
                        "Paycom_Account_Class": "Not Found", "Field": field,
                        "UZIO_Value": _get_field_val(u, field), "Paycom_Value": "",
                        "Paycom_SourceOfTruth_Status": STATUS_MISSING_PAYCOM})
            continue

        p_remaining = list(p_accs)
        u_unmatched = []

        for u in u_accs:
            candidates = [p for p in p_remaining if u["Routing"] == p["Routing"] and u["Account"] == p["Account"]]
            if not candidates:
                u_unmatched.append(u); continue
            match = candidates[0] if len(candidates) == 1 else max(
                candidates, key=lambda c: (abs(u.get("Percent",0)-c.get("Percent",0))<0.01)*10 + (strip_type(u["Type"])==strip_type(c["Type"]))*5)
            if match: p_remaining.remove(match)
            for field in FIELDS:
                status = _compare_field(field, _get_field_val(u, field), _get_field_val(match, field)) if match else STATUS_VAL_MISSING_PAYCOM
                rows.append({"Employee ID": emp_id, "Employee Name": u["Name"],
                    "Paycom_Account_Class": "Net Account" if (match and match.get("IsNet")) else "Distribution Account",
                    "Field": field, "UZIO_Value": _get_field_val(u, field),
                    "Paycom_Value": _get_field_val(match, field) if match else "",
                    "Paycom_SourceOfTruth_Status": status})

        for u in u_unmatched:
            u_type = strip_type(u["Type"])
            match = next((p for p in p_remaining if u["Routing"] == p["Routing"] and u_type and u_type == strip_type(p["Type"])), None)
            if match:
                p_remaining.remove(match)
                for field in FIELDS:
                    rows.append({"Employee ID": emp_id, "Employee Name": u["Name"],
                        "Paycom_Account_Class": "Net Account" if match.get("IsNet") else "Distribution Account",
                        "Field": field, "UZIO_Value": _get_field_val(u, field),
                        "Paycom_Value": _get_field_val(match, field),
                        "Paycom_SourceOfTruth_Status": _compare_field(field, _get_field_val(u, field), _get_field_val(match, field))})
            else:
                for field in FIELDS:
                    rows.append({"Employee ID": emp_id, "Employee Name": u["Name"],
                        "Paycom_Account_Class": "Not Found", "Field": field,
                        "UZIO_Value": _get_field_val(u, field), "Paycom_Value": "Not Found",
                        "Paycom_SourceOfTruth_Status": STATUS_VAL_MISSING_PAYCOM})

        for p in p_remaining:
            for field in FIELDS:
                rows.append({"Employee ID": emp_id, "Employee Name": emp_name,
                    "Paycom_Account_Class": "Net Account" if p.get("IsNet") else "Distribution Account",
                    "Field": field, "UZIO_Value": "Not Found", "Paycom_Value": _get_field_val(p, field),
                    "Paycom_SourceOfTruth_Status": STATUS_VAL_MISSING_UZIO})

    comparison_detail = rows
    df_cd = pd.DataFrame(rows)
    field_summary = []
    if not df_cd.empty:
        pivot = df_cd.pivot_table(index="Field", columns="Paycom_SourceOfTruth_Status",
            values="Employee ID", aggfunc="count", fill_value=0)
        for c in [STATUS_MATCH, STATUS_MISMATCH, STATUS_VAL_MISSING_UZIO, STATUS_VAL_MISSING_PAYCOM,
                  STATUS_MISSING_UZIO, STATUS_MISSING_PAYCOM]:
            if c not in pivot.columns: pivot[c] = 0
        pivot["Total"] = pivot.sum(axis=1)
        field_summary = pivot.reset_index().to_dict(orient="records")

    uzio_k = set(uzio_emp_names.keys())
    paycom_k = set(paycom_map.keys())
    summary = [
        {"Metric": "Employees in Uzio sheet", "Value": len(uzio_k)},
        {"Metric": "Employees in Paycom sheet", "Value": len(paycom_k)},
        {"Metric": "Employees present in both", "Value": len(uzio_k & paycom_k)},
        {"Metric": "Employees missing in Paycom (Uzio only)", "Value": len(uzio_k - paycom_k)},
        {"Metric": "Employees missing in Uzio (Paycom only)", "Value": len(paycom_k - uzio_k)},
        {"Metric": "Total comparison rows", "Value": len(comparison_detail)},
        {"Metric": "Total NOT OK rows", "Value": sum(1 for r in comparison_detail if r["Paycom_SourceOfTruth_Status"] != STATUS_MATCH)},
    ]

    return {
        "Summary": summary,
        "Field_Summary_By_Status": field_summary,
        "Comparison_Detail_AllFields": comparison_detail
    }
