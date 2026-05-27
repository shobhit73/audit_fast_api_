import pandas as pd
import io
import re
from utils.audit_utils import smart_read_df

# Mixed-mode (Partial $ + Partial %) statuses for the Exception bucket.
# Mirrors the R4 transformation done by the Streamlit sanity tool
# (apps/adp/payment_method_sanity.py). The math here is duplicated by design:
# the sanity tool stays the source of truth, this port stays in sync manually.
STATUS_CORRECTED_SETUP = "Corrected Setup (Mixed Mode)"
STATUS_MIXED_MODE_MISMATCH = "Mismatch (Mixed Mode)"

MODE_FULL = "Full"
MODE_PARTIAL_PCT = "Partial %"
MODE_PARTIAL_AMT = "Partial $"


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


def _classify_adp_mode(dep_type: str) -> str:
    """Tag an ADP Deposit Type as Full / Partial % / Partial $.

    Order matters: 'Partial %' must be checked before 'Partial' since the
    latter is a substring of the former.
    """
    s = (dep_type or "").strip().lower()
    if "full" in s or "balance" in s:
        return MODE_FULL
    if "partial %" in s or "partial%" in s or "%" in s or "percent" in s:
        return MODE_PARTIAL_PCT
    if "partial" in s or "amount" in s or "flat" in s:
        return MODE_PARTIAL_AMT
    return MODE_FULL


def _compute_r4_expected(adp_accs):
    """Per-account distribution Uzio should hold for a mixed-mode employee.

    Duplicates Rule R4 from apps/adp/payment_method_sanity.py. Keep in sync
    with the Streamlit version when R4 changes there.
    """
    pct_accs = [a for a in adp_accs if a.get("Mode") == MODE_PARTIAL_PCT]
    amt_accs = [a for a in adp_accs if a.get("Mode") == MODE_PARTIAL_AMT]
    full_accs = [a for a in adp_accs if a.get("Mode") == MODE_FULL]

    kept_pct = sum(a.get("Percent", 0.0) or 0.0 for a in pct_accs)
    non_pct_accs = amt_accs + full_accs
    remaining_pct = 100.0 - kept_pct

    expected = {}
    if remaining_pct <= 0 or not non_pct_accs:
        for a in adp_accs:
            if a in pct_accs:
                expected[id(a)] = {"expected_pct": round(a.get("Percent", 0.0) or 0.0, 2), "expected_amt": 0.0}
            else:
                expected[id(a)] = {"expected_pct": 0.0, "expected_amt": 0.0}
        return expected

    equal_share = round(remaining_pct / len(non_pct_accs), 2)
    full_acc = full_accs[0] if full_accs else amt_accs[-1]
    non_full_non_pct = [a for a in non_pct_accs if a is not full_acc]
    running_total = kept_pct + equal_share * len(non_full_non_pct)
    full_share = round(100.0 - running_total, 2)

    for a in adp_accs:
        if a in pct_accs:
            expected[id(a)] = {"expected_pct": round(a.get("Percent", 0.0) or 0.0, 2), "expected_amt": 0.0}
        elif a is full_acc:
            expected[id(a)] = {"expected_pct": full_share, "expected_amt": 0.0}
        else:
            expected[id(a)] = {"expected_pct": equal_share, "expected_amt": 0.0}
    return expected


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
            "IsNet": "Full" in dep_type or "Balance" in dep_type,
            "Mode": _classify_adp_mode(dep_type),
        }
        if acc["Routing"] or acc["Account"]: adp_map[emp_id].append(acc)

    # Simple 100% logic for single account net pay
    for eid, accs in adp_map.items():
        if len(accs) == 1 and accs[0]["IsNet"]: accs[0]["Percent"] = 100.0

    # Identify mixed-mode employees (both Partial $ and Partial % present on ADP side).
    mixed_mode_emp_ids = set()
    for eid, accs in adp_map.items():
        modes = {a.get("Mode") for a in accs}
        if MODE_PARTIAL_PCT in modes and MODE_PARTIAL_AMT in modes:
            mixed_mode_emp_ids.add(eid)

    rows = []
    exception_rows = []
    all_ids = set(uzio_map.keys()) | set(adp_map.keys())
    for eid in sorted(all_ids):
        u_accs = uzio_map.get(eid, [])
        a_accs = adp_map.get(eid, [])

        # Mixed-mode short-circuit: route to Exception bucket with R4 expected values.
        if eid in mixed_mode_emp_ids:
            expected_map = _compute_r4_expected(a_accs)
            for u in u_accs:
                match = next((a for a in a_accs if a["Account"] == u["Account"]), None)
                if match is None:
                    exception_rows.append({
                        "Employee ID": eid,
                        "Routing": u["Routing"],
                        "Account": u["Account"],
                        "UZIO_Percent": u["Percent"],
                        "UZIO_Amount": u["Amount"],
                        "ADP_Percent": None,
                        "ADP_Amount": None,
                        "Expected_Percent": None,
                        "Expected_Amount": None,
                        "Status": STATUS_MIXED_MODE_MISMATCH,
                    })
                    continue
                exp = expected_map.get(id(match), {"expected_pct": 0.0, "expected_amt": 0.0})
                pct_ok = abs(u["Percent"] - exp["expected_pct"]) < 0.01
                amt_ok = abs(u["Amount"] - exp["expected_amt"]) < 0.01
                exception_rows.append({
                    "Employee ID": eid,
                    "Routing": u["Routing"],
                    "Account": u["Account"],
                    "UZIO_Percent": u["Percent"],
                    "UZIO_Amount": u["Amount"],
                    "ADP_Percent": match["Percent"],
                    "ADP_Amount": match["Amount"],
                    "Expected_Percent": exp["expected_pct"],
                    "Expected_Amount": exp["expected_amt"],
                    "Status": STATUS_CORRECTED_SETUP if (pct_ok and amt_ok) else STATUS_MIXED_MODE_MISMATCH,
                })
            # Unmatched ADP-side accounts: they exist in ADP but not in Uzio after migration.
            uzio_accounts_seen = {u["Account"] for u in u_accs}
            for a in a_accs:
                if a["Account"] in uzio_accounts_seen:
                    continue
                exp = expected_map.get(id(a), {"expected_pct": 0.0, "expected_amt": 0.0})
                exception_rows.append({
                    "Employee ID": eid,
                    "Routing": a["Routing"],
                    "Account": a["Account"],
                    "UZIO_Percent": None,
                    "UZIO_Amount": None,
                    "ADP_Percent": a["Percent"],
                    "ADP_Amount": a["Amount"],
                    "Expected_Percent": exp["expected_pct"],
                    "Expected_Amount": exp["expected_amt"],
                    "Status": STATUS_MIXED_MODE_MISMATCH,
                })
            continue

        # Standard single-mode path (unchanged behavior).
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
    if not df_res.empty or exception_rows:
        parts = []
        if rows:
            parts.append(pd.DataFrame(rows)[["Status"]])
        if exception_rows:
            parts.append(pd.DataFrame(exception_rows)[["Status"]])
        df_all = pd.concat(parts, ignore_index=True)
        counts = df_all["Status"].value_counts().reset_index()
        counts.columns = ["Status", "Count"]
        summary = counts.to_dict(orient="records")

    return {
        "Comparison_Detail": rows,
        "Exception_Mixed_Mode": exception_rows,
        "Summary": summary
    }
