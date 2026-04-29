import pandas as pd
import io
import re
from datetime import datetime
from utils.audit_utils import get_identity_match_map, smart_read_df

NO_SIT_STATES = {"FL", "TX", "NV", "WA", "WY", "SD", "AK", "TN", "NH"}
FIELDS_REQUIRING_UI_VERIFICATION = {"SIT_WITHHOLDING_EXEMPTION"}

FILING_STATUS_MAP = {
    "FEDERAL_SINGLE": "Single",
    "FEDERAL_MARRIED": "Married",
    "FEDERAL_MARRIED_SINGLE": "Married but withhold as Single",
    "FEDERAL_HEAD_OF_HOUSEHOLD": "Head of Household",
    "NM_SINGLE": "Single or Married filing separately",
    "NM_MARRIED": "Married filing jointly or Qualifying Surviving Spouse",
    "NY_SINGLE": "Single",
    "NY_MARRIED": "Married",
    "CA_SINGLE": "Single or Married (with two or more incomes)",
    "CA_MARRIED": "Married (one income)",
    "GA_SINGLE": "Single",
    "GA_MARRIED": "Married",
    "IL_SINGLE": "Single",
    "IL_MARRIED": "Married",
    "NC_SINGLE": "Single",
    "NC_MARRIED": "Married",
    "PA_SINGLE": "Single",
    "PA_MARRIED": "Married",
    "OH_SINGLE": "Single",
    "OH_MARRIED": "Married",
    "VA_SINGLE": "Single",
    "VA_MARRIED": "Married",
    "CO_SINGLE": "Single",
    "CO_MARRIED": "Married",
    "AZ_SINGLE": "Single",
    "AZ_MARRIED": "Married",
}

FIELD_MAPPING = [
    {"UZIO": "FIT_WITHHOLDING_EXEMPTION",           "ADP": "Do Not Calculate Federal Income Tax",      "Label": "Fed Exempt"},
    {"UZIO": "FIT_ADDL_WITHHOLDING_PER_PAY_PERIOD", "ADP": "Federal Additional Tax Amount",            "Label": "Fed Addl $"},
    {"UZIO": "FIT_FILING_STATUS",                   "ADP": "Federal/W4 Marital Status Description",   "Label": "Fed Filing Status"},
    {"UZIO": "FIT_CHILD_AND_DEPENDENT_TAX_CREDIT",  "ADP": "Dependents",                              "Label": "Fed Child Credit"},
    {"UZIO": "FIT_DEDUCTIONS_OVER_STANDARD",        "ADP": "Deductions",                              "Label": "Fed Deductions"},
    {"UZIO": "FIT_HIGHER_WITHHOLDING",              "ADP": "Multiple Jobs indicator",                 "Label": "Fed Multi-Jobs"},
    {"UZIO": "FIT_OTHER_INCOME",                    "ADP": "Other Income",                            "Label": "Fed Other Income"},
    {"UZIO": "SIT_WITHHOLDING_EXEMPTION",           "ADP": "Do not calculate State Tax",              "Label": "State Exempt"},
    {"UZIO": "SIT_FILING_STATUS",                   "ADP": "State Marital Status Description",        "Label": "State Filing Status"},
    {"UZIO": "SIT_TOTAL_ALLOWANCES",                "ADP": "State Exemptions/Allowances",             "Label": "State Allowances"},
    {"UZIO": "SIT_ADDL_WITHHOLDING_PER_PAY_PERIOD", "ADP": "State Additional Tax Amount",            "Label": "State Addl $"},
]

MONEY_CENTS_FIELDS = {
    "FIT_ADDL_WITHHOLDING_PER_PAY_PERIOD", "FIT_CHILD_AND_DEPENDENT_TAX_CREDIT",
    "FIT_DEDUCTIONS_OVER_STANDARD", "FIT_OTHER_INCOME", "SIT_ADDL_WITHHOLDING_PER_PAY_PERIOD"
}

def _clean(x):
    return str(x).strip() if pd.notna(x) and x is not None else ""

def _parse_date(d_str):
    try: return pd.to_datetime(d_str)
    except: return pd.NaT

def _norm_filing_status(s):
    return re.sub(r'[\W_]+', ' ', _clean(s).lower()).strip()

def _norm_bool(s):
    s = str(s).strip().lower()
    if s in {"yes", "y", "true", "1", "on"}: return "1"
    if s in {"no", "n", "false", "0", "off"}: return "0"
    return ""

def _norm_float(s):
    s = str(s).replace("$", "").replace(",", "").strip()
    if not s: return None
    if s.startswith("(") and s.endswith(")"): s = "-" + s[1:-1]
    try: return float(s)
    except: return None

def get_field_label(uz_key):
    for m in FIELD_MAPPING:
        if m["UZIO"] == uz_key: return m["Label"]
    return uz_key

def determine_jurisdiction(uz_key):
    if uz_key.startswith("FIT_"): return "Federal"
    if uz_key.startswith("SIT_"): return "State"
    return "Other"

def compare_values(uz_key, adp_val_raw, uz_val_raw):
    araw, uraw = _clean(adp_val_raw), _clean(uz_val_raw)
    if "EXEMPTION" in uz_key or "HIGHER_WITHHOLDING" in uz_key:
        ab = _norm_bool(araw) or "0"
        ub = _norm_bool(uraw) or "0"
        rule = f"Bool compare: ADP='{araw}' → '{ab}', UZIO='{uraw}' → '{ub}'"
        return (ab == ub), ab, ub, "bool", rule
    if "FILING_STATUS" in uz_key:
        u_mapped = FILING_STATUS_MAP.get(uraw, uraw.split("_", 1)[1].replace("_", " ").title() if "_" in uraw else uraw)
        an = _norm_filing_status(araw)
        un = _norm_filing_status(u_mapped)
        match = an == un or an in un or un in an
        rule = f"Filing status: ADP='{araw}' → '{an}', UZIO='{uraw}' → '{u_mapped}' → '{un}'"
        return match, an, un, "filing_status", rule
    if uz_key in MONEY_CENTS_FIELDS:
        af = _norm_float(araw) or 0.0
        uf = (_norm_float(uraw) or 0.0) / 100.0
        match = abs(af - uf) < 0.01
        rule = f"Money (cents÷100): ADP='{araw}' → {af}, UZIO='{uraw}' → {uf}"
        return match, str(af), str(uf), "money_cents", rule
    af = _norm_float(araw) or 0.0
    uf = _norm_float(uraw) or 0.0
    rule = f"Numeric: ADP='{araw}' → {af}, UZIO='{uraw}' → {uf}"
    return (af == uf), str(af), str(uf), "numeric", rule

def run_adp_withholding_audit(uzio_content, adp_content):
    """Production-grade withholding audit — full 13-sheet output matching Streamlit tool."""
    def read_df(c, **kwargs):
        return smart_read_df(c, **kwargs)

    adp_df = read_df(adp_content)
    uzio_df = read_df(uzio_content)

    adp_id_col = next((c for c in adp_df.columns if c.lower().strip() in ["associate id", "employee id"]), adp_df.columns[0])
    uzio_id_col = next((c for c in uzio_df.columns if c.lower().strip() in ["employee_id", "employee id"]), uzio_df.columns[0])
    uzio_key_col = next((c for c in uzio_df.columns if "field_key" in c.lower()), "withholding_field_key")
    uzio_val_col = next((c for c in uzio_df.columns if "field_value" in c.lower()), "withholding_field_value")

    # ADP: track multi-row employees (W4 history) and deduplicate to most recent
    date_report_rows = []
    adp_id_counts = adp_df[adp_id_col].value_counts()
    multi_row_emp_ids = set(adp_id_counts[adp_id_counts > 1].index)

    eff_date_col = next((c for c in adp_df.columns if "effective date" in c.lower()), None)
    if eff_date_col:
        adp_df["_eff_date"] = adp_df[eff_date_col].apply(_parse_date)
        adp_sorted = adp_df.sort_values([adp_id_col, "_eff_date"], ascending=[True, False])
        for eid, grp in adp_sorted.groupby(adp_id_col):
            dates_used = grp[eff_date_col].tolist()
            date_report_rows.append({
                "EMPLOYEE_ID": eid,
                "ROWS_IN_ADP": len(grp),
                "EFFECTIVE_DATES": ", ".join(str(d) for d in dates_used),
                "DATE_USED": str(dates_used[0]) if dates_used else ""
            })
        adp_df = adp_sorted.drop_duplicates(subset=[adp_id_col], keep="first")
    date_report = pd.DataFrame(date_report_rows) if date_report_rows else pd.DataFrame(columns=["EMPLOYEE_ID", "ROWS_IN_ADP", "EFFECTIVE_DATES", "DATE_USED"])

    # Pivot UZIO long → wide
    uzio_wide = uzio_df.pivot_table(index=uzio_id_col, columns=uzio_key_col, values=uzio_val_col, aggfunc="last").reset_index()

    # ADP status column
    adp_status_col = next((c for c in adp_df.columns if "position status" in c.lower() or "employment status" in c.lower()), None)
    adp_state_col = next((c for c in adp_df.columns if c.lower().strip() in ["state", "work state", "state code"]), None)
    name_col1 = next((c for c in adp_df.columns if "first name" in c.lower() or "legal first" in c.lower()), None)
    name_col2 = next((c for c in adp_df.columns if "last name" in c.lower() or "legal last" in c.lower()), None)

    adp_ids = set(adp_df[adp_id_col].dropna())
    uzio_ids = set(uzio_wide[uzio_id_col].dropna())
    missing_in_uzio_ids = adp_ids - uzio_ids
    missing_in_adp_ids = uzio_ids - adp_ids

    missing_in_uzio = adp_df[adp_df[adp_id_col].isin(missing_in_uzio_ids)]
    missing_in_adp_df = uzio_wide[uzio_wide[uzio_id_col].isin(missing_in_adp_ids)]

    both = pd.merge(adp_df, uzio_wide, left_on=adp_id_col, right_on=uzio_id_col, how="inner")
    both["_IS_ACTIVE"] = both[adp_status_col].apply(lambda x: "active" in str(x).lower()) if adp_status_col else False

    adp_map = {m["UZIO"]: m["ADP"] for m in FIELD_MAPPING}

    mismatches = []
    ui_verification_needed = []
    false_positives_filtered = []
    rules_tracked = {}

    for _, row in both.iterrows():
        emp_id = row[adp_id_col]
        emp_status = "ACTIVE" if row.get("_IS_ACTIVE") else "TERMINATED"
        state_code = str(row.get(adp_state_col, "")).strip().upper() if adp_state_col else ""
        is_multi_row = emp_id in multi_row_emp_ids
        eff_date = str(row.get(eff_date_col, "")) if eff_date_col else ""
        fname = str(row.get(name_col1, "")).strip() if name_col1 else ""
        lname = str(row.get(name_col2, "")).strip() if name_col2 else ""
        emp_name = f"{fname} {lname}".strip()

        for uz_key, adp_col in adp_map.items():
            if adp_col not in both.columns or uz_key not in both.columns: continue

            a_raw = _clean(row.get(adp_col))
            u_raw = _clean(row.get(uz_key))

            # Skip SIT for no-SIT states
            if uz_key.startswith("SIT_") and state_code in NO_SIT_STATES:
                false_positives_filtered.append({
                    "EMPLOYEE_ID": emp_id, "EMPLOYEE_NAME": emp_name,
                    "STATE_CODE": state_code, "FIELD_KEY": uz_key,
                    "REASON": f"No SIT state ({state_code})",
                    "ADP_VALUE_RAW": a_raw, "UZIO_VALUE_RAW": u_raw
                })
                continue

            # UI verification fields
            if uz_key in FIELDS_REQUIRING_UI_VERIFICATION:
                match, a_n, u_n, c_type, rule_str = compare_values(uz_key, a_raw, u_raw)
                if not match:
                    ui_verification_needed.append({
                        "EMPLOYEE_ID": emp_id, "EMPLOYEE_NAME": emp_name,
                        "EMPLOYMENT_STATUS": emp_status, "STATE_CODE": state_code,
                        "FIELD_LABEL": get_field_label(uz_key), "FIELD_KEY": uz_key,
                        "ADP_COLUMN": adp_col, "ADP_VALUE_RAW": a_raw, "UZIO_VALUE_RAW": u_raw,
                        "WHY_FLAGGED_FOR_VERIFICATION": (
                            "This field is an internal/derived DB flag in Uzio "
                            "(typically auto-set when employee is Federal-Exempt). "
                            "It does NOT appear in the editable Uzio UI. "
                            "Verify in UI before taking action."
                        ),
                        "ADP_EFFECTIVE_DATE_USED": eff_date,
                    })
                continue

            # SIT_TOTAL_ALLOWANCES computed fallback
            if uz_key == "SIT_TOTAL_ALLOWANCES":
                u_computed_raw = u_raw
                rule_str = "Compare ADP State Exemptions/Allowances to UZIO SIT_TOTAL_ALLOWANCES"
                if u_computed_raw == "":
                    u_basic = _norm_float(row.get("SIT_BASIC_ALLOWANCES", ""))
                    u_addl = _norm_float(row.get("SIT_ADDITIONAL_ALLOWANCES", ""))
                    if u_basic is not None or u_addl is not None:
                        u_computed_raw = str(int((u_basic or 0) + (u_addl or 0)))
                        rule_str = "SIT_BASIC_ALLOWANCES + SIT_ADDITIONAL_ALLOWANCES"
                match, a_n, u_n, _, _ = compare_values(uz_key, a_raw, u_computed_raw)
                u_raw_display = u_computed_raw if u_raw == "" else u_raw
            else:
                match, a_n, u_n, c_type, rule_str = compare_values(uz_key, a_raw, u_raw)
                u_raw_display = u_raw
                c_type = c_type

            if uz_key not in rules_tracked:
                rules_tracked[uz_key] = {
                    "FIELD_LABEL": get_field_label(uz_key),
                    "FIELD_KEY": uz_key,
                    "JURISDICTION": determine_jurisdiction(uz_key),
                    "ADP_COLUMN": adp_col,
                    "RULE_APPLIED": rule_str
                }

            if not match:
                mismatches.append({
                    "EMPLOYEE_ID": emp_id, "EMPLOYEE_NAME": emp_name,
                    "EMPLOYMENT_STATUS": emp_status, "STATE_CODE": state_code,
                    "FIELD_LABEL": get_field_label(uz_key), "FIELD_KEY": uz_key,
                    "ADP_COLUMN": adp_col, "UZIO_FIELD": uz_key,
                    "ADP_VALUE_RAW": a_raw, "UZIO_VALUE_RAW": u_raw_display,
                    "ADP_VALUE_NORMALIZED": a_n, "UZIO_VALUE_NORMALIZED": u_n,
                    "RULE_APPLIED": rule_str,
                    "ADP_EFFECTIVE_DATE_USED": eff_date,
                    "HAS_W4_HISTORY": "Yes" if is_multi_row else "No",
                    "VERIFY_IN_UI_FIRST": "Yes" if is_multi_row else "",
                })

    df_miss_all = pd.DataFrame(mismatches) if mismatches else pd.DataFrame(columns=[
        "EMPLOYEE_ID","EMPLOYEE_NAME","EMPLOYMENT_STATUS","STATE_CODE","FIELD_LABEL","FIELD_KEY",
        "ADP_COLUMN","UZIO_FIELD","ADP_VALUE_RAW","UZIO_VALUE_RAW","ADP_VALUE_NORMALIZED",
        "UZIO_VALUE_NORMALIZED","RULE_APPLIED","ADP_EFFECTIVE_DATE_USED","HAS_W4_HISTORY","VERIFY_IN_UI_FIRST"
    ])

    df_miss_active = df_miss_all[df_miss_all["EMPLOYMENT_STATUS"] == "ACTIVE"].copy() if not df_miss_all.empty else df_miss_all.copy()
    df_miss_term = df_miss_all[df_miss_all["EMPLOYMENT_STATUS"] != "ACTIVE"].copy() if not df_miss_all.empty else df_miss_all.copy()

    df_ui_verification = pd.DataFrame(ui_verification_needed) if ui_verification_needed else pd.DataFrame(columns=[
        "EMPLOYEE_ID","EMPLOYEE_NAME","EMPLOYMENT_STATUS","STATE_CODE","FIELD_LABEL","FIELD_KEY",
        "ADP_COLUMN","ADP_VALUE_RAW","UZIO_VALUE_RAW","WHY_FLAGGED_FOR_VERIFICATION","ADP_EFFECTIVE_DATE_USED"
    ])
    df_filtered = pd.DataFrame(false_positives_filtered) if false_positives_filtered else pd.DataFrame(columns=[
        "EMPLOYEE_ID","EMPLOYEE_NAME","STATE_CODE","FIELD_KEY","REASON","ADP_VALUE_RAW","UZIO_VALUE_RAW"
    ])

    if not df_miss_all.empty:
        df_sum = df_miss_all.groupby(["FIELD_LABEL","FIELD_KEY"]).agg(
            mismatch_rows=("EMPLOYEE_ID","count"), employees_affected=("EMPLOYEE_ID","nunique")
        ).reset_index()
        df_emp_sum = df_miss_all.groupby("EMPLOYEE_ID").agg(
            EMPLOYEE_NAME=("EMPLOYEE_NAME","first"), EMPLOYMENT_STATUS=("EMPLOYMENT_STATUS","first"),
            STATE_CODE=("STATE_CODE","first"), mismatch_rows=("FIELD_KEY","count"),
            fields=("FIELD_LABEL", lambda x: ", ".join(sorted(set(x))))
        ).reset_index()
    else:
        df_sum = pd.DataFrame(columns=["FIELD_LABEL","FIELD_KEY","mismatch_rows","employees_affected"])
        df_emp_sum = pd.DataFrame(columns=["EMPLOYEE_ID","EMPLOYEE_NAME","EMPLOYMENT_STATUS","STATE_CODE","mismatch_rows","fields"])

    df_rules = pd.DataFrame(list(rules_tracked.values())) if rules_tracked else pd.DataFrame(columns=["FIELD_LABEL","FIELD_KEY","JURISDICTION","ADP_COLUMN","RULE_APPLIED"])

    is_active_mask = both["_IS_ACTIVE"] == True
    metrics = [
        {"Metric": "UZIO employees (total)", "Value": len(uzio_ids)},
        {"Metric": "UZIO employees (Active)", "Value": int(is_active_mask.sum())},
        {"Metric": "UZIO employees (Terminated)", "Value": len(both) - int(is_active_mask.sum())},
        {"Metric": "ADP employees compared (unique IDs)", "Value": len(both)},
        {"Metric": "ADP employees with W-4 history (multiple rows)", "Value": len(multi_row_emp_ids)},
        {"Metric": "Mismatch rows (All)", "Value": len(df_miss_all)},
        {"Metric": "Mismatch rows (Active)", "Value": len(df_miss_active)},
        {"Metric": "Mismatch rows (Terminated)", "Value": len(df_miss_term)},
        {"Metric": "Employees with ≥1 mismatch", "Value": df_miss_all["EMPLOYEE_ID"].nunique() if not df_miss_all.empty else 0},
        {"Metric": "Items routed to 'Needs UI Verification'", "Value": len(df_ui_verification)},
        {"Metric": "False positives filtered (no-SIT states, etc.)", "Value": len(df_filtered)},
        {"Metric": "Employees missing in UZIO", "Value": len(missing_in_uzio_ids)},
        {"Metric": "Employees missing in ADP", "Value": len(missing_in_adp_ids)},
    ]

    about_rows = [
        {"Section": "🟢 What you should act on first",
         "Notes": "Review the 'Mismatches (Active)' sheet first. These are real differences between ADP and UZIO that the implementor needs to resolve before the next payroll run. Items where 'VERIFY_IN_UI_FIRST = Yes' have W-4 history; check the UI to confirm which W-4 record is active."},
        {"Section": "🟡 What to verify before acting",
         "Notes": "Review the 'Needs UI Verification' sheet. These flagged items are likely false positives (e.g., 'Do not calculate State Tax' is an internal DB flag and not editable in the Uzio UI). Open the employee in Uzio UI to confirm before changing anything."},
        {"Section": "🔵 What was filtered out and why",
         "Notes": "Review the 'False Positives Filtered' sheet. These are comparisons intentionally skipped — most commonly SIT comparisons for employees in no-SIT states (FL, TX, NV, etc.). No action needed."},
        {"Section": "📜 W-4 history",
         "Notes": "Review 'ADP Effective Date Used'. Any employee with multiple rows had W-4 history — the most recent record was used. Older rows may have outdated values."},
        {"Section": "🔍 Population gaps",
         "Notes": "Review 'Missing in UZIO' (employees in ADP but not in UZIO) and 'Missing in ADP' (employees in UZIO but not in ADP)."},
        {"Section": "⚠️ Cannot detect from CSV alone",
         "Notes": "This tool cannot detect: Home Location states (FL/AL/MS as Home with Work elsewhere), saved-but-unacknowledged W-4 records in Uzio, or phantom records. These require UI screenshot verification."},
    ]

    df_missing_uzio = pd.DataFrame({
        "ASSOCIATE_ID": missing_in_uzio[adp_id_col] if not missing_in_uzio.empty else [],
        "LEGAL_FIRST_NAME": missing_in_uzio[name_col1] if name_col1 and not missing_in_uzio.empty else [],
        "LEGAL_LAST_NAME": missing_in_uzio[name_col2] if name_col2 and not missing_in_uzio.empty else [],
    })

    return {
        "📖 About This Report": about_rows,
        "Summary": metrics,
        "Mismatch Summary": df_sum.to_dict(orient="records"),
        "Mismatches (All)": df_miss_all.to_dict(orient="records"),
        "Mismatches (Active)": df_miss_active.to_dict(orient="records"),
        "Mismatches (Terminated)": df_miss_term.to_dict(orient="records"),
        "Needs UI Verification": df_ui_verification.to_dict(orient="records"),
        "False Positives Filtered": df_filtered.to_dict(orient="records"),
        "Employees with Mismatches": df_emp_sum.to_dict(orient="records"),
        "Field Mapping Rules": df_rules.to_dict(orient="records"),
        "ADP Effective Date Used": date_report.to_dict(orient="records"),
        "Missing in ADP": missing_in_adp_df.to_dict(orient="records"),
        "Missing in UZIO (Sample)": df_missing_uzio.to_dict(orient="records"),
    }
