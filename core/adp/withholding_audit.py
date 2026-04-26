import pandas as pd
import io
import re
from datetime import datetime

# =====================================================================
# ADP <-> UZIO Withholding Audit Logic (API Version)
# =====================================================================

NO_SIT_STATES = {"FL", "TX", "NV", "WA", "WY", "SD", "AK", "TN", "NH"}

FIELDS_REQUIRING_UI_VERIFICATION = {
    "SIT_WITHHOLDING_EXEMPTION",
}

FILING_STATUS_MAP = {
    "FEDERAL_SINGLE": "Single",
    "FEDERAL_MARRIED": "Married",
    "FEDERAL_MARRIED_SINGLE": "Married but withhold as Single",
    "MD_SINGLE": "Single",
    "MD_MARRIED": "Married",
    "MD_MARRIED_SINGLE": "Married but withhold at single rate",
    "DC_SINGLE": "Single",
    "DC_MARRIED_DP_JOINTLY": "Married/Domestic Partners Filing Jointly",
    "DC_MARRIED_SEPARATELY": "Married Filing Separately",
    "DC_HEAD_OF_HOUSEHOLD": "Head of Household",
    "DC_MARRIED_DP_SEPARATELY": "Married/Domestic Partners Filing Separately",
    "FEDERAL_SINGLE_OR_MARRIED": "Single or Married filing separately",
    "FEDERAL_MARRIED_JOINTLY": "Married filing jointly or Qualifying surviving spouse",
    "FEDERAL_HEAD_OF_HOUSEHOLD": "Head of household",
    "NM_SINGLE": "Single or Married filing separately",
    "NM_MARRIED": "Married filing jointly or Qualifying Surviving Spouse",
    "NM_MARRIED_SINGLE": "Married but withhold as Single",
    "NM_HEAD_OF_HOUSEHOLD": "Head of Household",
    "MS_SINGLE": "Single",
    "MS_HEAD_OF_HOUSEHOLD": "Head of Family",
    "MS_M1": "Married (Spouse NOT employed)",
    "MS_M2": "Married (Spouse is employed)",
    "MO_SINGLE": "Single or Married Spouse Works or Married Filing Separate",
    "MO_MARRIED": "Married (Spouse does not work)",
    "MO_HEAD_OF_HOUSEHOLD": "Head of Household",
    "AL_NO_PERSONAL_EXEMPTION": "No Personal Exemption",
    "AL_SINGLE": "Single",
    "AL_MARRIED": "Married",
    "AL_MARRIED_SEPARATELY": "Married Filing Separately",
    "AL_HEAD_OF_HOUSEHOLD": "Head of Family",
    "DE_MARRIED": "Married",
    "DE_SINGLE": "Single",
    "DE_MARRIED_SINGLE_RATE": "Married but Withhold as Single",
    "OK_MARRIED": "Married",
    "OK_SINGLE": "Single",
    "OK_MARRIED_SINGLE_RATE": "Married but Withhold as Single",
    "OK_NRA": "Non-Resident Alien",
    "NC_HEAD_OF_HOUSEHOLD": "Head of Household",
    "NC_MARRIED": "Married Filing Jointly or Surviving Spouse",
    "NC_SINGLE": "Single or Married Filing Separately",
    "SC_MARRIED_SINGLE_RATE": "Married but Withhold at higher Single Rate",
    "SC_MARRIED": "Married",
    "SC_SINGLE": "Single",
    "UT_SINGLE": "Single or Married filing separately",
    "UT_MARRIED": "Married filing jointly or Qualifying widow(er)",
    "UT_HEAD_OF_HOUSEHOLD": "Head of Household",
    "GA_SINGLE": "Single",
    "GA_SEPARATE_MARRIED_JOINT_BOTH_WORKING": "Married Filing Separate or Married Filing Joint both spouses working",
    "GA_MARRIED_JOINT_ONE_WORKING": "Married Filing Joint one spouse working",
    "GA_HEAD_OF_HOUSEHOLD": "Head of Household",
    "WI_SINGLE": "Single",
    "WI_MARRIED": "Married",
    "WI_MARRIED_SINGLE_RATE": "Married but withhold at higher single rate",
    "KS_SINGLE": "Single",
    "KS_JOINT": "Joint",
    "VT_SINGLE": "Single",
    "VT_MARRIED": "Married/Civil Union Filing Jointly",
    "VT_MARRIED_FILING_SEPERATELY": "Married/Civil Union Filing Separately",
    "VT_MARRIED_SINGLE_RATE": "Married, but withhold at higher single rate",
    "NJ_SINGLE": "Single",
    "NJ_MARRIED_DP_JOINTLY": "Married/Civil Union Couple Joint",
    "NJ_MARRIED_SEPARATELY": "Married/Civil Union Partner Separate",
    "NJ_HEAD_OF_HOUSEHOLD": "Head of Household",
    "NJ_QUALIFIED_WIDOW": "Qualifying Widow(er)/Surviving Civil Union Partner",
    "CA_HEAD_OF_HOUSEHOLD": "Head of Household",
    "CA_MARRIED": "Married (one income)",
    "CA_SINGLE": "Single or Married (with two or more incomes)",
    "MN_SINGLE": "Single, Married but legally separated or Spouse is a nonresident alien",
    "MN_MARRIED": "Married",
    "IA_OTHER": "Other (Including Single)",
    "IA_HEAD_OF_HOUSEHOLD": "Head of Household",
    "IA_MARRIED_JOINTLY": "Married filing jointly",
    "IA_QUALIFIED_SPOUSE": "Qualifying Surviving Spouse",
    "ME_SINGLE": "Single or Head of Household",
    "ME_MARRIED": "Married",
    "ME_MARRIED_SINGLE_RATE": "Married but withhold at higher single rate",
    "ME_NON_RESIDENT_ALIEN": "Nonresident alien",
    "MN_MARRIED_SINGLE_RATE": "Married but withhold at higher single rate",
    "NY_MARRIED_WITHHOLD_SINGLE": "Married but withhold as Single",
    "NY_SINGLE": "Single",
    "NY_MARRIED": "Married",
    "NY_HEAD_OF_HOUSEHOLD": "Head of Household",
    "NE_SINGLE": "Single",
    "NE_MARRIED": "Married Filing Jointly or Qualifying Widow(er)",
    "LA_NO_DEDUCTION": "No Deduction",
    "LA_SINGLE_OR_MARRIED": "Single or married filing separately",
    "LA_MARRIED_FILING_JOINTLY_HOH": "Married filing jointly, qualifying surviving spouse, or head of household",
    "OR_SINGLE": "Single",
    "OR_MARRIED": "Married",
    "OR_MARRIED_SINGLE_RATE": "Married but withhold at higher single rate",
    "ND_SINGLE": "Single",
    "ND_MARRIED": "Married",
    "ND_MARRIED_SINGLE_RATE": "Married but Withhold at higher Single Rate",
    "ND_SINGLE_MARRIED_SEPARATELY": "Single or Married filing separately",
    "ND_HEAD_OF_HOUSEHOLD": "Head of household",
    "ND_MARRIED_JOINTLY": "Married filing jointly  or Qualifying Surviving Spouse",
    "ID_SINGLE": "Single",
    "ID_MARRIED": "Married",
    "ID_MARRIED_SINGLE_RATE": "Married but Withhold at higher Single Rate",
    "CO_SINGLE_OR_MARRIED_SEPARATELY": "Single or Married filing separately",
    "CO_MARRIED_JOINTLY": "Married filing jointly",
    "CO_HEAD_OF_HOUSEHOLD": "Head of household",
    "CO_SINGLE": "Single",
    "CO_MARRIED": "Married",
    "CO_MARRIED_SINGLE_RATE": "Married but Withhold at higher Single Rate",
    "HI_SINGLE": "Single",
    "HI_MARRIED": "Married",
    "HI_MARRIED_SINGLE_RATE": "Married but Withhold at higher single rate",
    "HI_DISABLED": "Certified disabled person",
    "HI_NMS": "Nonresident Military Spouse",
    "MT_SINGLE": "Single or Married filing separately",
    "MT_MARRIED": "Married filing jointly or qualifying surviving spouse",
    "MT_HEAD_OF_HOUSEHOLD": "Head of household",
    "AR_SINGLE": "Single",
    "AR_MARRIED_FILING_JOINTLY": "Married Filing Jointly",
    "AR_HOH": "Head of Household"
}

FIELD_MAPPING = [
    {"UZIO": "employee_id", "ADP": "Associate ID"},
    {"UZIO": "employee_first_name", "ADP": "Legal First Name"},
    {"UZIO": "employee_last_name", "ADP": "Legal Last Name"},
    {"UZIO": "FIT_WITHHOLDING_EXEMPTION", "ADP": "Do Not Calculate Federal Income Tax"},
    {"UZIO": "FIT_ADDL_WITHHOLDING_PER_PAY_PERIOD", "ADP": "Federal Additional Tax Amount"},
    {"UZIO": "FIT_FILING_STATUS", "ADP": "Federal/W4 Marital Status Description"},
    {"UZIO": "FIT_CHILD_AND_DEPENDENT_TAX_CREDIT", "ADP": "Dependents"},
    {"UZIO": "FIT_DEDUCTIONS_OVER_STANDARD", "ADP": "Deductions"},
    {"UZIO": "FIT_HIGHER_WITHHOLDING", "ADP": "Multiple Jobs indicator"},
    {"UZIO": "FIT_OTHER_INCOME", "ADP": "Other Income"},
    {"UZIO": "FIT_WITHHOLD_AS_NON_RESIDENT", "ADP": "Non-Resident Alien"},
    {"UZIO": "FIT_WITHHOLDING_ALLOWANCE", "ADP": "Federal/W4 Exemptions"},
    {"UZIO": "SIT_WITHHOLDING_EXEMPTION", "ADP": "Do not calculate State Tax"},
    {"UZIO": "SIT_FILING_STATUS", "ADP": "State Marital Status Description"},
    {"UZIO": "SIT_TOTAL_ALLOWANCES", "ADP": "State Exemptions/Allowances"},
    {"UZIO": "SIT_ADDL_WITHHOLDING_PER_PAY_PERIOD", "ADP": "State Additional Tax Amount"},
]

MONEY_CENTS_FIELDS = {
    "FIT_ADDL_WITHHOLDING_PER_PAY_PERIOD",
    "FIT_CHILD_AND_DEPENDENT_TAX_CREDIT",
    "FIT_DEDUCTIONS_OVER_STANDARD",
    "FIT_OTHER_INCOME",
    "SIT_ADDL_WITHHOLDING_PER_PAY_PERIOD"
}

def is_active_status(status_str):
    if not status_str:
        return True
    s = str(status_str).lower().strip()
    if s in {"active", "active employee", "a", "act", "active (current)"}:
        return True
    if s.startswith("act"):
        return True
    return False

def _clean(x):
    if pd.isna(x) or x is None:
        return ""
    return str(x).strip()

def _parse_date(d_str):
    if not d_str:
        return pd.NaT
    try:
        return pd.to_datetime(d_str)
    except:
        return pd.NaT

def apply_latest_effective_date(adp_df, emp_id_col):
    if "Federal/W4 Effective Date" not in adp_df.columns:
        adp_df["_eff_date"] = pd.NaT
        return adp_df, pd.DataFrame()

    adp_df["_eff_date"] = adp_df["Federal/W4 Effective Date"].apply(_parse_date)
    adp_df_sorted = adp_df.sort_values([emp_id_col, "_eff_date"], ascending=[True, False], na_position='last')
    adp_df_dedup = adp_df_sorted.drop_duplicates(subset=[emp_id_col], keep="first").copy()
    
    date_report = adp_df_sorted[[emp_id_col, "Legal First Name", "Legal Last Name", "Federal/W4 Effective Date", "_eff_date"]].copy()
    date_report.rename(columns={"_eff_date": "EFF_DATE"}, inplace=True)
    return adp_df_dedup, date_report

def _norm_filing_status(s):
    s = _clean(s).lower()
    return re.sub(r'[\W_]+', ' ', s).strip()

def _norm_bool(s):
    s = str(s).strip().lower()
    if s in {"yes", "y", "true", "1", "on"}:
        return "1"
    if s in {"no", "n", "false", "0", "off"}:
        return "0"
    return ""

def _norm_float(s):
    s = str(s).replace("$", "").replace(",", "").strip()
    if s == "":
        return None
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except:
        return None

def compare_values(uz_key, adp_val_raw, uz_val_raw, filing_status_map):
    araw = _clean(adp_val_raw)
    uraw = _clean(uz_val_raw)

    if "EXEMPTION" in uz_key or "HIGHER_WITHHOLDING" in uz_key:
        ab = _norm_bool(araw)
        ub = _norm_bool(uraw)
        if ab == "" and araw == "": ab = "0"
        if ub == "" and uraw == "": ub = "0"
        match = (ab == ub)
        return match, ab, ub, "bool_blank_false", "ADP Yes/No vs UZIO True/False; blank treated as False"

    if "FILING_STATUS" in uz_key:
        if uraw in filing_status_map:
            u_mapped = filing_status_map[uraw]
        else:
            u_mapped = uraw.split("_", 1)[1].replace("_", " ").title() if "_" in uraw else uraw.title()
        a_n = _norm_filing_status(araw)
        u_n = _norm_filing_status(u_mapped)
        match = (a_n == u_n) or (a_n and a_n in u_n) or (u_n and u_n in a_n)
        return match, a_n, u_n, "filing_status", "UZIO enum mapped to ADP label"

    if uz_key in MONEY_CENTS_FIELDS:
        af = _norm_float(araw)
        uf = _norm_float(uraw)
        af_val = af if af is not None else 0.0
        uf_val = (uf / 100.0) if uf is not None else 0.0
        match = (abs(af_val - uf_val) < 0.01)
        a_out = "0" if af is None and araw=="" else (str(int(af_val)) if af_val.is_integer() else str(af_val))
        u_out = "0" if uf is None and uraw=="" else (str(int(uf_val)) if uf_val.is_integer() else str(uf_val))
        return match, a_out, u_out, "money_cents", "UZIO stored in cents; compared in dollars"

    af = _norm_float(araw)
    uf = _norm_float(uraw)
    af_val = af if af is not None else 0.0
    uf_val = uf if uf is not None else 0.0
    match = (af_val == uf_val)
    a_out = "0" if af is None and araw=="" else (str(int(af_val)) if af_val.is_integer() else str(af_val))
    u_out = "0" if uf is None and uraw=="" else (str(int(uf_val)) if uf_val.is_integer() else str(uf_val))
    return match, a_out, u_out, "int_blank_zero", "Numeric; blank treated as 0"

def run_adp_withholding_audit(uzio_content, adp_content):
    try:
        def read_any(content):
            try:
                return pd.read_excel(io.BytesIO(content), dtype=str)
            except:
                return pd.read_csv(io.BytesIO(content), dtype=str)

        adp_df_raw = read_any(adp_content)
        uzio_df_raw = read_any(uzio_content)

        adp_cols = list(adp_df_raw.columns)
        uzio_cols = list(uzio_df_raw.columns)

        adp_id_col = next((c for c in adp_cols if c.strip().lower() in ["associate id", "employee id", "employee_id", "emp_id"]), adp_cols[0])
        uzio_id_col = next((c for c in uzio_cols if c.strip().lower() in ["employee_id", "employee id", "emp_id"]), uzio_cols[0])

        adp_df_dedup, date_report = apply_latest_effective_date(adp_df_raw, adp_id_col)
        adp_df_dedup[adp_id_col] = adp_df_dedup[adp_id_col].astype(str).apply(_clean)
        
        multi_row_emp_ids = set()
        if not date_report.empty:
            cnt = date_report.groupby(adp_id_col).size()
            multi_row_emp_ids = set(cnt[cnt > 1].index)

        uzio_df_raw[uzio_id_col] = uzio_df_raw[uzio_id_col].astype(str).apply(_clean)
        uzio_key_col = next((c for c in uzio_cols if c.strip().lower() in ["withholding_field_key", "field_key", "key"]), "withholding_field_key")
        uzio_val_col = next((c for c in uzio_cols if c.strip().lower() in ["withholding_field_value", "field_value", "value"]), "withholding_field_value")

        uzio_wide = uzio_df_raw.pivot_table(
            index=uzio_id_col,
            columns=uzio_key_col,
            values=uzio_val_col,
            aggfunc=lambda x: list(x)[-1]
        ).reset_index()

        status_uz_col = next((c for c in uzio_cols if c.strip().lower() == "status"), None)
        priority = ["worker status", "employment status", "associate status", "status description"]
        status_col = next((c for c in adp_cols if c.lower().strip() in priority), None)
        if not status_col:
            status_col = next((c for c in adp_cols if "status" in c.lower() and "marital" not in c.lower() and "tax" not in c.lower()), None)

        adp_state_col = next((c for c in adp_cols if c.strip().lower() in ["worked in state", "state", "work state", "state code"]), None)
        name_col1 = next((c for c in adp_cols if "first" in c.lower()), None)
        name_col2 = next((c for c in adp_cols if "last" in c.lower()), None)

        merg = pd.merge(adp_df_dedup, uzio_wide, left_on=adp_id_col, right_on=uzio_id_col, how="outer", indicator=True)
        both = merg[merg["_merge"] == "both"].copy()

        adp_map = {m["UZIO"]: m["ADP"] for m in FIELD_MAPPING}
        mismatches = []
        ui_verification = []
        filtered = []

        for _, row in both.iterrows():
            emp_id = row[adp_id_col]
            emp_name = f"{row.get(name_col1, '')} {row.get(name_col2, '')}".strip()
            
            raw_status = row.get(status_col, "")
            is_active = is_active_status(raw_status)
            emp_status = "ACTIVE" if is_active else (str(raw_status).upper() if raw_status else "TERMINATED")

            state_code = str(row.get(adp_state_col, "")).strip().upper()
            eff_date = str(row["_eff_date"])[:10] if pd.notna(row["_eff_date"]) else ""
            is_multi = emp_id in multi_row_emp_ids

            for uz_key, adp_col in adp_map.items():
                if adp_col not in both.columns or uz_key not in uzio_wide.columns: continue
                a_raw = row[adp_col] if pd.notna(row[adp_col]) else ""
                u_raw = row[uz_key] if pd.notna(row.get(uz_key)) else ""

                if uz_key.startswith("SIT_") and state_code in NO_SIT_STATES:
                    filtered.append({
                        "EMPLOYEE_ID": emp_id, "EMPLOYEE_NAME": emp_name, "STATE": state_code,
                        "FIELD": uz_key, "REASON": "No SIT state", "ADP_VAL": a_raw, "UZIO_VAL": u_raw
                    })
                    continue

                if uz_key in FIELDS_REQUIRING_UI_VERIFICATION:
                    match, a_n, u_n, _, _ = compare_values(uz_key, a_raw, u_raw, FILING_STATUS_MAP)
                    if not match:
                        ui_verification.append({
                            "EMPLOYEE_ID": emp_id, "EMPLOYEE_NAME": emp_name, "STATE": state_code,
                            "FIELD": uz_key, "ADP_VAL": a_raw, "UZIO_VAL": u_raw,
                            "REASON": "Internal DB flag, verify in UI"
                        })
                    continue

                if uz_key == "SIT_TOTAL_ALLOWANCES" and u_raw == "" and ("SIT_BASIC_ALLOWANCES" in row or "SIT_ADDITIONAL_ALLOWANCES" in row):
                    u_basic = _norm_float(row.get("SIT_BASIC_ALLOWANCES", "")) or 0.0
                    u_addl = _norm_float(row.get("SIT_ADDITIONAL_ALLOWANCES", "")) or 0.0
                    u_raw = str(int(u_basic + u_addl))

                match, a_n, u_n, c_type, rule = compare_values(uz_key, a_raw, u_raw, FILING_STATUS_MAP)
                if not match:
                    mismatches.append({
                        "EMPLOYEE_ID": emp_id, "EMPLOYEE_NAME": emp_name, "STATUS": emp_status, "STATE": state_code,
                        "FIELD": uz_key, "ADP_VAL": a_raw, "UZIO_VAL": u_raw,
                        "ADP_NORM": a_n, "UZIO_NORM": u_n, "RULE": rule, "EFF_DATE": eff_date,
                        "W4_HISTORY": "Yes" if is_multi else "No"
                    })

        return {
            "status": "success",
            "metrics": {
                "total_compared": len(both),
                "mismatches": len(mismatches),
                "ui_verification": len(ui_verification),
                "filtered": len(filtered),
                "missing_in_uzio": len(merg[merg["_merge"] == "left_only"]),
                "missing_in_adp": len(merg[merg["_merge"] == "right_only"])
            },
            "mismatches": mismatches,
            "needs_ui_verification": ui_verification,
            "false_positives_filtered": filtered
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}
