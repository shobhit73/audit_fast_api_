"""ADP Consolidated Audit (MCP core).

Pure-Python port of the Streamlit apps/common/adp_combined_audit.py tool. Runs
Census + Direct Deposit (Payment) + Emergency Contact + License audits in one
pass against the Uzio Master / HR Report (CSV) and up to three ADP exports.

Unlike the self-contained Paycom consolidated port, this orchestrator REUSES the
existing audit_fast_api ADP core audits (run_adp_census_audit, run_adp_payment_audit,
run_adp_emergency_audit, run_adp_license_audit). The Uzio HR Report is reshaped into
the per-audit file shapes each core reader expects (the same _adapt_* trick the
Streamlit tool uses), then each audit is run and its sheets are merged under a
vendor-section prefix (CEN_ / DD_ / EC_ / LIC_).

The orchestrator run_adp_consolidated_audit returns a dict-of-lists keyed by sheet
name, ready for save_results_to_excel to render.
"""

import io
import pandas as pd

from core.common.paycom_consolidated_audit import read_uzio_master
from core.adp.census_audit import run_adp_census_audit
from core.adp.payment_audit import run_adp_payment_audit
from core.adp.misc_audits import run_adp_emergency_audit, run_adp_license_audit

# ---------------------------------------------------------------------------
# Column maps: Uzio HR Report "Section|Field" -> the column name the matching
# ADP core audit's reader expects. Identical to the Streamlit tool's maps.
# ---------------------------------------------------------------------------

_CENSUS_COL_MAP = {
    "Job|Employee ID": "Employee ID*",
    "Personal|First Name": "Employee First Name*",
    "Personal|Last Name": "Employee Last Name*",
    "Personal|Middle Name": "Employee Middle Initial",
    "Personal|Suffix": "Employee Suffix",
    "Job|Status": "Employment Status*",
    "Job|Date of Hire": "Date of Hire*",
    "Job|Original DOH": "Original DOH",
    "Job|Termination Date": "Termination Date",
    "Job|Termination Reason": "Termination Reason",
    "Job|Employment Type": "Employment Type*",
    "Job|Pay Type": "Pay Type*",
    "Job|Annual Salary": "Annual Salary(Digits)**",
    "Job|Hourly Rate": "Hourly Pay Rate**",
    "Job|Working Hours per Week": "Working Hours per Week(Digits)**",
    "Job|Job Title": "Job Title",
    "Job|Department": "Department",
    "Personal|Work Email": "Official Email*",
    "Home Address|Personal Email": "Personal Email",
    "Home Address|Phone": "Phone Number(Digits)",
    "Personal|SSN": "Employee SSN",
    "Personal|Date Of Birth": "Employee Date of Birth*",
    "Personal|Gender": "Employee Gender*",
    "Personal|Tobacco Usage": "Employee Tobacco usage in last 12 months",
    "Job|FLSA Classification": "FLSA Classification",
    "Home Address|Address Line 1": "Employee Address Line 1",
    "Home Address|Address Line 2": "Employee Address Line 2",
    "Home Address|City": "City*",
    "Home Address|Zip": "Zipcode*",
    "Home Address|State": "State(Abbreviation)*",
    "Mailing Address|Address Line 1": "Mailing Address Line 1",
    "Mailing Address|Address Line 2": "Mailing Address Line 2",
    "Mailing Address|City": "Mailing City",
    "Mailing Address|Zip": "Mailing Zipcode",
    "Mailing Address|State": "Mailing State(Abbreviation)",
    "Job|Reporting Manager": "Reporting Manager ID",
    "Job|Work Location": "Work Location",
    "Additional Information|License Number": "License Number*",
    "Additional Information|License Expiration Date": "License Expiration Date",
}

_PAYMENT_COL_MAP = {
    "Job|Employee ID": "Employee ID",
    "Payment Method|Routing Number": "Routing Number",
    "Payment Method|Account Number": "Account Number",
    "Payment Method|Account Type": "Account Type",
    "Payment Method|Paycheck Percentage": "Paycheck Percentage",
    "Payment Method|Paycheck Amount": "Paycheck Amount",
}

_EMERGENCY_COL_MAP = {
    "Job|Employee ID": "Employee ID",
    "Emergency Contact|Name": "Name",
    "Emergency Contact|Relationship": "Relationship",
    "Emergency Contact|Phone": "Phone",
}

_LICENSE_COL_MAP = {
    "Job|Employee ID": "Employee ID",
    "Additional Information|License Number": "License Number",
    "Additional Information|License Expiration Date": "License Expiration Date",
}


# ---------------------------------------------------------------------------
# Reporting-manager name -> Employee ID resolver (ported from the Streamlit
# paycom_combined_audit.py). The HR Report's 'Reporting Manager' is a NAME, but
# the ADP census compares it as an ID (Reports To Associate ID). Resolving the
# name against the roster makes the comparison ID-vs-ID instead of name-vs-ID.
# ---------------------------------------------------------------------------

def _norm_person_name(s):
    return " ".join(str(s).strip().casefold().split())


def _first_last_name(s):
    toks = _norm_person_name(s).split()
    return f"{toks[0]} {toks[-1]}" if len(toks) >= 2 else _norm_person_name(s)


def build_manager_name_to_id(df):
    """{full-name -> Employee ID} + {first-last -> Employee ID} resolver built from
    the roster. Ambiguous names (same name, different IDs) are dropped."""
    if df is None or df.empty:
        return {"full": {}, "fl": {}}
    name_col = next((c for c in df.columns if str(c).endswith("|Full Name")), None)
    id_col = next((c for c in df.columns if str(c).endswith("|Employee ID")), None)
    if not name_col or not id_col:
        return {"full": {}, "fl": {}}
    full, fl, full_dupes, fl_dupes = {}, {}, set(), set()
    for nm, eid in zip(df[name_col], df[id_col]):
        eid = str(eid).strip() if pd.notna(eid) else ""
        if not eid or pd.isna(nm) or not str(nm).strip():
            continue
        k = _norm_person_name(nm)
        if k in full and full[k] != eid:
            full_dupes.add(k)
        else:
            full[k] = eid
        k2 = _first_last_name(nm)
        if k2 in fl and fl[k2] != eid:
            fl_dupes.add(k2)
        else:
            fl[k2] = eid
    for k in full_dupes:
        full.pop(k, None)
    for k in fl_dupes:
        fl.pop(k, None)
    return {"full": full, "fl": fl}


def resolve_manager_id(name, resolver):
    if not resolver or pd.isna(name) or not str(name).strip():
        return ""
    hit = resolver.get("full", {}).get(_norm_person_name(name))
    if hit:
        return hit
    return resolver.get("fl", {}).get(_first_last_name(name), "")


# ---------------------------------------------------------------------------
# Uzio HR Report -> per-audit file-shape adapters (return raw .xlsx bytes).
# ---------------------------------------------------------------------------

def _full_name_series(df_master):
    fn = df_master.get("Personal|First Name", pd.Series([""] * len(df_master))).fillna("").astype(str)
    ln = df_master.get("Personal|Last Name", pd.Series([""] * len(df_master))).fillna("").astype(str)
    return (fn + " " + ln).str.strip()


def _project_and_rename(df_master, col_map):
    out = pd.DataFrame()
    for src, dst in col_map.items():
        out[dst] = df_master[src] if src in df_master.columns else ""
    return out


def _to_xlsx_bytes(df, sheet_name="Sheet1", startrow=0):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=startrow, header=True)
    return buf.getvalue()


def _adapt_census(df_master):
    """Sheet 'Employee Details', header on row 4 — what read_uzio_raw_file expects."""
    df = _project_and_rename(df_master, _CENSUS_COL_MAP)
    if "Reporting Manager ID" in df.columns:
        resolver = build_manager_name_to_id(df_master)
        df["Reporting Manager ID"] = df["Reporting Manager ID"].map(
            lambda nm: resolve_manager_id(nm, resolver))
    df = df.drop_duplicates(subset=["Employee ID*"], keep="first").reset_index(drop=True)
    if "Job|Employee ID" in df_master.columns:
        df["Full Name"] = _full_name_series(
            df_master.drop_duplicates(subset=["Job|Employee ID"], keep="first").reset_index(drop=True))
    return _to_xlsx_bytes(df, sheet_name="Employee Details", startrow=3)


def _adapt_payment(df_master):
    """Header on row 2 — what run_adp_payment_audit's smart_read_df(header=1) expects.
    Multi-row preserved; rows with no banking data dropped."""
    df = _project_and_rename(df_master, _PAYMENT_COL_MAP)
    df["Full Name"] = _full_name_series(df_master)
    routing = df["Routing Number"].fillna("").astype(str).str.strip().ne("")
    account = df["Account Number"].fillna("").astype(str).str.strip().ne("")
    df = df[routing | account].reset_index(drop=True)
    return _to_xlsx_bytes(df, startrow=1)


def _adapt_emergency(df_master):
    """Header on row 2 — what run_adp_emergency_audit's smart_read_df(header=1) expects.
    Multi-row preserved; rows with no contact data dropped."""
    df = _project_and_rename(df_master, _EMERGENCY_COL_MAP)
    name_filled = df["Name"].fillna("").astype(str).str.strip().ne("")
    phone_filled = df["Phone"].fillna("").astype(str).str.strip().ne("")
    df = df[name_filled | phone_filled].reset_index(drop=True)
    return _to_xlsx_bytes(df, startrow=1)


def _adapt_license(df_master):
    """Header on row 1 — run_adp_license_audit scans the first 20 rows for an
    'Employee ID' header cell. Multi-row preserved; rows with no license dropped."""
    df = _project_and_rename(df_master, _LICENSE_COL_MAP)
    df["Full Name"] = _full_name_series(df_master)
    has_lic = df["License Number"].fillna("").astype(str).str.strip().ne("")
    df = df[has_lic].reset_index(drop=True)
    return _to_xlsx_bytes(df, startrow=0)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

# Census roll-up sheets are dropped from the consolidated workbook (the detail and
# anomaly sheets carry everything); mirrors the Streamlit tool's no-summary output.
_CENSUS_SKIP = {"Summary_Metrics"}
_PAYMENT_SKIP = {"Summary"}
_EMERGENCY_SKIP = {"Summary"}


def _merge(sheets, prefix, result, skip):
    """Merge a sub-audit's dict-of-lists into `sheets` with a section prefix.
    Sheet names are capped at Excel's 31-char limit."""
    if not isinstance(result, dict):
        return
    for key, val in result.items():
        if key in skip:
            continue
        sheets[f"{prefix}{key}"[:31]] = val


def run_adp_consolidated_audit(uzio_content, adp_census_content=None,
                               adp_dd_content=None, adp_em_content=None):
    """End-to-end ADP Consolidated Audit. Returns a dict-of-lists for
    save_results_to_excel.

    The Uzio HR Report is required. Each ADP file is optional — the matching audit
    runs only if its file is supplied (the Emergency + License Details report drives
    BOTH the Emergency Contact and License audits). A failure in one audit is captured
    in the _Errors sheet and does not abort the others.

    Sheets: CEN_* (census detail + anomalies), DD_* (direct deposit), EC_* (emergency
    contact), LIC_* (license), plus _Errors if any sub-audit failed.
    """
    df_master = read_uzio_master(uzio_content)
    sheets = {}
    errors = []

    if adp_census_content:
        try:
            _merge(sheets, "CEN_", run_adp_census_audit(_adapt_census(df_master), adp_census_content), _CENSUS_SKIP)
        except Exception as exc:
            errors.append({"Audit": "Census", "Error": f"{type(exc).__name__}: {exc}"})

    if adp_dd_content:
        try:
            _merge(sheets, "DD_", run_adp_payment_audit(_adapt_payment(df_master), adp_dd_content), _PAYMENT_SKIP)
        except Exception as exc:
            errors.append({"Audit": "Direct Deposit", "Error": f"{type(exc).__name__}: {exc}"})

    if adp_em_content:
        try:
            _merge(sheets, "EC_", run_adp_emergency_audit(_adapt_emergency(df_master), adp_em_content), _EMERGENCY_SKIP)
        except Exception as exc:
            errors.append({"Audit": "Emergency Contact", "Error": f"{type(exc).__name__}: {exc}"})
        try:
            _merge(sheets, "LIC_", run_adp_license_audit(_adapt_license(df_master), adp_em_content), set())
        except Exception as exc:
            errors.append({"Audit": "License", "Error": f"{type(exc).__name__}: {exc}"})

    if errors:
        sheets["_Errors"] = errors
    return sheets
