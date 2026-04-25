import pandas as pd
import io
import re
from datetime import datetime
import numpy as np

def norm_text(val) -> str:
    if pd.isna(val): return ""
    s = str(val).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def upper_clean(val) -> str:
    return norm_text(val).upper()

def yes_no(val) -> str:
    s = upper_clean(val)
    if s in {"Y", "YES", "TRUE", "1"}: return "Yes"
    if s in {"N", "NO", "FALSE", "0"}: return "No"
    return s.title() if s else ""

def is_open_ended_date(val) -> bool:
    if pd.isna(val): return False
    s = str(val).strip()
    if not s: return False
    s_nospace = s.replace(" ", "")
    zero_patterns = {"0000", "00/00/0000", "0/0/0000", "00-00-0000", "0-0-0000", "00/00/00", "0/0/00"}
    return s_nospace in zero_patterns or bool(re.search(r"(^|\D)0{4}(\D|$)", s_nospace)) or s_nospace.startswith("00/") or s_nospace.endswith("/0000")

def parse_date_safe(val) -> pd.Timestamp:
    if pd.isna(val) or is_open_ended_date(val): return pd.NaT
    s = str(val).strip()
    return pd.to_datetime(s, errors="coerce") if s else pd.NaT

def classify_item(type_code: str, description: str) -> str:
    code, desc = upper_clean(type_code), upper_clean(description)
    if code in {"NSD", "PFL"} or desc in {"NEW YORK SDI", "NY PAID FAMILY LEAVE"}: return "Tax / Statutory Payroll Item"
    if any(k in desc for k in [" MATCH", "MEMO", "EMPLOYER MEMO", "ER MEMO"]): return "Contribution"
    if any(k in desc for k in ["WITHHOLDING TAX", "SOCIAL SECURITY", "MEDICARE", "FUTA", "SUTA", "WORKERS COMPENSATION", "STATE W/H", "LOCAL", "SIT", "FWT"]): return "Tax / Statutory Payroll Item"
    if any(k in desc for k in ["MEDICAL", "DENTAL", "VISION", "401K", "ROTH", "LOAN", "AD&D", "STD", "VOL EE LIFE", "SUPPORT ORDER", "GARNISH", "EARNED WAGE ACCESS", "HEALTHCUES", "REIMBURSE", "OVERPAYMENT"]): return "Deduction"
    return "Review"

def run_paycom_deduction_analysis(scheduled_content, prior_content, config_content=None):
    # This is a simplified version of the logic
    df_sched = pd.read_excel(io.BytesIO(scheduled_content))
    df_prior = pd.read_excel(io.BytesIO(prior_content))
    
    # ... logic to process both files ...
    # For now, return a basic summary to prove the concept
    return {
        "scheduled_rows": len(df_sched),
        "prior_rows": len(df_prior),
        "message": "Deduction analysis complete (Simplified logic for demonstration)"
    }
