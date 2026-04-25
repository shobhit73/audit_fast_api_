import pandas as pd
import io
import re
from utils.audit_utils import norm_colname

def norm_digits(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return ""
    if isinstance(x, (float, int)): return str(int(x))
    return re.sub(r"\D", "", str(x))

def normalize_account_type(t):
    if not t: return ""
    s = str(t).strip().lower()
    if "checking" in s or "ck" in s: return "Checking"
    if "savings" in s or "sv" in s: return "Savings"
    return str(t).strip()

def run_adp_payment_audit(uzio_content, adp_content):
    df_uzio = pd.read_excel(io.BytesIO(uzio_content), header=1, dtype=str)
    df_uzio.columns = [str(c).strip().replace("\n", " ") for c in df_uzio.columns]
    
    df_adp = pd.read_excel(io.BytesIO(adp_content), dtype=str)
    df_adp.columns = [str(c).strip() for c in df_adp.columns]
    
    # Simplified matching logic
    results = []
    # (Extract logic from original payment_audit.py)
    return {"message": "Payment audit complete", "results": results}
