import pandas as pd
import io
import re
from utils.audit_utils import norm_colname

FILING_STATUS_MAP = {
    "FEDERAL_SINGLE": "Single",
    "FEDERAL_MARRIED": "Married",
    # ... (Keep the map from the original file) ...
}

def run_adp_withholding_audit(uzio_content, adp_content):
    # Simplified logic
    adp_df = pd.read_excel(io.BytesIO(adp_content), dtype=str)
    uzio_df = pd.read_excel(io.BytesIO(uzio_content), dtype=str)
    
    # Matching logic...
    results = []
    return {"message": "Withholding audit complete", "results": results}
