import pandas as pd
import os
import sys
import json
import io
from datetime import datetime

# Add current directory to sys.path
sys.path.append(os.getcwd())

from core.adp.total_comparison import run_adp_total_comparison
# from mcp_server import save_results_to_excel, load_mappings_from_paths

# --- Paths ---
BASE_PATH = r'C:\Users\shobhit.sharma\Downloads\Carvan Prior Payroll Setup'
ADP_FILES = [
    os.path.join(BASE_PATH, 'Payroll_History_Q1_Consolidated.csv'),
    os.path.join(BASE_PATH, 'Copy of Payroll History Q2.csv')
]
UZIO_FILE = os.path.join(BASE_PATH, 'Prior Payroll Register Report_2026-05-02-02-32-42.xlsx')

MAPPING_PATHS = [
    os.path.join(BASE_PATH, 'Payroll Mappings - Earnings Mapping.csv'),
    os.path.join(BASE_PATH, 'Payroll Mappings - Deductions Mapping.csv'),
    os.path.join(BASE_PATH, 'Payroll Mappings - Contributions Mapping.csv'),
    os.path.join(BASE_PATH, 'Payroll_Mappings_Tax_Mapping_CORRECTED.csv')
]

# --- LOAD DATA ---
adp_data = []
for p in ADP_FILES:
    with open(p, 'rb') as f:
        adp_data.append((f.read(), os.path.basename(p)))

with open(UZIO_FILE, 'rb') as f:
    uzio_data = (f.read(), os.path.basename(UZIO_FILE))

# --- LOAD MAPPINGS ---
# We'll use the fixed version of load_mappings_from_paths (which we just edited in mcp_server.py)
# Since we can't import mcp_server due to missing 'mcp' module, we'll inline the helper here.

def load_mappings_inlined(paths):
    mappings = []
    for p in paths:
        p = p.strip().strip('"')
        if not os.path.isfile(p): continue
        cat = "Earnings"
        fname = os.path.basename(p).lower()
        if "deduction" in fname: cat = "Deductions"
        elif "contribution" in fname: cat = "Contributions"
        elif "tax" in fname: cat = "Taxes"
        df = pd.read_csv(p) if p.lower().endswith('.csv') else pd.read_excel(p)
        from utils.audit_utils import norm_colname
        s_col = next((c for c in df.columns if "source" in str(c).lower() and "name" in str(c).lower()), None)
        u_col = next((c for c in df.columns if "uzio" in str(c).lower() and ("name" in str(c).lower() or "description" in str(c).lower())), None)
        if s_col and u_col:
            for _, row in df.iterrows():
                mappings.append({
                    "Category": cat,
                    "ADP_Name": str(row[s_col]).strip(),
                    "UZIO_Name": str(row[u_col]).strip()
                })
    return mappings

mappings = load_mappings_inlined(MAPPING_PATHS)

# --- RUN AUDIT ---
results = run_adp_total_comparison(adp_data, uzio_data, mappings)

# --- SAVE TO EXCEL ---
# Inline save_results_to_excel logic to avoid mcp dependency
AUDIT_INBOX = r"C:\Users\shobhit.sharma\Desktop\Audit Files"
if not os.path.exists(AUDIT_INBOX):
    os.makedirs(AUDIT_INBOX, exist_ok=True)

stamp = datetime.now().strftime("%Y%m%d_%H%M")
filename = f"Carvan_MCP_Audit_{stamp}.xlsx"
out_path = os.path.join(AUDIT_INBOX, filename)

with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
    for sheet_name, data in results.items():
        if data:
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

print(f"Audit completed successfully.")
print(f"Mismatches: {len(results['Mismatches Only'])}")
print(f"Report saved to: {out_path}")
