import pandas as pd
import os
import sys
import json
import io

# Add current directory to sys.path
sys.path.append(os.getcwd())

from core.adp.total_comparison import run_adp_total_comparison as mcp_run
from utils.audit_utils import norm_colname

# --- Paths ---
BASE_PATH = r'C:\Users\shobhit.sharma\Downloads\Carvan Prior Payroll Setup'
ADP_FILES = [
    os.path.join(BASE_PATH, 'Payroll_History_Q1_Consolidated.csv'),
    os.path.join(BASE_PATH, 'Copy of Payroll History Q2.csv')
]
UZIO_FILE = os.path.join(BASE_PATH, 'Prior Payroll Register Report_2026-05-02-02-32-42.xlsx')

MAPPINGS = {
    "Earnings": os.path.join(BASE_PATH, 'Payroll Mappings - Earnings Mapping.csv'),
    "Deductions": os.path.join(BASE_PATH, 'Payroll Mappings - Deductions Mapping.csv'),
    "Contributions": os.path.join(BASE_PATH, 'Payroll Mappings - Contributions Mapping.csv'),
    "Taxes": os.path.join(BASE_PATH, 'Payroll_Mappings_Tax_Mapping_CORRECTED.csv')
}

def streamlit_load_mapping(path, cat_name, adp_col, uzio_col):
    if not os.path.exists(path):
        print(f"Warning: {path} not found")
        return []
    df = pd.read_csv(path) if path.lower().endswith('.csv') else pd.read_excel(path)
    df.columns = [norm_colname(c) for c in df.columns]
    
    actual_adp_col = next((c for c in df.columns if adp_col.lower() in c.lower()), None)
    actual_uzio_col = next((c for c in df.columns if uzio_col.lower() in c.lower()), None)
    
    if not actual_adp_col or not actual_uzio_col:
        print(f"Warning: Could not find columns for {cat_name} in {path}")
        return []
        
    mappings = []
    for _, row in df.iterrows():
        a_val = str(row[actual_adp_col]).strip()
        u_val = str(row[actual_uzio_col]).strip()
        if a_val and u_val and a_val.lower() != 'nan' and u_val.lower() != 'nan':
            mappings.append({
                "Category": cat_name,
                "ADP_Name": a_val,
                "UZIO_Name": u_val
            })
    return mappings

print("--- Loading Mappings (Streamlit Style) ---")
all_mappings = []
all_mappings.extend(streamlit_load_mapping(MAPPINGS["Earnings"], "Earnings", "Source Earning Code Name", "Uzio Earning Code Name"))
all_mappings.extend(streamlit_load_mapping(MAPPINGS["Deductions"], "Deductions", "Source Deduction Code Name", "Uzio Deduction Code Name"))
all_mappings.extend(streamlit_load_mapping(MAPPINGS["Contributions"], "Contributions", "Source Contribution Code Name", "Uzio Contribution Code Name"))
all_mappings.extend(streamlit_load_mapping(MAPPINGS["Taxes"], "Taxes", "Source Tax Code Name", "Uzio Tax Code Description"))

print(f"Total mappings loaded: {len(all_mappings)}")

# --- Run MCP Audit ---
print("\n--- Running MCP Audit Logic ---")
adp_data = []
for p in ADP_FILES:
    with open(p, 'rb') as f:
        adp_data.append((f.read(), os.path.basename(p)))

with open(UZIO_FILE, 'rb') as f:
    uzio_data = (f.read(), os.path.basename(UZIO_FILE))

try:
    mcp_results = mcp_run(adp_data, uzio_data, all_mappings)
    
    full_comp = mcp_results["Full Comparison"]
    mismatches = mcp_results["Mismatches Only"]
    
    print(f"MCP Items Checked: {len(full_comp)}")
    print(f"MCP Mismatches: {len(mismatches)}")
    
    if mismatches:
        print("\nTop 5 Mismatches (MCP):")
        for m in mismatches[:5]:
            print(f"  {m['Category']} | {m['UZIO Item']}: ADP={m['ADP Total']}, UZIO={m['UZIO Total']}, Diff={m['Difference']}")
            
    stub_counts = mcp_results.get("Pay Stub Counts", [])
    if stub_counts:
        extra_uzio = [r for r in stub_counts if r["Status"] == "Extra in UZIO"]
        missing_uzio = [r for r in stub_counts if r["Status"] == "Missing in UZIO"]
        print(f"\nPay Stub Count Analysis:")
        print(f"  Total Employees: {len(stub_counts)}")
        print(f"  Extra stubs in UZIO: {len(extra_uzio)}")
        print(f"  Missing stubs in UZIO: {len(missing_uzio)}")
        
        april_missing_adp = 0
        for r in extra_uzio:
            if "2026-04" in r.get("Pay Dates Missing in ADP", ""):
                april_missing_adp += 1
        print(f"  Employees with April stubs in UZIO but not ADP: {april_missing_adp}")

    # Output JSON for easy parsing by Claude
    summary = {
        "mismatches_count": len(mismatches),
        "total_items": len(full_comp),
        "april_gap_employees": april_missing_adp if 'april_missing_adp' in locals() else 0
    }
    with open("audit_summary.json", "w") as f:
        json.dump(summary, f)

except Exception as e:
    import traceback
    print(f"MCP Audit failed: {e}")
    traceback.print_exc()
