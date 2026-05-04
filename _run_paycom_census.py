import os, sys, json
from datetime import datetime
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from core.paycom.census_audit import run_paycom_census_audit

UZIO_PATH = r"C:\Users\shobhit.sharma\Downloads\DNI Prior Payroll Setup\Multi_Client_DNI Carriers LLC_Employee_Census.xlsm"
PAYCOM_PATH = r"C:\Users\shobhit.sharma\Downloads\DNI Prior Payroll Setup\20260423095838_Advanced_Report_Writer_9f36c448.xlsx - Report Data.csv"
OUT_DIR = r"C:\Users\shobhit.sharma\Desktop\Audit Files"
os.makedirs(OUT_DIR, exist_ok=True)

with open(UZIO_PATH, "rb") as f:
    uzio_bytes = f.read()
with open(PAYCOM_PATH, "rb") as f:
    paycom_bytes = f.read()

results = run_paycom_census_audit(uzio_bytes, paycom_bytes)

stamp = datetime.now().strftime("%Y%m%d_%H%M")
xlsx_path = os.path.join(OUT_DIR, f"Paycom_Census_Audit_DNI_Carriers_{stamp}.xlsx")

with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
    if isinstance(results, dict):
        for sheet, data in results.items():
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
                df.to_excel(w, sheet_name=str(sheet)[:31], index=False)
            elif isinstance(data, list):
                pd.DataFrame({"(empty)": []}).to_excel(w, sheet_name=str(sheet)[:31], index=False)
    elif isinstance(results, list):
        pd.DataFrame(results).to_excel(w, sheet_name="Results", index=False)

print("Workbook:", xlsx_path)
print()
if isinstance(results, dict):
    for k, v in results.items():
        if isinstance(v, list):
            print(f"  {k}: {len(v)} rows")
        else:
            print(f"  {k}: {type(v).__name__}")
else:
    print(f"  total rows: {len(results)}")
