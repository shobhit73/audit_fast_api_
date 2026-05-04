import pandas as pd
import os
from utils.audit_utils import find_header_and_data, format_pay_date

BASE_PATH = r'C:\Users\shobhit.sharma\Downloads\Carvan Prior Payroll Setup'
ADP_FILES = [
    os.path.join(BASE_PATH, 'Payroll_History_Q1_Consolidated.csv'),
    os.path.join(BASE_PATH, 'Copy of Payroll History Q2.csv')
]
UZIO_FILE = os.path.join(BASE_PATH, 'Prior Payroll Register Report_2026-05-02-02-32-42.xlsx')

print("--- ADP Pay Dates ---")
for p in ADP_FILES:
    with open(p, 'rb') as f:
        df, _, _ = find_header_and_data(f.read(), p)
        date_col = next((c for c in df.columns if any(x == str(c).lower().strip() for x in ["pay date", "check date"])), None)
        if date_col:
            dates = df[date_col].apply(format_pay_date).unique()
            print(f"{os.path.basename(p)}: {sorted(dates)}")
        else:
            print(f"{os.path.basename(p)}: No pay date column found")

print("\n--- UZIO Pay Dates ---")
with open(UZIO_FILE, 'rb') as f:
    df, _, _ = find_header_and_data(f.read(), UZIO_FILE)
    date_col = next((c for c in df.columns if any(x == str(c).lower().strip() for x in ["pay date"])), None)
    if date_col:
        dates = df[date_col].apply(format_pay_date).unique()
        print(f"{os.path.basename(UZIO_FILE)}: {sorted(dates)}")
    else:
        print(f"{os.path.basename(UZIO_FILE)}: No pay date column found")
