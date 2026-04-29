import sys
import os
sys.path.append(os.getcwd())
from core.paycom.census_audit import run_paycom_census_audit
import pandas as pd

paycom_path = r'C:\Users\shobhit.sharma\Downloads\DNI Carriers\DNI Carriers Paycom Census.xlsx'
uzio_path = r'C:\Users\shobhit.sharma\Downloads\DNI Carriers\Multi_Client_DNI Carriers LLC_Employee_Census.xlsm'

with open(paycom_path, 'rb') as f: paycom_content = f.read()
with open(uzio_path, 'rb') as f: uzio_content = f.read()

results = run_paycom_census_audit(uzio_content, paycom_content)
print("--- AUDIT RESULTS ---")
for m in results['Summary_Metrics']:
    print(f"{m['Metric']}: {m['Value']}")
print("---------------------")
