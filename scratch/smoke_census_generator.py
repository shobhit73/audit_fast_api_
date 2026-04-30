"""Smoke test for the newly-ported census generator tools.

Run from the audit_fast_api directory:
    python scratch/smoke_census_generator.py
"""
import io
import os
import sys
import pandas as pd

# Make sure we import audit_fast_api/utils, not the parent's utils.
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)  # audit_fast_api/
sys.path.insert(0, ROOT)

from utils.audit_utils import resolve_uzio_template_path
from core.adp.census_generator import run_adp_census_generation
from core.paycom.census_generator import run_paycom_census_generation


def _make_adp_xlsx_bytes() -> bytes:
    """Build a minimal in-memory ADP-shaped Census export."""
    df = pd.DataFrame([
        {
            "Associate ID": "ADP001",
            "Legal First Name": "Alice",
            "Legal Last Name": "Walker",
            "Legal Middle Name": "M",
            "Position Status": "Active",
            "Worker Category Description": "Full-Time",
            "Hire/Rehire Date": "2024-01-15",
            "Hire Date": "2022-03-10",
            "Regular Pay Rate Description": "Hourly",
            "Annual Salary": "",
            "Regular Pay Rate Amount": "22.50",
            "Standard Hours": "40",
            "Job Title Description": "Driver",            # Should trigger Hourly + Non-Exempt
            "Department Description": "Operations",
            "Work Contact: Work Email": "alice@example.com",
            "Personal Contact: Personal Email": "alice.personal@example.com",
            "Tax ID (SSN)": "123-45-6789",
            "Birth Date": "1990-05-12",
            "Gender / Sex (Self-ID)": "F",
            "FLSA Description": "",
            "Primary Address: Address Line 1": "1 Main St",
            "Primary Address: City": "Phoenix",
            "Primary Address: Zip / Postal Code": "5001",   # Should be padded to 05001
            "Primary Address: State / Territory Code": "AZ",
            "Reports To Associate ID": "MGR001",
            "Location Description": "PHX-1",
        },
        {
            "Associate ID": "ADP002",
            "Legal First Name": "Bob",
            "Legal Last Name": "Harris",
            "Legal Middle Name": "",
            "Position Status": "Inactive",
            "Worker Category Description": "Part-Time",
            "Hire/Rehire Date": "2023-06-01",
            "Hire Date": "2023-06-01",
            "Termination Date": "",                        # Inactive without term date -> ACTIVE if fix_inactive
            "Regular Pay Rate Description": "Salaried",
            "Annual Salary": "65000",
            "Regular Pay Rate Amount": "0",
            "Standard Hours": "",
            "Job Title Description": "Operations Lead",
            "Department Description": "Operations",
            "Work Contact: Work Email": "",                # Should fall back to personal if fix_emails
            "Personal Contact: Personal Email": "bob.h@example.com",
            "Tax ID (SSN)": "987654321",
            "Birth Date": "1985-09-30",
            "Gender / Sex (Self-ID)": "M",
            "FLSA Description": "",                        # Salaried + fix_flsa -> Exempt
            "Primary Address: Address Line 1": "22 Oak Ave",
            "Primary Address: City": "Tempe",
            "Primary Address: Zip / Postal Code": "852811234",  # Should truncate to 85281
            "Primary Address: State / Territory Code": "AZ",
            "Reports To Associate ID": "MGR001",
            "Location Description": "TMP-1",
        },
        {
            "Associate ID": "ADP003",
            "Legal First Name": "Carol",
            "Legal Last Name": "Doe",
            "Position Status": "Not Hired",                # Should be EXCLUDEd entirely if fix_status
            "Worker Category Description": "Other",
            "Hire/Rehire Date": "2024-02-01",
            "Job Title Description": "Trainer",
            "Department Description": "HR",
            "Tax ID (SSN)": "111-22-3333",
            "Birth Date": "1995-04-22",
            "Gender / Sex (Self-ID)": "Female",
            "Primary Address: City": "Tucson",
            "Primary Address: Zip / Postal Code": "85701",
            "Primary Address: State / Territory Code": "AZ",
            "Location Description": "TUS-1",
        },
    ])
    bio = io.BytesIO()
    df.to_excel(bio, index=False, engine='openpyxl')
    return bio.getvalue()


def _make_paycom_xlsx_bytes() -> bytes:
    df = pd.DataFrame([
        {
            "Employee_Code": "PCM001",
            "Legal_Firstname": "Dana",
            "Legal_Lastname": "Stone",
            "Legal_Middle_Name": "K",
            "Employee_Status": "Active",
            "DOL_Status": "Full-Time",
            "Most_Recent_Hire_Date": "2024-01-10",
            "Hire_Date": "2024-01-10",
            "Pay_Type": "Hourly",
            "Annual_Salary": "",
            "Rate_1": "20.00",
            "Scheduled_Pay_Period_Hours": "40",
            "Position": "Driver-Lite",                     # Driver detection
            "Department_Desc": "Logistics",
            "Work_Email": "",
            "Personal_Email": "dana@example.com",
            "Primary_Phone": "555-0100",
            "SS_Number": "111-22-3333",
            "Birth_Date_(MM/DD/YYYY)": "01/01/1992",
            "Gender": "F",
            "Exempt_Status": "",
            "Primary_Address_Line_1": "5 Pine Rd",
            "Primary_City/Municipality": "Mesa",
            "Primary_Zip/Postal_Code": "85201",
            "Primary_State/Province": "AZ",
            "DriversLicense": "DL12345",
            "DLExpirationDate": "12/31/2026",
            "Work_Location": "MES-1",
        },
        {
            "Employee_Code": "PCM002",
            "Legal_Firstname": "Evan",
            "Legal_Lastname": "Park",
            "Employee_Status": "Inactive",
            "DOL_Status": "",                              # blank + fix_dol_status -> Full Time
            "Most_Recent_Hire_Date": "2023-08-15",
            "Hire_Date": "2023-08-15",
            "Termination_Date": "2025-04-01",
            "Pay_Type": "Salaried",
            "Annual_Salary": "75000",
            "Position": "",                                # blank + fix_position -> "Logistics"
            "Department_Desc": "Logistics",
            "Work_Email": "evan@example.com",
            "SS_Number": "999-88-7777",
            "Birth_Date_(MM/DD/YYYY)": "06/15/1988",
            "Gender": "M",
            "Exempt_Status": "Exempt",
            "Primary_City/Municipality": "Tempe",
            "Primary_Zip/Postal_Code": "5281",             # 4-digit zip -> 05281
            "Primary_State/Province": "AZ",
            "DriversLicense": "",                          # No license -> exp must clear
            "DLExpirationDate": "06/30/2027",
            "Work_Location": "TMP-1",
        },
    ])
    bio = io.BytesIO()
    df.to_excel(bio, index=False, engine='openpyxl')
    return bio.getvalue()


def main():
    tpl = resolve_uzio_template_path()
    print(f"[template] resolved: {tpl}")
    if not tpl:
        print("ABORT: no template found.")
        return 1
    print(f"[template] exists: {os.path.isfile(tpl)}, size={os.path.getsize(tpl)} bytes\n")

    # --- ADP smoke ---
    print("=== ADP run ===")
    adp_bytes = _make_adp_xlsx_bytes()
    adp_fix = {
        "fix_flsa": True, "fix_emails": True, "fix_status": True,
        "fix_inactive": True, "fix_type": True, "fix_zip": True,
        "fix_license": True,
    }
    out_bytes, summary = run_adp_census_generation(adp_bytes, "adp_smoke.xlsx", fix_options=adp_fix)
    out_path = os.path.join(HERE, "smoke_adp_out.xlsm")
    with open(out_path, "wb") as f:
        f.write(out_bytes)
    print(f"  output: {out_path} ({len(out_bytes)} bytes)")
    print(f"  rows_in_source        : {summary['rows_in_source']}")
    print(f"  rows_in_uzio_output   : {summary['rows_in_uzio_output']}  (Carol with 'Not Hired' should be dropped -> 2)")
    print(f"  auto_fix_count        : {summary['auto_fix_count']}")
    print(f"  applied_toggles       : {summary['applied_toggles']}")
    print(f"  unmapped_std_fields[:5]: {summary['unmapped_standard_fields'][:5]}")
    if summary["auto_fix_log_preview"]:
        print(f"  first fix log entry   : {summary['auto_fix_log_preview'][0]}")

    # --- Paycom smoke ---
    print("\n=== Paycom run ===")
    pcm_bytes = _make_paycom_xlsx_bytes()
    pcm_fix = {
        "fix_flsa": True, "fix_emails": True, "fix_status": True,
        "fix_inactive": True, "fix_type": True, "fix_position": True,
        "fix_dol_status": True, "fix_zip": True, "fix_license": True,
    }
    out_bytes, summary = run_paycom_census_generation(pcm_bytes, "paycom_smoke.xlsx", fix_options=pcm_fix)
    out_path = os.path.join(HERE, "smoke_paycom_out.xlsm")
    with open(out_path, "wb") as f:
        f.write(out_bytes)
    print(f"  output: {out_path} ({len(out_bytes)} bytes)")
    print(f"  rows_in_source        : {summary['rows_in_source']}")
    print(f"  rows_in_uzio_output   : {summary['rows_in_uzio_output']}")
    print(f"  auto_fix_count        : {summary['auto_fix_count']}")
    print(f"  applied_toggles       : {summary['applied_toggles']}")
    print(f"  unmapped_std_fields[:5]: {summary['unmapped_standard_fields'][:5]}")
    if summary["auto_fix_log_preview"]:
        print(f"  first fix log entry   : {summary['auto_fix_log_preview'][0]}")

    # --- Verify the output xlsm round-trips and has the expected rows ---
    print("\n=== Verify ADP output by reading 'Employee Details' sheet back ===")
    df_check = pd.read_excel(os.path.join(HERE, "smoke_adp_out.xlsm"), sheet_name='Employee Details', header=3, dtype=str)
    df_check.columns = [str(c).replace("\n", " ").strip() for c in df_check.columns]
    df_check = df_check.dropna(how='all')
    print(f"  rows in output xlsm   : {len(df_check)}  (should be 2)")
    if 'Employee ID*' in df_check.columns:
        print(f"  Employee IDs          : {df_check['Employee ID*'].dropna().tolist()}")
    if 'Pay Type*' in df_check.columns:
        print(f"  Pay Types             : {df_check['Pay Type*'].dropna().tolist()}")
    if 'FLSA Classification' in df_check.columns:
        print(f"  FLSA                  : {df_check['FLSA Classification'].dropna().tolist()}")
    if 'Zipcode*' in df_check.columns:
        print(f"  Zipcodes              : {df_check['Zipcode*'].dropna().tolist()}  (Alice should be 05001, Bob 85281)")
    if 'Official Email*' in df_check.columns:
        print(f"  Official Emails       : {df_check['Official Email*'].dropna().tolist()}  (Bob should fall back to personal)")

    print("\nSMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
