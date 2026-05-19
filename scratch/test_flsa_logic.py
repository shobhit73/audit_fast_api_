"""
FLSA-rule verification test.

Two phases:
  Phase 1 (isolated): Build a synthetic df_uzio with Job Title / Pay Type* /
                     FLSA Classification pre-populated, then apply the FLSA
                     rule chunk lifted from generate_uzio_template, and verify
                     each row's outcome against an expected value.
  Phase 2 (e2e):     Pipe the same 22 cases through run_adp_census_generation
                     end-to-end and inspect the produced Uzio XLSM.

Run from the audit_fast_api directory:
    python scratch/test_flsa_logic.py
"""
import io
import os
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from utils.audit_utils import is_hourly_only_job_title


# ---------------------------------------------------------------------------
# Test matrix — every row carries (id, job_title, pay_type_src, flsa_src,
# expected_pay_type, expected_flsa, description)
# ---------------------------------------------------------------------------

CASES = [
    # ─── Rule 1: Driver/Hourly-only ALWAYS wins (overwrites source) ─────
    ("T01", "Driver",              "Salary",  "Exempt",     "Hourly",   "Non-Exempt", "Driver+Salary+Exempt -> Driver overrides to Hourly+Non-Exempt"),
    ("T02", "Driver",              "Hourly",  "Non-exempt", "Hourly",   "Non-Exempt", "Driver+Hourly+Non-exempt -> already correct (case-normalized)"),
    ("T03", "Lead Driver",         "",        "",           "Hourly",   "Non-Exempt", "Lead Driver+blank+blank -> Driver rule fires"),
    ("T04", "Walker",              "Salary",  "Exempt",     "Hourly",   "Non-Exempt", "Walker+Salary+Exempt -> Walker is in roster"),
    ("T05", "Helper",              "",        "",           "Hourly",   "Non-Exempt", "Helper+blank+blank -> Helper is in roster"),
    ("T06", "DDU Dedicated",       "Hourly",  "",           "Hourly",   "Non-Exempt", "DDU Dedicated -> Driver rule fires (blank FLSA filled)"),
    ("T07", "DDU Shared",          "Salary",  "Exempt",     "Hourly",   "Non-Exempt", "DDU Shared+Salary+Exempt -> Driver rule overrides"),
    ("T08", "Driver-Lite",         "Hourly",  "",           "Hourly",   "Non-Exempt", "Driver-Lite -> Driver rule fires"),
    ("T09", "Senior Driver",       "Salary",  "Exempt",     "Hourly",   "Non-Exempt", "Senior Driver -> whole-word match on 'driver'"),
    ("T10", "Dog Walker",          "Salary",  "Exempt",     "Hourly",   "Non-Exempt", "Dog Walker -> whole-word match on 'walker'"),
    ("T11", "Delivery Associate",  "Hourly",  "Non-exempt", "Hourly",   "Non-Exempt", "Delivery Associate -> Driver rule fires (values already match)"),
    ("T12", "Driver Helper",       "Salary",  "Exempt",     "Hourly",   "Non-Exempt", "Driver Helper -> whole-word match on both 'driver' and 'helper'"),

    # ─── Whole-word boundary NEGATIVE tests ─────────────────────────────
    ("T13", "Drivership",          "Salary",  "Exempt",     "Salaried", "Exempt",     "Drivership -> NOT a Driver (preserved as Salaried+Exempt)"),
    ("T14", "Sidewalker",          "Hourly",  "",           "Hourly",   "Non-Exempt", "Sidewalker -> NOT Walker (Rule 3 fills blank FLSA)"),

    # ─── Rule 2: blank FLSA + Salaried + not Driver -> Exempt ──────────
    ("T15", "Software Engineer",   "Salary",  "",           "Salaried", "Exempt",     "Rule 2: blank FLSA + Salary -> Exempt"),

    # ─── Rule 3: blank FLSA + Hourly + not Driver -> Non-Exempt ────────
    ("T16", "Cashier",             "Hourly",  "",           "Hourly",   "Non-Exempt", "Rule 3: blank FLSA + Hourly -> Non-Exempt"),

    # ─── Rule 4: cannot determine — leave blank, log manual review ─────
    ("T17", "Software Engineer",   "",        "",           "",         "",           "Rule 4: blank FLSA + blank Pay Type + non-Driver -> leave blank"),
    ("T18", "",                    "",        "",           "",         "",           "Rule 4: everything blank -> leave blank, log cannot determine"),
    ("T19", "Consultant",          "Commission", "",        "Commission","",          "Rule 4: unrecognized Pay Type + blank FLSA -> leave blank"),

    # ─── Source preservation (no overwrite, EVEN when mismatched) ──────
    ("T20", "Software Engineer",   "Salary",  "Non-exempt", "Salaried", "Non-exempt", "Salaried+Non-exempt SOURCE PRESERVED (was overwritten in old code)"),
    ("T21", "Cashier",             "Hourly",  "Exempt",     "Hourly",   "Exempt",     "Hourly+Exempt SOURCE PRESERVED (was overwritten in old code)"),
    ("T22", "Accountant",          "Salary",  "Exempt",     "Salaried", "Exempt",     "Already aligned -> no change, no log"),
]


def apply_flsa_logic(df_uzio, fix_options):
    """Replica of the FLSA chunk from generate_uzio_template — kept in this
    test file so we can exercise the logic against a synthetic df_uzio that
    HAS Job Title populated (the real generate_uzio_template clears Job Title
    early; populating it is the caller's responsibility)."""
    fix_logs = []
    emp_ids = df_uzio['Employee ID*'] if 'Employee ID*' in df_uzio.columns else df_uzio.index

    if 'Pay Type*' not in df_uzio.columns:
        return df_uzio, fix_logs

    # Rule 1 — Driver/Hourly-only always wins.
    if 'Job Title' in df_uzio.columns:
        driver_mask = df_uzio['Job Title'].apply(is_hourly_only_job_title)
        pt_to_fix = driver_mask & ((df_uzio['Pay Type*'].astype(str).str.lower().str.strip() != 'hourly') | df_uzio['Pay Type*'].isna() | (df_uzio['Pay Type*'] == ""))
        for idx in df_uzio[pt_to_fix].index:
            fix_logs.append({
                "Employee": emp_ids[idx], "Field Fixed": "Pay Type*",
                "Original Value": df_uzio.loc[idx, 'Pay Type*'] if pd.notna(df_uzio.loc[idx, 'Pay Type*']) and str(df_uzio.loc[idx, 'Pay Type*']).strip() else "(Blank)",
                "New Value": "Hourly", "Fix Applied": "Forced Hourly for Driver/Hourly-only Position",
            })
        df_uzio.loc[driver_mask, 'Pay Type*'] = "Hourly"

        if 'FLSA Classification' in df_uzio.columns:
            flsa_to_fix = driver_mask & ((df_uzio['FLSA Classification'].astype(str).str.lower().str.strip() != 'non-exempt') | df_uzio['FLSA Classification'].isna() | (df_uzio['FLSA Classification'] == ""))
            for idx in df_uzio[flsa_to_fix].index:
                fix_logs.append({
                    "Employee": emp_ids[idx], "Field Fixed": "FLSA Classification",
                    "Original Value": df_uzio.loc[idx, 'FLSA Classification'] if pd.notna(df_uzio.loc[idx, 'FLSA Classification']) and str(df_uzio.loc[idx, 'FLSA Classification']).strip() else "(Blank)",
                    "New Value": "Non-Exempt", "Fix Applied": "Forced Non-Exempt for Driver/Hourly-only Position",
                })
            df_uzio.loc[driver_mask, 'FLSA Classification'] = "Non-Exempt"
    else:
        driver_mask = pd.Series(False, index=df_uzio.index)

    pay_type_series = df_uzio['Pay Type*'].astype(str).str.lower().str.strip()

    hourly_mask = pay_type_series.str.contains('hour', na=False)
    df_uzio.loc[hourly_mask, 'Pay Type*'] = "Hourly"

    salary_mask = pay_type_series.str.contains('salar', na=False)
    df_uzio.loc[salary_mask, 'Pay Type*'] = "Salaried"

    if fix_options.get('fix_flsa', False) and 'FLSA Classification' in df_uzio.columns:
        blank_flsa_mask = (
            df_uzio['FLSA Classification'].isna()
            | (df_uzio['FLSA Classification'].astype(str).str.strip() == "")
            | (df_uzio['FLSA Classification'].astype(str).str.strip().str.lower() == "nan")
        )

        hourly_fill_mask = blank_flsa_mask & hourly_mask & ~driver_mask
        for idx in df_uzio[hourly_fill_mask].index:
            fix_logs.append({
                "Employee": emp_ids[idx], "Field Fixed": "FLSA Classification",
                "Original Value": "(Blank)", "New Value": "Non-Exempt",
                "Fix Applied": "Filled blank FLSA based on Hourly Pay Type",
            })
        df_uzio.loc[hourly_fill_mask, 'FLSA Classification'] = "Non-Exempt"

        salary_fill_mask = blank_flsa_mask & salary_mask & ~driver_mask
        for idx in df_uzio[salary_fill_mask].index:
            fix_logs.append({
                "Employee": emp_ids[idx], "Field Fixed": "FLSA Classification",
                "Original Value": "(Blank)", "New Value": "Exempt",
                "Fix Applied": "Filled blank FLSA based on Salaried Pay Type",
            })
        df_uzio.loc[salary_fill_mask, 'FLSA Classification'] = "Exempt"

        still_blank_mask = (
            df_uzio['FLSA Classification'].isna()
            | (df_uzio['FLSA Classification'].astype(str).str.strip() == "")
        ) & ~driver_mask
        for idx in df_uzio[still_blank_mask].index:
            pt_cur = df_uzio.loc[idx, 'Pay Type*'] if pd.notna(df_uzio.loc[idx, 'Pay Type*']) and str(df_uzio.loc[idx, 'Pay Type*']).strip() else "(Blank)"
            fix_logs.append({
                "Employee": emp_ids[idx], "Field Fixed": "FLSA Classification",
                "Original Value": "(Blank)", "New Value": "(Blank — Not Filled)",
                "Fix Applied": f"Cannot derive FLSA — source FLSA is blank, Job Title is not in Driver/Hourly-only list, and Pay Type is '{pt_cur}'. Manual review required.",
            })

    return df_uzio, fix_logs


def phase1_isolated():
    print("=" * 80)
    print("PHASE 1 — Isolated FLSA-logic test (synthetic df_uzio with Job Title pre-populated)")
    print("=" * 80)

    rows = []
    for cid, jt, pt, flsa, _, _, _ in CASES:
        rows.append({
            'Employee ID*': cid,
            'Job Title': jt,
            'Pay Type*': pt,
            'FLSA Classification': flsa,
        })
    df_uzio = pd.DataFrame(rows)
    df_uzio, fix_logs = apply_flsa_logic(df_uzio, fix_options={'fix_flsa': True})

    log_by_emp = {}
    for entry in fix_logs:
        log_by_emp.setdefault(entry['Employee'], []).append(entry)

    pad_jt = max(len(c[1]) or 2 for c in CASES) + 2
    pad_pt = 10
    pad_fl = 12
    print(f"\n{'ID':4} {'Job Title':{pad_jt}} {'PT in':{pad_pt}} {'FLSA in':{pad_fl}} | {'PT out':{pad_pt}} {'FLSA out':{pad_fl}} | RESULT  Description")
    print("-" * (4 + pad_jt + pad_pt + pad_fl + pad_pt + pad_fl + 90))

    fails = 0
    for i, (cid, jt, pt_src, flsa_src, exp_pt, exp_flsa, desc) in enumerate(CASES):
        row = df_uzio.iloc[i]
        actual_pt = "" if pd.isna(row['Pay Type*']) else str(row['Pay Type*'])
        actual_flsa = "" if pd.isna(row['FLSA Classification']) else str(row['FLSA Classification'])
        ok = (actual_pt == exp_pt) and (actual_flsa == exp_flsa)
        marker = "OK  " if ok else "FAIL"
        if not ok:
            fails += 1
        print(f"{cid:4} {jt or '(blank)':{pad_jt}} {pt_src or '(blank)':{pad_pt}} {flsa_src or '(blank)':{pad_fl}} | "
              f"{actual_pt or '(blank)':{pad_pt}} {actual_flsa or '(blank)':{pad_fl}} | {marker}    {desc}")
        if not ok:
            print(f"      expected  PT={exp_pt!r:12} FLSA={exp_flsa!r:12}")
            print(f"      actual    PT={actual_pt!r:12} FLSA={actual_flsa!r:12}")

    print()
    print(f"Logs produced: {len(fix_logs)}")
    for cid, _, _, _, _, _, _ in CASES:
        entries = log_by_emp.get(cid, [])
        if entries:
            for e in entries:
                print(f"  [{cid}] {e['Field Fixed']:20} {str(e['Original Value']):14} -> {str(e['New Value']):20} :: {e['Fix Applied']}")
    print()
    print(f"PHASE 1 RESULT: {len(CASES) - fails} / {len(CASES)} passed.")
    if fails:
        print(f"  {fails} FAILURE(S)")
    return fails


def phase2_end_to_end():
    """Run the same cases through run_adp_census_generation to verify the
    full pipeline. Watch closely: if the driver rule fails to fire here
    (but worked in Phase 1), that's a Job-Title-timing bug in
    generate_uzio_template, not a logic bug."""
    print()
    print("=" * 80)
    print("PHASE 2 — End-to-end test through run_adp_census_generation")
    print("=" * 80)

    try:
        from core.adp.census_generator import run_adp_census_generation
        from utils.audit_utils import resolve_uzio_template_path
    except Exception as e:
        print(f"  SKIPPED: cannot import pipeline ({e})")
        return 0

    tpl = resolve_uzio_template_path()
    if not tpl or not os.path.isfile(tpl):
        print(f"  SKIPPED: Uzio template not found (resolve_uzio_template_path -> {tpl})")
        return 0

    rows = []
    for cid, jt, pt, flsa, _, _, _ in CASES:
        rows.append({
            "Associate ID": cid,
            "Legal First Name": "First",
            "Legal Last Name": "Last",
            "Position Status": "Active",
            "Worker Category Description": "Full-Time",
            "Hire/Rehire Date": "2024-01-01",
            "Hire Date": "2024-01-01",
            "Regular Pay Rate Description": pt,
            "Annual Salary": "50000" if "salar" in (pt or "").lower() else "",
            "Regular Pay Rate Amount": "20" if "hour" in (pt or "").lower() else "0",
            "Standard Hours": "40",
            "Job Title Description": jt,
            "Department Description": "Ops",
            "Work Contact: Work Email": f"{cid.lower()}@example.com",
            "Personal Contact: Personal Email": f"{cid.lower()}@personal.com",
            "Tax ID (SSN)": "111-11-1111",
            "Birth Date": "1990-01-01",
            "Gender / Sex (Self-ID)": "M",
            "FLSA Description": flsa,
            "Primary Address: Address Line 1": "1 St",
            "Primary Address: City": "City",
            "Primary Address: Zip / Postal Code": "10001",
            "Primary Address: State / Territory Code": "NY",
            "Reports To Associate ID": "MGR",
            "Location Description": "LOC",
        })
    df = pd.DataFrame(rows)
    bio = io.BytesIO()
    df.to_excel(bio, index=False, engine='openpyxl')

    out_bytes, summary = run_adp_census_generation(
        bio.getvalue(), "test_flsa.xlsx",
        fix_options={"fix_flsa": True},
    )
    out_path = os.path.join(HERE, "test_flsa_e2e_out.xlsm")
    with open(out_path, "wb") as f:
        f.write(out_bytes)
    print(f"  output: {out_path} ({len(out_bytes)} bytes)")
    print(f"  rows_in_source: {summary['rows_in_source']}  rows_in_uzio_output: {summary['rows_in_uzio_output']}")
    print(f"  auto_fix_count: {summary['auto_fix_count']}")

    df_out = pd.read_excel(out_path, sheet_name='Employee Details', header=3, dtype=str)
    df_out.columns = [str(c).replace("\n", " ").strip() for c in df_out.columns]
    df_out = df_out.dropna(how='all').reset_index(drop=True)

    pad_jt = max(len(c[1]) or 2 for c in CASES) + 2
    pad_pt = 10
    pad_fl = 12
    print(f"\n{'ID':4} {'Job Title':{pad_jt}} {'PT in':{pad_pt}} {'FLSA in':{pad_fl}} | {'PT out':{pad_pt}} {'FLSA out':{pad_fl}} | RESULT  Description")
    print("-" * (4 + pad_jt + pad_pt + pad_fl + pad_pt + pad_fl + 90))

    fails = 0
    for i, (cid, jt, pt_src, flsa_src, exp_pt, exp_flsa, desc) in enumerate(CASES):
        match = df_out[df_out['Employee ID*'].astype(str).str.strip() == cid]
        if len(match) == 0:
            print(f"{cid:4} {jt or '(blank)':{pad_jt}} {'?':{pad_pt}} {'?':{pad_fl}} | {'(no row)':{pad_pt}} {'(no row)':{pad_fl}} | MISS  {desc}")
            fails += 1
            continue
        row = match.iloc[0]
        actual_pt = "" if pd.isna(row.get('Pay Type*')) else str(row['Pay Type*']).strip()
        actual_flsa = "" if pd.isna(row.get('FLSA Classification')) else str(row['FLSA Classification']).strip()
        ok = (actual_pt == exp_pt) and (actual_flsa == exp_flsa)
        marker = "OK  " if ok else "FAIL"
        if not ok:
            fails += 1
        print(f"{cid:4} {jt or '(blank)':{pad_jt}} {pt_src or '(blank)':{pad_pt}} {flsa_src or '(blank)':{pad_fl}} | "
              f"{actual_pt or '(blank)':{pad_pt}} {actual_flsa or '(blank)':{pad_fl}} | {marker}    {desc}")
        if not ok:
            print(f"      expected  PT={exp_pt!r:12} FLSA={exp_flsa!r:12}")
            print(f"      actual    PT={actual_pt!r:12} FLSA={actual_flsa!r:12}")

    print()
    print(f"PHASE 2 RESULT: {len(CASES) - fails} / {len(CASES)} passed.")
    if fails:
        print(f"  {fails} FAILURE(S)")
    return fails


def main():
    f1 = phase1_isolated()
    f2 = phase2_end_to_end()
    print()
    print("=" * 80)
    print(f"OVERALL: phase1_fails={f1}, phase2_fails={f2}")
    return 0 if (f1 == 0 and f2 == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
