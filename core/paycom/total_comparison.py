import pandas as pd
import io
import re
from utils.audit_utils import clean_money_val, norm_colname, normalize_id, format_pay_date

def parse_paycom_filename_date(filename):
    match = re.findall(r'(\d{8})', str(filename))
    if len(match) >= 3:
        d = match[2]
        return f"{d[4:]}-{d[:2]}-{d[2:4]}"
    return "Unknown"

def find_header_and_data_uzio(file_content):
    """Find the data row in a Uzio export file (bytes)."""
    try:
        xls = pd.ExcelFile(io.BytesIO(file_content))
        target_sheet = xls.sheet_names[0]
        if len(xls.sheet_names) > 1 and "criteria" in xls.sheet_names[0].lower():
            target_sheet = xls.sheet_names[1]
        df_peek = pd.read_excel(xls, sheet_name=target_sheet, header=None, nrows=50)
        header_idx = 0
        for i, row in df_peek.iterrows():
            row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
            if "employee id" in row_str or "employee name" in row_str:
                header_idx = i; break
        df = pd.read_excel(xls, sheet_name=target_sheet, header=header_idx)
        header_top = df_peek.iloc[header_idx - 1].tolist() if header_idx > 0 else None
        return df, header_top
    except Exception:
        df = pd.read_csv(io.BytesIO(file_content), header=None, nrows=50)
        header_idx = 0
        for i, row in df.iterrows():
            row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
            if "employee id" in row_str or "employee name" in row_str:
                header_idx = i; break
        df = pd.read_csv(io.BytesIO(file_content), header=header_idx)
        return df, None

def find_header_and_data_paycom(file_content, filename):
    """Find the data row in a Paycom export file (bytes)."""
    fname = str(filename).lower()
    if fname.endswith('.csv'):
        df_peek = pd.read_csv(io.BytesIO(file_content), header=None, nrows=20)
        header_idx = 0
        for i, row in df_peek.iterrows():
            row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
            if any(kw in row_str for kw in ["ee code", "description", "earning", "amount", "row labels"]):
                header_idx = i; break
        return pd.read_csv(io.BytesIO(file_content), header=header_idx), None
    
    xls = pd.ExcelFile(io.BytesIO(file_content))
    df_peek = pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None, nrows=10)
    header_idx = 0
    for i, row in df_peek.iterrows():
        row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
        if any(kw in row_str for kw in ["ee code", "description", "earning", "amount", "row labels"]):
            header_idx = i; break
    return pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=header_idx), None

def calculate_totals_uzio(df, header_top, column_names):
    """Sum up values for Uzio columns (wide format)."""
    found_cols = []
    emp_tots = {}
    id_aliases = ["employee id", "file #", "associate id", "ee code"]
    date_aliases = ["pay date", "check date", "period end"]
    id_col = next((c for c in df.columns if any(x in str(c).lower() for x in id_aliases)), None)
    date_col = next((c for c in df.columns if any(x in str(c).lower() for x in date_aliases)), None)

    if id_col:
        df_clean = df[df[id_col].notna()].copy()
        df_clean[id_col] = df_clean[id_col].apply(normalize_id)
        df_clean = df_clean[~df_clean[id_col].str.lower().str.contains("total|grand", na=False)]
    else:
        df_clean = df.copy()

    norm_cols_main = {norm_colname(c).lower(): i for i, c in enumerate(df.columns)}
    norm_cols_top = {}
    if header_top:
        for i, c in enumerate(header_top):
            if pd.notna(c) and str(c).strip() != "":
                norm_cols_top[norm_colname(c).lower()] = i

    cols_to_sum = []
    for name in column_names:
        n_name = norm_colname(name).lower()
        if n_name in norm_cols_main:
            idx = norm_cols_main[n_name]
            cols_to_sum.append(df.columns[idx])
            found_cols.append(df.columns[idx])
        elif n_name in norm_cols_top:
            start_idx = norm_cols_top[n_name]
            end_idx = len(df.columns)
            if header_top:
                for k in range(start_idx + 1, len(header_top)):
                    if pd.notna(header_top[k]) and str(header_top[k]).strip() != "":
                        end_idx = k; break
            for k in range(start_idx, end_idx):
                main_h = str(df.columns[k]).lower()
                if any(x in main_h for x in ['amount', 'total', 'current', 'ee', 'er', 'tax']):
                    if not any(x in main_h for x in ['wages', 'hours', 'rate', 'basis', 'taxable']):
                        cols_to_sum.append(df.columns[k])
                        found_cols.append(str(df.columns[k]))

    for _, row in df_clean.iterrows():
        eid = row[id_col] if id_col else "Summary"
        pay_date = format_pay_date(row[date_col]) if date_col else "Unknown"
        row_tot = sum(clean_money_val(row[c]) for c in set(cols_to_sum))
        key = (eid, pay_date)
        emp_tots[key] = emp_tots.get(key, 0.0) + row_tot

    return sum(emp_tots.values()), found_cols, emp_tots

def calculate_totals_paycom(df, mapping_source_names, filename, uzio_item_name=""):
    """Sum up values for Paycom (long format)."""
    found_items = set()
    emp_tots = {}
    id_aliases = ["ee code", "employee code", "file #", "clock #", "associate id"]
    desc_aliases = ["type description", "description", "earning/deduction/tax", "code description", "row labels"]
    id_col = next((c for c in df.columns if any(x in str(c).lower() for x in id_aliases)), None)
    desc_col = next((c for c in df.columns if any(x in str(c).lower() for x in desc_aliases)), None)
    code_desc_col = next((c for c in df.columns if "code description" in str(c).lower()), None)
    amt_col = next((c for c in df.columns if "current amount" in str(c).lower()), None)
    if not amt_col:
        amt_col = next((c for c in df.columns if any(x in str(c).lower() for x in ["amount", "total amount", "value", "sum of amount"])), None)
    if not desc_col or not amt_col:
        return 0.0, [], {}

    pay_date = parse_paycom_filename_date(filename)
    norm_mappings = [n.lower().strip() for n in mapping_source_names]

    for _, row in df.iterrows():
        raw_desc = str(row[desc_col]).strip()
        val_desc = raw_desc.lower()
        if val_desc not in norm_mappings:
            continue
        if "medicare" in val_desc or "social security" in val_desc or "ssc" in val_desc:
            if code_desc_col and pd.notna(row.get(code_desc_col)):
                code_desc_val = str(row[code_desc_col]).strip().lower()
                is_employer_tax = "employer" in uzio_item_name.lower() or "er " in uzio_item_name.lower()
                if is_employer_tax and "client side" not in code_desc_val:
                    continue
                if not is_employer_tax and "w/h" not in code_desc_val:
                    continue
        eid = normalize_id(row[id_col]) if id_col else "Summary"
        amount = clean_money_val(row[amt_col])
        key = (eid, pay_date)
        emp_tots[key] = emp_tots.get(key, 0.0) + amount
        found_items.add(raw_desc)

    return sum(emp_tots.values()), list(found_items), emp_tots

def run_paycom_total_comparison(paycom_files_data, uzio_file_data, mappings):
    """
    Full production-grade Paycom total comparison — 3 sheets matching apps/paycom/total_comparison.py.
    paycom_files_data: list of (content_bytes, filename)
    uzio_file_data: (content_bytes, filename)
    mappings: list of dicts with Category, Source_Name, UZIO_Name
    """
    uzio_content, uzio_fname = uzio_file_data
    df_uzio, uzio_top = find_header_and_data_uzio(uzio_content)

    results = []
    employee_mismatches = []
    
    # --- Global Employee Collection ---
    global_emp_p = {} # {eid: total}
    global_emp_u = {} # {eid: total}

    paycom_data_list = []
    for content, fname in paycom_files_data:
        df_p, _ = find_header_and_data_paycom(content, fname)
        paycom_data_list.append((df_p, fname))

    unique_uzio_items = {}
    for m in mappings:
        u_name = m["UZIO_Name"]
        if u_name not in unique_uzio_items:
            unique_uzio_items[u_name] = {"Category": m.get("Category", ""), "Source_Names": []}
        unique_uzio_items[u_name]["Source_Names"].append(m["Source_Name"])

    for u_name, data in unique_uzio_items.items():
        cat = data["Category"]
        source_names = data["Source_Names"]

        paycom_total = 0.0
        paycom_items_found = []
        paycom_emp_detail = {}

        for df_p, fname in paycom_data_list:
            tot, found, emp_m = calculate_totals_paycom(df_p, source_names, fname, u_name)
            paycom_total += tot
            for f in found:
                if f not in paycom_items_found: paycom_items_found.append(f)
            for (eid, p_date), v in emp_m.items():
                if eid not in paycom_emp_detail: paycom_emp_detail[eid] = {}
                paycom_emp_detail[eid][p_date] = paycom_emp_detail[eid].get(p_date, 0.0) + v
                # Aggregate globally
                global_emp_p[eid] = global_emp_p.get(eid, 0.0) + v

        uzio_total, uzio_cols, uzio_emp_m = calculate_totals_uzio(df_uzio, uzio_top, [u_name])
        uzio_emp_detail = {}
        for (eid, p_date), v in uzio_emp_m.items():
            if eid not in uzio_emp_detail: uzio_emp_detail[eid] = {}
            uzio_emp_detail[eid][p_date] = uzio_emp_detail[eid].get(p_date, 0.0) + v
            # Aggregate globally
            global_emp_u[eid] = global_emp_u.get(eid, 0.0) + v

        diff = uzio_total - paycom_total
        status = "Match" if abs(diff) <= 0.02 else "Mismatch"

        results.append({
            "Category": cat,
            "UZIO Item": u_name,
            "Paycom Total": round(paycom_total, 2),
            "UZIO Total": round(uzio_total, 2),
            "Difference": round(diff, 2),
            "Status": status,
            "Paycom Items Found": ", ".join(paycom_items_found) if paycom_items_found else "None",
            "UZIO Columns Found": ", ".join(uzio_cols) if uzio_cols else "None"
        })

        if status == "Mismatch":
            all_emp_ids = set(paycom_emp_detail.keys()).union(set(uzio_emp_detail.keys()))
            for eid in all_emp_ids:
                if eid == "Unknown": continue
                emp_p_total = sum(paycom_emp_detail.get(eid, {}).values())
                emp_u_total = sum(uzio_emp_detail.get(eid, {}).values())
                if abs(emp_u_total - emp_p_total) > 0.02:
                    p_dates = paycom_emp_detail.get(eid, {})
                    u_dates = uzio_emp_detail.get(eid, {})
                    for p_date in set(p_dates.keys()).union(u_dates.keys()):
                        val_p = p_dates.get(p_date, 0.0)
                        val_u = u_dates.get(p_date, 0.0)
                        date_diff = val_u - val_p
                        if abs(date_diff) > 0.02:
                            employee_mismatches.append({
                                "Associate ID": eid, "Pay Date": p_date,
                                "Category": cat, "UZIO Item": u_name,
                                "Paycom Amount": round(val_p, 2),
                                "UZIO Amount": round(val_u, 2),
                                "Difference": round(date_diff, 2)
                            })

    all_employee_details = []
    all_emp_ids = set(global_emp_p.keys()).union(set(global_emp_u.keys()))
    for eid in sorted(all_emp_ids):
        if eid == "Unknown": continue
        emp_p_total = global_emp_p.get(eid, 0.0)
        emp_u_total = global_emp_u.get(eid, 0.0)
        diff = emp_u_total - emp_p_total
        all_employee_details.append({
            "Associate ID": eid,
            "Paycom Total": round(emp_p_total, 2),
            "UZIO Total": round(emp_u_total, 2),
            "Total Difference": round(diff, 2),
            "Status": "Match" if abs(diff) <= 0.02 else "Mismatch"
        })

    mismatches_only = [r for r in results if r["Status"] == "Mismatch"]
    return {
        "Full Comparison": results,
        "Mismatches Only": mismatches_only,
        "Employee Mismatches": employee_mismatches,
        "All Employee Details": all_employee_details
    }
