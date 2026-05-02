import pandas as pd
import io
import re
from utils.audit_utils import clean_money_val, norm_colname, normalize_id, format_pay_date, smart_read_df

def parse_paycom_filename_date(filename):
    match = re.findall(r'(\d{8})', str(filename))
    if len(match) >= 3:
        d = match[2]
        return f"{d[4:]}-{d[:2]}-{d[2:4]}"
    return "Unknown"

def find_header_and_data_uzio(file_content):
    """Find the data row in a Uzio export file (bytes)."""
    # Try finding the right sheet first if it's Excel
    try:
        import io
        xls = pd.ExcelFile(io.BytesIO(file_content))
        target_sheet = xls.sheet_names[0]
        if len(xls.sheet_names) > 1 and "criteria" in xls.sheet_names[0].lower():
            target_sheet = xls.sheet_names[1]
        df_peek = pd.read_excel(xls, sheet_name=target_sheet, header=None, nrows=50)
    except Exception:
        # Fallback to direct read (CSV or simple Excel)
        df_peek = smart_read_df(file_content, header=None, nrows=50)
        target_sheet = None

    header_idx = 0
    for i, row in df_peek.iterrows():
        row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
        if "employee id" in row_str or "employee name" in row_str:
            header_idx = i; break

    if target_sheet:
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=target_sheet, header=header_idx)
    else:
        df = smart_read_df(file_content, header=header_idx)
    
    header_top = df_peek.iloc[header_idx - 1].tolist() if header_idx > 0 else None
    return df, header_top

def find_header_and_data_paycom(file_content, filename):
    """Find the data row in a Paycom export file (bytes)."""
    df_peek = smart_read_df(file_content, filename=filename, header=None, nrows=20)
    header_idx = 0
    for i, row in df_peek.iterrows():
        row_str = " ".join([str(x).lower() for x in row if pd.notna(x)])
        if any(kw in row_str for kw in ["ee code", "description", "earning", "amount", "row labels"]):
            header_idx = i; break
    
    return smart_read_df(file_content, filename=filename, header=header_idx), None

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

# Standard federal tax rates used for verification (in percent)
STANDARD_TAX_RATES = {
    "Social Security EE": 6.20,
    "Social Security ER": 6.20,
    "Medicare EE":        1.45,
    "Medicare ER":        1.45,
    "FUTA ER":            0.60,
}
RATE_TOLERANCE_PCT = 0.05


def _filter_data_rows(df, eid_col):
    if not eid_col:
        return df
    work = df[df[eid_col].notna()].copy()
    work[eid_col] = work[eid_col].astype(str).str.strip()
    return work[(work[eid_col] != "") & (~work[eid_col].str.lower().str.contains("total|grand", na=False))]


def _sum_uzio_section(df, header_top, section_name, side):
    """Sum Taxable Wages and EE/ER Amount within a UZIO section header."""
    if not header_top:
        return 0.0, 0.0
    eid_col = next((c for c in df.columns if any(x in str(c).lower() for x in ["employee id", "associate id"])), None)
    work = _filter_data_rows(df, eid_col)
    target = norm_colname(section_name).lower()
    wages = amount = 0.0
    for i, h in enumerate(header_top):
        if pd.notna(h) and norm_colname(str(h)).lower() == target:
            end_i = len(df.columns)
            for j in range(i + 1, len(header_top)):
                if pd.notna(header_top[j]) and str(header_top[j]).strip() != "":
                    end_i = j
                    break
            for k in range(i, end_i):
                col = str(df.columns[k]).strip().lower()
                if "taxable wages" in col:
                    wages += work.iloc[:, k].apply(clean_money_val).sum()
                elif side == "EE" and (col == "ee amount" or col.startswith("ee amount.")):
                    amount += work.iloc[:, k].apply(clean_money_val).sum()
                elif side == "ER" and (col == "er amount" or col.startswith("er amount.")):
                    amount += work.iloc[:, k].apply(clean_money_val).sum()
            break
    return wages, amount


def _sum_paycom_for_uzio_name(paycom_data_list, source_names):
    """Best-effort sum of (taxable wages, amount) across Paycom long-format rows.
    Wages are inferred from rows whose Description matches the tax with 'tax'->'wages',
    or contains 'taxable wages'/'wages' for the same tax."""
    if not source_names:
        return 0.0, 0.0
    desc_aliases = ["type description", "description", "earning/deduction/tax", "code description", "row labels"]
    norm_targets = [n.lower().strip() for n in source_names]
    wage_targets = set()
    for n in norm_targets:
        wage_targets.add(re.sub(r"\btax\b", "wages", n, flags=re.I))
        wage_targets.add(n + " wages")
        wage_targets.add(n + " taxable wages")

    total_w = total_a = 0.0
    for df_p, _ in paycom_data_list:
        desc_col = next((c for c in df_p.columns if any(x in str(c).lower() for x in desc_aliases)), None)
        amt_col  = next((c for c in df_p.columns if "current amount" in str(c).lower()), None)
        if not amt_col:
            amt_col = next((c for c in df_p.columns if any(x in str(c).lower() for x in ["amount", "total amount", "value", "sum of amount"])), None)
        if not desc_col or not amt_col:
            continue
        for _, row in df_p.iterrows():
            d = str(row[desc_col]).strip().lower()
            if d in norm_targets:
                total_a += clean_money_val(row[amt_col])
            elif d in wage_targets:
                total_w += clean_money_val(row[amt_col])
    return total_w, total_a


def compute_tax_rate_verification(df_uzio, uzio_top, paycom_data_list, mappings):
    """Build the tax-rate verification table (SS, Medicare, FUTA, SUTA per state) for Paycom."""
    uzio_to_source = {}
    for m in mappings:
        if m.get("Category") == "Taxes":
            uzio_to_source.setdefault(m["UZIO_Name"], []).append(m.get("Source_Name") or m.get("ADP_Name"))

    targets = [
        ("Social Security", "EE", "Social Security Tax",          STANDARD_TAX_RATES["Social Security EE"]),
        ("Social Security", "ER", "Employer Social Security Tax", STANDARD_TAX_RATES["Social Security ER"]),
        ("Medicare",        "EE", "Medicare Tax",                 STANDARD_TAX_RATES["Medicare EE"]),
        ("Medicare",        "ER", "Employer Medicare Tax",        STANDARD_TAX_RATES["Medicare ER"]),
        ("FUTA",            "ER", "Federal Unemployment Tax",     STANDARD_TAX_RATES["FUTA ER"]),
    ]
    if uzio_top:
        suta_re = re.compile(r"^\s*([A-Z]{2})\s+STATE\s+UNEMPLOYMENT\s+TAX\s*$", re.I)
        for h in uzio_top:
            if pd.notna(h):
                m = suta_re.match(str(h))
                if m:
                    targets.append((f"SUTA - {m.group(1).upper()}", "ER", str(h).strip(), None))

    rows = []
    for tax, side, uzio_name, std in targets:
        u_w, u_a = _sum_uzio_section(df_uzio, uzio_top, uzio_name, side)
        p_w, p_a = _sum_paycom_for_uzio_name(paycom_data_list, uzio_to_source.get(uzio_name, []))
        u_rate = (u_a / u_w * 100) if u_w > 0 else None
        p_rate = (p_a / p_w * 100) if p_w > 0 else None
        if std is None:
            status = "Info (Employer-set)"
            std_disp = "Employer-set"
        else:
            off_u = (u_rate is not None) and abs(u_rate - std) > RATE_TOLERANCE_PCT
            off_p = (p_rate is not None) and abs(p_rate - std) > RATE_TOLERANCE_PCT
            status = "Mismatch" if (off_u or off_p) else "Match"
            std_disp = f"{std:.2f}%"
        rows.append({
            "Tax": tax,
            "Side": side,
            "Paycom Taxable Wages":  round(p_w, 2),
            "Paycom Amount":         round(p_a, 2),
            "Paycom Effective Rate": (f"{p_rate:.4f}%" if p_rate is not None else "-"),
            "UZIO Taxable Wages":    round(u_w, 2),
            "UZIO Amount":           round(u_a, 2),
            "UZIO Effective Rate":   (f"{u_rate:.4f}%" if u_rate is not None else "-"),
            "Standard Rate":         std_disp,
            "Status":                status,
        })
    return rows


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

    tax_rate_verification = compute_tax_rate_verification(df_uzio, uzio_top, paycom_data_list, mappings)

    return {
        "Full Comparison": results,
        "Mismatches Only": mismatches_only,
        "Employee Mismatches": employee_mismatches,
        "All Employee Details": all_employee_details,
        "Tax Rate Verification": tax_rate_verification,
    }
