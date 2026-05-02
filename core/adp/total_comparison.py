import pandas as pd
import io
import re
from utils.audit_utils import clean_money_val, norm_colname, normalize_id, format_pay_date, find_header_and_data

def calculate_totals(df, header_top, column_names):
    """Sum up values for columns that match any of the provided names, handling multi-row headers."""
    found_cols = []
    emp_tots = {}
    emp_row_counts = {}
    
    # --- STRICT ROW FILTERING ---
    id_col = next((c for c in df.columns if any(x in str(c).lower() for x in ["associate id", "employee id", "file #"])), None)
    date_col = next((c for c in df.columns if any(x == str(c).lower().strip() for x in ["pay date", "check date"])), None)
    if date_col is None:
        date_col = next((c for c in df.columns if any(x in str(c).lower() for x in ["pay date", "period end", "check date"])), None)
    
    if id_col:
        df_clean = df[df[id_col].notna()].copy()
        df_clean[id_col] = df_clean[id_col].apply(normalize_id)
        df_clean = df_clean[
            (df_clean[id_col] != "Unknown") & 
            (~df_clean[id_col].str.lower().str.contains("total|grand", na=False))
        ]
    else:
        mask = df.iloc[:, 0].astype(str).str.lower().str.contains("total|grand", na=False)
        df_clean = df[~mask].copy()
    
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
                        end_idx = k
                        break
            for k in range(start_idx, end_idx):
                main_h = str(df.columns[k]).lower()
                if any(x in main_h for x in ['amount', 'total', 'current', 'ee', 'er', 'tax']):
                    if not any(x in main_h for x in ['wages', 'hours', 'rate', 'basis', 'taxable']):
                        cols_to_sum.append(df.columns[k])
                        found_cols.append(f"{df.columns[k]}")
                        
    for _, row in df_clean.iterrows():
        eid = row[id_col] if id_col else "Unknown"
        pay_date = format_pay_date(row[date_col]) if date_col else "Unknown"
        
        row_tot = sum(clean_money_val(row[c]) for c in set(cols_to_sum))
        
        key = (eid, pay_date)
        if key not in emp_tots:
            emp_tots[key] = 0.0
            emp_row_counts[key] = 0
        emp_tots[key] += row_tot
        emp_row_counts[key] += 1
            
    return sum(emp_tots.values()), found_cols, emp_tots, emp_row_counts

def detect_duplicate_pay_periods(df):
    """Find UZIO rows that share Employee ID + Start/End/Pay Date with another row.
    Returns a list of dicts (one per offending row), classifying each as Skeleton or Detail.
    """
    def find_col(candidates):
        for cand in candidates:
            for c in df.columns:
                if str(c).strip().lower() == cand.lower():
                    return c
        return None

    eid_col   = find_col(["Employee ID", "Associate ID", "File #"])
    first_col = find_col(["First Name"])
    last_col  = find_col(["Last Name"])
    start_col = find_col(["Start Date", "Period Start"])
    end_col   = find_col(["End Date", "Period End"])
    pay_col   = find_col(["Pay Date", "Check Date"])
    gross_col = find_col(["Gross Pay", "Gross"])
    net_col   = find_col(["Net Pay", "Net"])

    if not all([eid_col, start_col, end_col, pay_col]):
        return []

    work = df[df[eid_col].notna()].copy()
    work[eid_col] = work[eid_col].astype(str).str.strip()
    work = work[(work[eid_col] != "") & (~work[eid_col].str.lower().str.contains("total|grand", na=False))]

    keys = [eid_col, start_col, end_col, pay_col]
    counts = work.groupby(keys).size().reset_index(name="_n")
    dup_keys = counts[counts["_n"] > 1]
    if dup_keys.empty:
        return []

    dup_rows = work.merge(dup_keys, on=keys, how="inner")

    def classify(row):
        if gross_col and clean_money_val(row.get(gross_col)) != 0:
            return "Detail (real values)"
        dash_count = sum(1 for v in row.values if str(v).strip() == "-")
        if dash_count >= 5:
            return "Skeleton (dashes / zeros)"
        return "Zero detail"

    out_records = []
    for _, row in dup_rows.iterrows():
        fn = str(row[first_col]).strip() if first_col and pd.notna(row[first_col]) else ""
        ln = str(row[last_col]).strip() if last_col and pd.notna(row[last_col]) else ""
        rec = {
            "Employee ID": str(row[eid_col]),
            "Employee Name": (fn + " " + ln).strip(),
            "Start Date": str(row[start_col]),
            "End Date": str(row[end_col]),
            "Pay Date": str(row[pay_col]),
            "Row Type": classify(row),
            "Rows in Group": int(row["_n"]),
        }
        if gross_col:
            rec["Gross Pay"] = round(clean_money_val(row[gross_col]), 2)
        if net_col:
            rec["Net Pay"] = round(clean_money_val(row[net_col]), 2)
        out_records.append(rec)

    out_records.sort(key=lambda r: (r["Employee ID"], r["Pay Date"], r["Row Type"]))
    return out_records


def compute_pay_stub_count_diff(adp_data_list, df_uzio):
    """Per employee, compare distinct Pay Date count between combined ADP files and UZIO file."""
    def find_col_in(df, candidates):
        for cand in candidates:
            for c in df.columns:
                if str(c).strip().lower() == cand.lower():
                    return c
        for cand in candidates:
            for c in df.columns:
                if cand.lower() in str(c).strip().lower():
                    return c
        return None

    adp_stubs, adp_names = {}, {}
    for df_adp, _ in adp_data_list:
        eid_col   = find_col_in(df_adp, ["Associate ID", "Employee ID", "File #"])
        pay_col   = find_col_in(df_adp, ["Check Date", "Pay Date", "Pay Period End", "Period End Date"])
        first_col = find_col_in(df_adp, ["First Name", "Employee First Name"])
        last_col  = find_col_in(df_adp, ["Last Name", "Employee Last Name"])
        full_col  = find_col_in(df_adp, ["Employee Name", "Name"])
        if not eid_col or not pay_col:
            continue
        for _, row in df_adp.iterrows():
            raw_eid = str(row[eid_col]).strip()
            if not raw_eid or raw_eid.lower() in ("nan", "total", "grand"):
                continue
            pay = format_pay_date(row[pay_col])
            if pay == "Unknown":
                continue
            key = normalize_id(raw_eid)
            if key == "Unknown":
                continue
            adp_stubs.setdefault(key, set()).add(pay)
            if key not in adp_names:
                if first_col or last_col:
                    fn = str(row[first_col]).strip() if first_col and pd.notna(row[first_col]) else ""
                    ln = str(row[last_col]).strip() if last_col and pd.notna(row[last_col]) else ""
                    nm = (fn + " " + ln).strip()
                elif full_col and pd.notna(row[full_col]):
                    nm = str(row[full_col]).strip()
                else:
                    nm = ""
                if nm:
                    adp_names[key] = nm

    eid_col   = find_col_in(df_uzio, ["Employee ID"])
    pay_col   = find_col_in(df_uzio, ["Pay Date"])
    first_col = find_col_in(df_uzio, ["First Name"])
    last_col  = find_col_in(df_uzio, ["Last Name"])

    uzio_stubs, uzio_names = {}, {}
    if eid_col and pay_col:
        for _, row in df_uzio.iterrows():
            raw_eid = str(row[eid_col]).strip()
            if not raw_eid or raw_eid.lower() in ("nan", "total", "grand"):
                continue
            pay = format_pay_date(row[pay_col])
            if pay == "Unknown":
                continue
            key = normalize_id(raw_eid)
            if key == "Unknown":
                continue
            uzio_stubs.setdefault(key, set()).add(pay)
            if key not in uzio_names:
                fn = str(row[first_col]).strip() if first_col and pd.notna(row[first_col]) else ""
                ln = str(row[last_col]).strip() if last_col and pd.notna(row[last_col]) else ""
                nm = (fn + " " + ln).strip()
                if nm:
                    uzio_names[key] = nm

    out = []
    all_keys = set(adp_stubs.keys()) | set(uzio_stubs.keys())
    for k in all_keys:
        a_dates = adp_stubs.get(k, set())
        u_dates = uzio_stubs.get(k, set())
        a_n, u_n = len(a_dates), len(u_dates)
        diff = u_n - a_n
        if diff == 0:
            status = "Match"
        elif diff > 0:
            status = "Extra in UZIO"
        else:
            status = "Missing in UZIO"
        out.append({
            "Employee ID": k,
            "Employee Name": uzio_names.get(k) or adp_names.get(k, ""),
            "ADP Pay Stubs": a_n,
            "UZIO Pay Stubs": u_n,
            "Difference": diff,
            "Status": status,
            "Pay Dates Missing in UZIO": ", ".join(sorted(a_dates - u_dates)),
            "Pay Dates Missing in ADP":  ", ".join(sorted(u_dates - a_dates)),
        })
    out.sort(key=lambda r: (0 if r["Status"] != "Match" else 1, r["Employee ID"]))
    return out


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


def _sum_adp_for_uzio_name(adp_data_list, adp_names, side):
    if not adp_names:
        return 0.0, 0.0
    total_w = total_a = 0.0
    for df_adp, adp_top in adp_data_list:
        eid_col = next((c for c in df_adp.columns if any(x in str(c).lower() for x in ["associate id", "employee id", "file #"])), None)
        work = _filter_data_rows(df_adp, eid_col)
        norm_main = {norm_colname(c).lower(): i for i, c in enumerate(df_adp.columns)}
        norm_top  = {norm_colname(str(c)).lower(): i for i, c in enumerate(adp_top or []) if pd.notna(c) and str(c).strip() != ""}
        for name in adp_names:
            n = norm_colname(name).lower()
            if n in norm_main:
                idx = norm_main[n]
                total_a += work.iloc[:, idx].apply(clean_money_val).sum()
                tax_col = str(df_adp.columns[idx])
                cand_names = []
                if re.search(r"\btax\b", tax_col, re.I):
                    cand_names.append(re.sub(r"\btax\b", "Wages", tax_col, flags=re.I))
                cand_names.extend([tax_col + " Wages", tax_col + " Taxable Wages"])
                found_wages = False
                for cn in cand_names:
                    nn = norm_colname(cn).lower()
                    if nn in norm_main:
                        total_w += work.iloc[:, norm_main[nn]].apply(clean_money_val).sum()
                        found_wages = True
                        break
                if not found_wages:
                    for off in (-1, 1, -2, 2):
                        j = idx + off
                        if 0 <= j < len(df_adp.columns):
                            ch = str(df_adp.columns[j]).lower()
                            if "wages" in ch and "tax" not in ch:
                                total_w += work.iloc[:, j].apply(clean_money_val).sum()
                                break
            elif n in norm_top:
                start_idx = norm_top[n]
                end_i = len(df_adp.columns)
                for j in range(start_idx + 1, len(adp_top)):
                    if pd.notna(adp_top[j]) and str(adp_top[j]).strip() != "":
                        end_i = j
                        break
                for k in range(start_idx, end_i):
                    ch = str(df_adp.columns[k]).strip().lower()
                    if "taxable wages" in ch:
                        total_w += work.iloc[:, k].apply(clean_money_val).sum()
                    elif side == "EE" and (ch == "ee amount" or ch.startswith("ee amount.")):
                        total_a += work.iloc[:, k].apply(clean_money_val).sum()
                    elif side == "ER" and (ch == "er amount" or ch.startswith("er amount.")):
                        total_a += work.iloc[:, k].apply(clean_money_val).sum()
    return total_w, total_a


def compute_tax_rate_verification(df_uzio, uzio_top, adp_data_list, mappings):
    """Build the tax-rate verification table (SS, Medicare, FUTA, SUTA per state)."""
    uzio_to_adp = {}
    for m in mappings:
        if m.get("Category") == "Taxes":
            uzio_to_adp.setdefault(m["UZIO_Name"], []).append(m["ADP_Name"])

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
        a_w, a_a = _sum_adp_for_uzio_name(adp_data_list, uzio_to_adp.get(uzio_name, []), side)
        u_rate = (u_a / u_w * 100) if u_w > 0 else None
        a_rate = (a_a / a_w * 100) if a_w > 0 else None
        if std is None:
            status = "Info (Employer-set)"
            std_disp = "Employer-set"
        else:
            off_u = (u_rate is not None) and abs(u_rate - std) > RATE_TOLERANCE_PCT
            off_a = (a_rate is not None) and abs(a_rate - std) > RATE_TOLERANCE_PCT
            status = "Mismatch" if (off_u or off_a) else "Match"
            std_disp = f"{std:.2f}%"
        rows.append({
            "Tax": tax,
            "Side": side,
            "ADP Taxable Wages":  round(a_w, 2),
            "ADP Amount":         round(a_a, 2),
            "ADP Effective Rate": (f"{a_rate:.4f}%" if a_rate is not None else "-"),
            "UZIO Taxable Wages": round(u_w, 2),
            "UZIO Amount":        round(u_a, 2),
            "UZIO Effective Rate":(f"{u_rate:.4f}%" if u_rate is not None else "-"),
            "Standard Rate":      std_disp,
            "Status":             status,
        })
    return rows


def run_adp_total_comparison(adp_files_data, uzio_file_data, mappings):
    """
    Main logic to compare totals based on mappings.
    adp_files_data: list of (content_bytes, filename)
    uzio_file_data: (content_bytes, filename)
    mappings: list of dicts with Category, ADP_Name, UZIO_Name
    """
    df_uzio, uzio_top, _ = find_header_and_data(uzio_file_data[0], uzio_file_data[1])
    adp_data_list = []
    for content, filename in adp_files_data:
        df_adp, adp_top, _ = find_header_and_data(content, filename)
        adp_data_list.append((df_adp, adp_top))

    results = []
    employee_mismatches = []
    
    # --- Global Employee Collection ---
    global_emp_adp = {} # {eid: total}
    global_emp_uzio = {} # {eid: total}

    unique_uzio_items = {}
    for m in mappings:
        u_name = m["UZIO_Name"]
        if u_name not in unique_uzio_items:
            unique_uzio_items[u_name] = {"Category": m["Category"], "ADP_Names": []}
        unique_uzio_items[u_name]["ADP_Names"].append(m["ADP_Name"])

    for u_name, data in unique_uzio_items.items():
        cat = data["Category"]
        adp_names = data["ADP_Names"]
        
        adp_total = 0.0
        adp_cols = []
        adp_emp_detail = {}
        adp_emp_counts = {}
        for df_a, adp_t in adp_data_list:
            tot, cols, emp_m, emp_c = calculate_totals(df_a, adp_t, adp_names)
            adp_total += tot
            for c in cols:
                if c not in adp_cols: adp_cols.append(c)
            for (eid, p_date), v in emp_m.items():
                if eid not in adp_emp_detail: adp_emp_detail[eid] = {}
                adp_emp_detail[eid][p_date] = adp_emp_detail[eid].get(p_date, 0.0) + v
                # Aggregate globally
                global_emp_adp[eid] = global_emp_adp.get(eid, 0.0) + v
                
            for (eid, p_date), c_val in emp_c.items():
                if eid not in adp_emp_counts: adp_emp_counts[eid] = {}
                adp_emp_counts[eid][p_date] = adp_emp_counts[eid].get(p_date, 0) + c_val
        
        uzio_total, uzio_cols, uzio_emp_m, _ = calculate_totals(df_uzio, uzio_top, [u_name])
        uzio_emp_detail = {}
        for (eid, p_date), v in uzio_emp_m.items():
            if eid not in uzio_emp_detail: uzio_emp_detail[eid] = {}
            uzio_emp_detail[eid][p_date] = uzio_emp_detail[eid].get(p_date, 0.0) + v
            # Aggregate globally
            global_emp_uzio[eid] = global_emp_uzio.get(eid, 0.0) + v
        
        diff = uzio_total - adp_total
        status = "Match" if abs(diff) <= 0.02 else "Mismatch"
        
        results.append({
            "Category": cat,
            "UZIO Item": u_name,
            "ADP Total": round(adp_total, 2),
            "UZIO Total": round(uzio_total, 2),
            "Difference": round(diff, 2),
            "Status": status,
            "ADP Columns Found": ", ".join(adp_cols) if adp_cols else "None",
            "UZIO Columns Found": ", ".join(uzio_cols) if uzio_cols else "None"
        })
        
        if status == "Mismatch":
            all_emp_ids = set(adp_emp_detail.keys()).union(set(uzio_emp_detail.keys()))
            for eid in all_emp_ids:
                if eid == "Unknown": continue
                emp_adp_total = sum(adp_emp_detail.get(eid, {}).values())
                emp_uzio_total = sum(uzio_emp_detail.get(eid, {}).values())
                if abs(emp_uzio_total - emp_adp_total) > 0.02:
                    adp_dates = adp_emp_detail.get(eid, {})
                    uzio_dates = uzio_emp_detail.get(eid, {})
                    all_dates = set(adp_dates.keys()).union(set(uzio_dates.keys()))
                    for p_date in all_dates:
                        val_adp = adp_dates.get(p_date, 0.0)
                        val_uzio = uzio_dates.get(p_date, 0.0)
                        date_diff = val_uzio - val_adp
                        if abs(date_diff) > 0.02:
                            multiple_entries = "Yes" if adp_emp_counts.get(eid, {}).get(p_date, 0) > 1 else "No"
                            employee_mismatches.append({
                                "Associate ID": eid,
                                "Pay Date": p_date,
                                "Category": cat,
                                "UZIO Item": u_name,
                                "ADP Amount": round(val_adp, 2),
                                "UZIO Amount": round(val_uzio, 2),
                                "Difference": round(date_diff, 2),
                                "Multiple ADP Entries": multiple_entries
                            })

    all_employee_details = []
    all_emp_ids = set(global_emp_adp.keys()).union(set(global_emp_uzio.keys()))
    for eid in sorted(all_emp_ids):
        if eid == "Unknown": continue
        val_a = global_emp_adp.get(eid, 0.0)
        val_u = global_emp_uzio.get(eid, 0.0)
        diff = val_u - val_a
        all_employee_details.append({
            "Associate ID": eid,
            "ADP Total": round(val_a, 2),
            "UZIO Total": round(val_u, 2),
            "Total Difference": round(diff, 2),
            "Status": "Match" if abs(diff) <= 0.02 else "Mismatch"
        })

    mismatches_only = [r for r in results if r["Status"] == "Mismatch"]

    # Three new analyses (matching the Streamlit version's tabs)
    duplicate_pay_periods = detect_duplicate_pay_periods(df_uzio)
    pay_stub_counts       = compute_pay_stub_count_diff(adp_data_list, df_uzio)
    tax_rate_verification = compute_tax_rate_verification(df_uzio, uzio_top, adp_data_list, mappings)

    return {
        "Full Comparison": results,
        "Mismatches Only": mismatches_only,
        "Employee Mismatches": employee_mismatches,
        "All Employee Details": all_employee_details,
        "Duplicate Pay Periods": duplicate_pay_periods,
        "Pay Stub Counts": pay_stub_counts,
        "Tax Rate Verification": tax_rate_verification,
    }
