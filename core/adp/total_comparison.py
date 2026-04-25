import pandas as pd
import io
from utils.audit_utils import clean_money_val, norm_colname, normalize_id, format_pay_date, find_header_and_data

def calculate_totals(df, header_top, column_names):
    """Sum up values for columns that match any of the provided names."""
    found_cols = []
    emp_tots = {}
    emp_row_counts = {}
    
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

def run_adp_total_comparison(adp_files_data, uzio_file_data, mappings):
    """
    Main logic to compare totals.
    adp_files_data: list of (content, filename)
    uzio_file_data: (content, filename)
    """
    df_uzio, uzio_top, _ = find_header_and_data(uzio_file_data[0], uzio_file_data[1])
    adp_data_list = []
    for content, filename in adp_files_data:
        df_adp, adp_top, _ = find_header_and_data(content, filename)
        adp_data_list.append((df_adp, adp_top))

    results = []
    employee_mismatches = []
    
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
            for (eid, p_date), c_val in emp_c.items():
                if eid not in adp_emp_counts: adp_emp_counts[eid] = {}
                adp_emp_counts[eid][p_date] = adp_emp_counts[eid].get(p_date, 0) + c_val
        
        uzio_total, uzio_cols, uzio_emp_m, _ = calculate_totals(df_uzio, uzio_top, [u_name])
        uzio_emp_detail = {}
        for (eid, p_date), v in uzio_emp_m.items():
            if eid not in uzio_emp_detail: uzio_emp_detail[eid] = {}
            uzio_emp_detail[eid][p_date] = uzio_emp_detail[eid].get(p_date, 0.0) + v
        
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
    return {
        "summary": results,
        "mismatches": employee_mismatches
    }
