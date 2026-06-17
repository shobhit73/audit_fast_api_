import io
import itertools
import pandas as pd

# Reuse the ADP formula-aware reader: ADP money cells are =ROUND(x,n) formulas,
# and pandas.read_excel returns null for those. read_input_bytes() evaluates the
# formulas (and locates the real header row past ADP's banner preamble), so the
# numeric math below works on real ADP exports. This is the one intentional
# divergence from the Streamlit agent (which used a naive pd.read_excel) -- the
# repo convention (see CLAUDE.md / prior_payroll_sanity) requires this evaluator
# for any ADP-side reader. The CLASSIFICATION algorithms are otherwise a
# byte-for-byte port of apps/adp/payroll_setup_agent.py -- fix logic in BOTH.
from core.adp.prior_payroll_sanity import read_input_bytes

# =========================================================
# ADP Payroll Setup Agent (a.k.a. "ADP Payroll Analyzer") -- MCP core port.
#
# Ported from the Streamlit module apps/adp/payroll_setup_agent.py (root repo,
# sidebar entry "ADP - Payroll Setup Agent"). Three analyses:
#   Tab 1 - Earnings Classifier:  Hourly vs Flat earnings + Discretionary vs
#           Non-Discretionary (statistical OT-rate inflation test).
#   Tab 2 - Tax Mapping:          ADP tax columns -> Uzio tax codes ("Hansen
#           format"), federal + one row per WORKED-IN state.
#   Tab 3 - Deduction Classifier: Pre-Tax vs Post-Tax via per-row subset-sum on
#           GAP = Total Earnings - Federal Income Taxable, decided by 60%
#           majority across rows.
#
# **TWO-STEP, ASK-FIRST FLOW (do NOT assume / default).** The Streamlit Tax
# Mapping tab presents an on-screen multiselect for the states employees worked
# in. The MCP port mirrors that: called WITHOUT `selected_states`, the tool
# returns a discovery payload (detected states are a HINT only) and writes
# nothing. The caller must show the user the detected states, get an explicit
# confirmed list, and re-call with selected_states=[...]. Nothing is defaulted
# to the auto-detected states, and no bundled state-tax CSV is silently used.
# =========================================================

FEDERAL_MAP = {
    'FEDERAL INCOME - EMPLOYEE TAX':        'FIT',
    'MEDICARE - EMPLOYEE TAX':               'MEDI',
    'SOCIAL SECURITY - EMPLOYEE TAX':        'FICA',
    'MEDICARE - EMPLOYER TAX':               'ER_MEDI',
    'SOCIAL SECURITY - EMPLOYER TAX':        'ER_FICA',
    'FUTA - EMPLOYER TAX':                   'ER_FUTA',
}

STATE_MAP = {
    'WORKED IN STATE - EMPLOYEE TAX':        'SIT',
    'SUI/SDI - EMPLOYER TAX':                'ER_SUTA',
    'SUI/SDI - EMPLOYEE TAX':                'SDI',
    'WORKED IN LOCAL - EMPLOYEE TAX':        'CITY',
    'LIVED-IN LOCAL - EMPLOYEE TAX':         'CITY',
    'FAMILY LEAVE INSURANCE - EMPLOYEE TAX': 'FLI',
}

OUTPUT_COLS = [
    'Source Tax Code', 'Source Tax Code Name', 'Source Tax Code Description',
    'Uzio Tax Code', 'Unique Tax ID', 'Uzio Tax Code Description', 'Uzio Sub-Tax Description',
]


# ── helpers ────────────────────────────────────────────────────────────────────

def _extract_code(col):
    return col.split(':')[1].strip().split('-')[0].strip() if ':' in col else col.strip()


def _extract_desc(col):
    return col.split(':')[1].strip() if ':' in col else col.strip()


def _num(series):
    """Coerce a column to float, stripping $ and , -- mirrors the agent's
    clean_currency and makes the math robust whether the source cell came in as
    a number (formula-evaluated) or a string."""
    return pd.to_numeric(
        series.astype(str).str.replace(r'[\$,]', '', regex=True),
        errors='coerce',
    )


def _first_col(cols, exact_upper):
    return next((c for c in cols if str(c).strip().upper() == exact_upper), None)


def _read_payroll(content: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".csv"):
        df, _, _ = read_input_bytes(content, filename)
        return df
    df, _, _ = read_input_bytes(content, filename)
    return df


def _read_master(content: bytes) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(content), dtype=str)


def detect_states(df: pd.DataFrame) -> list:
    cols = list(df.columns)
    state_col = _first_col(cols, 'WORKED IN STATE')
    if not state_col:
        return []
    return sorted(
        {str(v).strip() for v in df[state_col].dropna()
         if str(v).strip() and str(v).strip().lower() != 'nan'}
    )


# ── Tab 1: Earnings classifier ─────────────────────────────────────────────────

def classify_earnings(df: pd.DataFrame) -> dict:
    all_cols = list(df.columns)
    hours_cols = [c for c in all_cols if 'ADDITIONAL HOURS' in str(c).upper()]
    earning_cols = [c for c in all_cols if 'ADDITIONAL EARNINGS' in str(c).upper()]
    reg_earn_col = _first_col(all_cols, 'REGULAR EARNINGS')
    ot_earn_col = _first_col(all_cols, 'OVERTIME EARNINGS')
    reg_hrs_col = _first_col(all_cols, 'REGULAR HOURS')
    ot_hrs_col = _first_col(all_cols, 'OVERTIME HOURS')

    hours_codes = {_extract_code(c): c for c in hours_cols}
    hourly_earnings, flat_earnings = [], []
    for ecol in earning_cols:
        code, desc = _extract_code(ecol), _extract_desc(ecol)
        (hourly_earnings if code in hours_codes else flat_earnings).append(
            {'code': code, 'description': desc, 'earn_col': ecol,
             **({'hrs_col': hours_codes[code]} if code in hours_codes else {})}
        )

    # Coerce the numeric columns the discretionary test relies on.
    work = df.copy()
    for c in [reg_earn_col, ot_earn_col, reg_hrs_col, ot_hrs_col] + earning_cols:
        if c is not None and c in work.columns:
            work[c] = _num(work[c])

    def analyze_discretionary(items):
        results = []
        if not all([reg_earn_col, ot_earn_col, reg_hrs_col, ot_hrs_col]):
            return results
        for item in items:
            ecol = item['earn_col']
            mask = (work[ot_earn_col].notna() & (work[ot_earn_col] > 0) &
                    work[reg_hrs_col].notna() & (work[ot_hrs_col] > 0) &
                    work[ecol].notna() & (work[ecol] > 0))
            sub = work[mask].copy()
            if len(sub) < 2:
                results.append({**item, 'verdict': 'Insufficient Data', 'avg_diff': None, 'n_rows': len(sub)})
                continue
            base_rate = sub[reg_earn_col] / sub[reg_hrs_col]
            actual_ot = sub[ot_earn_col] / sub[ot_hrs_col]
            expected_ot = base_rate * 1.5
            diff = actual_ot - expected_ot
            avg_diff, med_diff = diff.mean(), diff.median()
            verdict = 'Non-Discretionary' if (avg_diff > 0.15 and med_diff > 0.05) else 'Discretionary'
            results.append({**item, 'verdict': verdict, 'avg_diff': float(avg_diff), 'n_rows': int(len(sub))})
        return results

    discr_results = analyze_discretionary(hourly_earnings + flat_earnings)
    non_discr = sum(1 for r in discr_results if r['verdict'] == 'Non-Discretionary')
    discr_cnt = sum(1 for r in discr_results if r['verdict'] == 'Discretionary')
    insuf = sum(1 for r in discr_results if r['verdict'] == 'Insufficient Data')

    rows = [
        {'Code': 'REG', 'Description': 'Regular Earnings', 'Type': 'Hourly', 'Classification': 'Non-Discretionary', 'Avg OT Diff': '—'},
        {'Code': 'OT', 'Description': 'Overtime Earnings', 'Type': 'Hourly', 'Classification': 'Non-Discretionary', 'Avg OT Diff': '—'},
    ]
    for r in discr_results:
        is_hourly = any(r['code'] == i['code'] for i in hourly_earnings)
        rows.append({
            'Code': r['code'], 'Description': r['description'],
            'Type': 'Hourly' if is_hourly else 'Flat',
            'Classification': r['verdict'],
            'Avg OT Diff': f"${r['avg_diff']:.4f}" if r['avg_diff'] is not None else '—',
        })

    return {
        'summary_rows': rows,
        'counts': {
            'total_earnings': 2 + len(earning_cols),
            'hourly_earnings': len(hourly_earnings) + 2,
            'flat_earnings': len(flat_earnings),
            'non_discretionary': non_discr,
            'discretionary': discr_cnt,
            'insufficient_data': insuf,
        },
        'hourly_earnings': [i['description'] for i in hourly_earnings],
        'flat_earnings': [i['description'] for i in flat_earnings],
    }


# ── Tab 2: Tax mapping ──────────────────────────────────────────────────────────

def map_taxes(df: pd.DataFrame, master_df: pd.DataFrame, selected_states: list) -> dict:
    all_cols = list(df.columns)

    def is_actual_tax(col):
        c = str(col).upper()
        return ('TAX' in c and 'TAXABLE' not in c and 'TOTAL' not in c and col != 'TAX ID')

    raw_tax_cols = [c for c in all_cols if is_actual_tax(c)]

    fed_df = master_df[master_df['state_abbreviation'] == 'FED']

    def lookup_fed(type_code):
        m = fed_df[fed_df['unique_tax_id'].str.contains(f'-{type_code}-', na=False)]
        if not m.empty:
            r = m.iloc[0]
            return r['tax_code'], r['unique_tax_id'], r['tax_name']
        return None, None, None

    def lookup_state(state_abbr, type_code):
        st_df = master_df[master_df['state_abbreviation'] == state_abbr]
        m = st_df[st_df['unique_tax_id'].str.contains(f'-{type_code}-', na=False)]
        if not m.empty:
            r = m.iloc[0]
            sub = r.get('sub_tax_desc', None)
            return r['tax_code'], r['unique_tax_id'], r['tax_name'], (sub if pd.notna(sub) else '')
        return None, None, None, ''

    mapping_rows = []
    for adp_col in raw_tax_cols:
        col_upper = str(adp_col).upper()

        fed_key = next((kw for kw in FEDERAL_MAP if kw in col_upper), None)
        if fed_key:
            tc = FEDERAL_MAP[fed_key]
            uzio_code, uid, uname = lookup_fed(tc)
            mapping_rows.append({
                'Source Tax Code': '', 'Source Tax Code Name': adp_col, 'Source Tax Code Description': '',
                'Uzio Tax Code': uzio_code or '— NOT FOUND —', 'Unique Tax ID': uid or '—',
                'Uzio Tax Code Description': uname or '—', 'Uzio Sub-Tax Description': '',
                '_scope': 'Federal', '_state': 'FED', '_mapped': uzio_code is not None,
            })
            continue

        st_key = next((kw for kw in STATE_MAP if kw in col_upper), None)
        if st_key:
            tc = STATE_MAP[st_key]
            for state in selected_states:
                uzio_code, uid, uname, sub = lookup_state(state, tc)
                mapping_rows.append({
                    'Source Tax Code': '', 'Source Tax Code Name': adp_col, 'Source Tax Code Description': f'State: {state}',
                    'Uzio Tax Code': uzio_code or '— NOT FOUND —', 'Unique Tax ID': uid or '—',
                    'Uzio Tax Code Description': uname or '—', 'Uzio Sub-Tax Description': sub,
                    '_scope': 'State', '_state': state, '_mapped': uzio_code is not None,
                })
            continue

        mapping_rows.append({
            'Source Tax Code': '', 'Source Tax Code Name': adp_col, 'Source Tax Code Description': '',
            'Uzio Tax Code': '— MANUAL REVIEW —', 'Unique Tax ID': '—',
            'Uzio Tax Code Description': '—', 'Uzio Sub-Tax Description': '',
            '_scope': 'Unknown', '_state': '—', '_mapped': False,
        })

    total = len(mapping_rows)
    mapped = sum(1 for r in mapping_rows if r['_mapped'])
    return {
        'rows': [{k: r[k] for k in OUTPUT_COLS} for r in mapping_rows],
        'counts': {
            'total_tax_lines': total,
            'mapped': mapped,
            'unmapped': total - mapped,
            'federal': sum(1 for r in mapping_rows if r['_scope'] == 'Federal'),
            'state': sum(1 for r in mapping_rows if r['_scope'] == 'State'),
        },
        'unknown_columns': [r['Source Tax Code Name'] for r in mapping_rows if r['_scope'] == 'Unknown'],
        'unmapped_state': [
            {'column': r['Source Tax Code Name'], 'state': r['_state']}
            for r in mapping_rows if not r['_mapped'] and r['_scope'] == 'State'
        ],
    }


# ── Tab 3: Deduction classifier (pre-tax vs post-tax) ───────────────────────────

def classify_deductions(df: pd.DataFrame) -> dict:
    all_cols_ded = list(df.columns)
    total_earn_col = _first_col(all_cols_ded, 'TOTAL EARNINGS')
    fed_taxable_col = next((c for c in all_cols_ded if 'FEDERAL INCOME - EMPLOYEE TAXABLE' in str(c).upper()), None)

    ded_cols_raw = [c for c in all_cols_ded
                    if 'VOLUNTARY DEDUCTION' in str(c).upper()
                    and 'TOTAL' not in str(c).upper()
                    and 'REV' not in str(c).upper()]

    if not total_earn_col or not fed_taxable_col:
        return {'error': "Could not find TOTAL EARNINGS or FEDERAL INCOME - EMPLOYEE TAXABLE columns in this file."}
    if not ded_cols_raw:
        return {'results': [], 'counts': {'total': 0, 'pre_tax': 0, 'post_tax': 0, 'mixed': 0},
                'note': "No voluntary deduction columns found in this file."}

    def get_code(col):
        return col.split(':')[1].strip().split('-')[0].strip() if ':' in col else col.strip()

    def get_desc(col):
        return col.split(':')[1].strip() if ':' in col else col.strip()

    df_clean = df.copy()
    df_clean[total_earn_col] = _num(df_clean[total_earn_col])
    df_clean[fed_taxable_col] = _num(df_clean[fed_taxable_col])
    for col in ded_cols_raw:
        df_clean[col] = _num(df_clean[col]).fillna(0)

    df_valid = df_clean[
        df_clean[total_earn_col].notna() &
        df_clean[fed_taxable_col].notna() &
        (df_clean[total_earn_col] < 100_000)  # exclude aggregate/summary rows
    ].copy()
    df_valid['_GAP'] = (df_valid[total_earn_col] - df_valid[fed_taxable_col]).round(2)
    df_valid = df_valid[df_valid['_GAP'] >= 0]

    TOLERANCE = 5.00
    tally = {col: {'pretax': 0, 'posttax': 0, 'total': 0} for col in ded_cols_raw}

    for _, row in df_valid.iterrows():
        gap = row['_GAP']
        if gap <= 0:
            continue
        active = {col: row[col] for col in ded_cols_raw if row[col] > 0}
        if not active:
            continue

        best_err = float('inf')
        best_combo = set()
        active_cols = list(active.keys())
        active_cols.sort(key=lambda x: active[x], reverse=True)
        if len(active_cols) > 12:  # cap to avoid 2^n explosion
            active_cols = active_cols[:12]

        for r in range(1, len(active_cols) + 1):
            for combo in itertools.combinations(active_cols, r):
                s = sum(active[c] for c in combo)
                err = abs(s - gap)
                if err < best_err:
                    best_err = err
                    best_combo = set(combo)

        for col in active:
            tally[col]['total'] += 1
            if col in best_combo and best_err <= TOLERANCE:
                tally[col]['pretax'] += 1
            else:
                tally[col]['posttax'] += 1

    results = []
    for col in ded_cols_raw:
        t = tally[col]
        if t['total'] == 0:
            continue
        pre_pct = t['pretax'] / t['total'] * 100
        post_pct = t['posttax'] / t['total'] * 100
        if pre_pct >= 60:
            verdict = 'Pre-Tax'
        elif post_pct >= 60:
            verdict = 'Post-Tax'
        else:
            verdict = 'Mixed / Unclear'
        results.append({
            'Code': get_code(col),
            'Description': get_desc(col),
            'Total Rows': t['total'],
            'Pre-Tax Rows': f"{t['pretax']} ({pre_pct:.0f}%)",
            'Post-Tax Rows': f"{t['posttax']} ({post_pct:.0f}%)",
            'Verdict': verdict,
        })

    return {
        'results': results,
        'counts': {
            'total': len(results),
            'pre_tax': sum(1 for r in results if r['Verdict'] == 'Pre-Tax'),
            'post_tax': sum(1 for r in results if r['Verdict'] == 'Post-Tax'),
            'mixed': sum(1 for r in results if r['Verdict'] == 'Mixed / Unclear'),
        },
    }


# ── Orchestration ───────────────────────────────────────────────────────────────

def discover_adp_payroll_setup_agent(content: bytes, filename: str, state_tax_master_content: bytes | None = None) -> dict:
    """Step 1: parse the file and report what needs confirming. Writes nothing.

    Detected states are a HINT only -- the caller MUST get an explicit confirmed
    state list from the user before calling run_*(). Nothing is defaulted.
    """
    df = _read_payroll(content, filename)
    cols = [str(c) for c in df.columns]
    detected = detect_states(df)

    master_available = bool(state_tax_master_content)
    available_states = None
    if master_available:
        try:
            master_df = _read_master(state_tax_master_content)
            available_states = sorted(
                master_df[master_df['state_abbreviation'] != 'FED']['state_abbreviation'].dropna().unique().tolist()
            )
        except Exception:
            available_states = None

    def _tax_cols(c):
        cu = c.upper()
        return 'TAX' in cu and 'TAXABLE' not in cu and 'TOTAL' not in cu and c != 'TAX ID'

    return {
        "step": "discover",
        "detected_states": detected,
        "available_states": available_states,
        "state_tax_master_available": master_available,
        "tax_columns_found": [c for c in cols if _tax_cols(c)],
        "earnings_codes_found": [_extract_desc(c) for c in cols if 'ADDITIONAL EARNINGS' in c.upper()],
        "deduction_codes_found": [
            _extract_desc(c) for c in cols
            if 'VOLUNTARY DEDUCTION' in c.upper() and 'TOTAL' not in c.upper() and 'REV' not in c.upper()
        ],
        "instruction": (
            "ASK THE USER which states employees worked in -- the detected_states list "
            "is only a hint, do NOT assume it. Then re-call adp_payroll_setup_agent with "
            "selected_states=[...] set to the user's confirmed list. "
            + ("The State Tax master is available. "
               if master_available else
               "No State Tax master was provided -- supply state_tax_master_path or "
               "state_tax_master_base64 (or set STATE_TAX_MASTER_PATH) so the Tax "
               "Mapping tab can resolve Uzio tax codes.")
        ),
    }


def run_adp_payroll_setup_agent(content: bytes, filename: str, selected_states: list, state_tax_master_content: bytes):
    """Step 2: produce the full analysis for the user-confirmed states.

    Returns (xlsx_bytes, csv_outputs: dict[str, bytes], summary: dict).
    Raises ValueError if the State Tax master is missing (no silent fallback).
    """
    if not state_tax_master_content:
        raise ValueError(
            "State Tax master CSV is required to build the Tax Mapping. Provide "
            "state_tax_master_path / state_tax_master_base64, or set the "
            "STATE_TAX_MASTER_PATH env var. (No bundled default is used.)"
        )
    selected_states = [str(s).strip() for s in (selected_states or []) if str(s).strip()]

    df = _read_payroll(content, filename)
    master_df = _read_master(state_tax_master_content)

    earnings = classify_earnings(df)
    taxes = map_taxes(df, master_df, selected_states)
    deductions = classify_deductions(df)

    earnings_df = pd.DataFrame(earnings['summary_rows'])
    tax_df = pd.DataFrame(taxes['rows'], columns=OUTPUT_COLS)
    ded_df = pd.DataFrame(
        deductions.get('results', []),
        columns=['Code', 'Description', 'Total Rows', 'Pre-Tax Rows', 'Post-Tax Rows', 'Verdict'],
    )

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        earnings_df.to_excel(writer, sheet_name="Earnings_Summary", index=False)
        tax_df.to_excel(writer, sheet_name="Tax_Mapping", index=False)
        ded_df.to_excel(writer, sheet_name="Deduction_Classification", index=False)

    # Plain UTF-8, NO BOM (API ingestion). Three CSVs mirror the agent's three
    # on-screen download buttons.
    csv_outputs = {
        "Earnings_Summary": earnings_df.to_csv(index=False).encode("utf-8"),
        "Tax_Mapping": tax_df.to_csv(index=False).encode("utf-8"),
        "Deduction_Classification": ded_df.to_csv(index=False).encode("utf-8"),
    }

    summary = {
        "selected_states": selected_states,
        "earnings": {"counts": earnings['counts'], "summary_rows": earnings['summary_rows'],
                     "hourly_earnings": earnings['hourly_earnings'], "flat_earnings": earnings['flat_earnings']},
        "tax_mapping": {"counts": taxes['counts'], "rows": taxes['rows'],
                        "unknown_columns": taxes['unknown_columns'], "unmapped_state": taxes['unmapped_state']},
        "deductions": {k: v for k, v in deductions.items()},
    }
    return out.getvalue(), csv_outputs, summary
