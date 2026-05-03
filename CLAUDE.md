# CLAUDE.md (v1.5)

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Scope of this directory

`audit_fast_api/` is a separate Python project from the parent `Deduction Tool/` Streamlit app — it has its own `.git`, its own `requirements.txt`, and its own `core/` reimplementations of the audit logic. The parent's [CLAUDE.md](../CLAUDE.md) and [README.md](../README.md) describe the Streamlit tool; this document covers only the FastAPI + MCP service.

When fixing a bug that exists in both, check the Streamlit `apps/{adp,paycom}/*.py` modules — most of the audit semantics here were ported from there and may need the same fix in both trees (see commit `f254a50` for an example: the ADP Census Sanity auto-fix pipeline was mirrored from the Streamlit `census_generator.py` into [core/census/sanity_check.py](core/census/sanity_check.py)).

## Run

```bash
pip install -r requirements.txt

# FastAPI HTTP server (mounts MCP at /mcp/sse)
uvicorn main:app --reload --port 8000

# MCP server over stdio (for Claude Desktop / local clients)
python mcp_server.py
```

There are no tests, no linter, no build step.

## Two entry points, one shared core

### [main.py](main.py) — FastAPI

`app = FastAPI(...)` is defined at module top level (this is what Vercel's Zero Config deploy detects — see commit `21192a1`).

Almost the entire body of `main.py` is wrapped in a single `try: ... except: ...`. If any import or endpoint definition fails at startup, the `except` block installs a fallback `/` and `/{path:path}` that return the captured `startup_error`. Don't refactor this away — when deployed to Vercel it's the only way to see why the app failed to boot.

The MCP Starlette app is mounted into FastAPI at `/mcp`:
```python
from mcp_server import mcp_app
app.mount("/mcp", mcp_app)
```
So the SSE endpoint is reachable at `/mcp/sse` and clients post messages to `/mcp/messages`.

### [mcp_server.py](mcp_server.py) — MCP Server

Exposes the same audits as MCP tools. Two transports in one file:
- **stdio** (`if __name__ == "__main__"`) — for Claude Desktop
- **SSE** (`mcp_app = Starlette(...)`) — mounted by `main.py`

Critical at the top of the file: `sys.stdout = sys.stderr`. Never remove this. Stdio MCP uses stdout as the protocol channel, so any `print()` from imported modules would corrupt the stream.

#### The audit-inbox drop-folder pattern

```python
AUDIT_INBOX = r"C:\Users\shobhit.sharma\Desktop\Audit Files"
```

**MANDATORY**: 100% of tool output files are consolidated in this folder. Claude Desktop must always check this folder via `list_audit_files` to find generated reports.

#### Data Correction & Formatting
The `apply_data_corrections` tool uses `openpyxl` to perform row-level updates based on **Employee ID**. This tool is specifically designed to **preserve all original Excel formatting** (colors, fonts, borders). Always prioritize this for "Implementer Overrides" over standard Pandas-based re-writes.

#### File input convention (every MCP tool)

Each tool accepts both a local path *and* a base64 fallback — see `load_file()` and `load_files_list()` in [mcp_server.py](mcp_server.py). Path is preferred; base64 is for HTTP/remote callers. Don't add a tool that only accepts one of them — keep the pair.

#### Output convention

`save_results_to_excel()` writes results to `~/Desktop/<Prefix>_<timestamp>.xlsx` and returns a JSON summary with a top-10 preview. The MCP response is the summary — the full report is the file on disk. Keep this contract: streaming megabytes of audit results back through MCP will exceed token limits.

`_json_default()` handles numpy/pandas/datetime serialization. Use it (`json.dumps(..., default=_json_default)`) anywhere you're returning audit results, since pandas leaks `np.int64`/`Timestamp` into dicts in non-obvious places.

## core/ — audit implementations

**Many modules in `core/` are stubs.** Don't trust the directory listing — check the file. Known stubs that just return `{"message": "...", "results": []}`:

- [core/adp/misc_audits.py](core/adp/misc_audits.py) — emergency, license, timeoff
- [core/misc_audits.py](core/misc_audits.py) — emergency (both vendors), license, timeoff, paycom payment

Real implementations live in: [core/adp/census_audit.py](core/adp/census_audit.py), [core/adp/deduction_audit.py](core/adp/deduction_audit.py), [core/adp/payment_audit.py](core/adp/payment_audit.py), [core/adp/withholding_audit.py](core/adp/withholding_audit.py), [core/adp/total_comparison.py](core/adp/total_comparison.py), [core/adp/prior_payroll_sanity.py](core/adp/prior_payroll_sanity.py), [core/adp/prior_payroll_generator.py](core/adp/prior_payroll_generator.py), [core/adp/prior_payroll_setup_helper.py](core/adp/prior_payroll_setup_helper.py), [core/adp/selective_census_sync.py](core/adp/selective_census_sync.py), [core/paycom/census_audit.py](core/paycom/census_audit.py), [core/paycom/deduction_analyzer.py](core/paycom/deduction_analyzer.py), [core/paycom/total_comparison.py](core/paycom/total_comparison.py), [core/paycom/withholding_audit.py](core/paycom/withholding_audit.py), [core/paycom/sql_master.py](core/paycom/sql_master.py), [core/paycom/prior_payroll_generator.py](core/paycom/prior_payroll_generator.py), [core/paycom/selective_census_sync.py](core/paycom/selective_census_sync.py), [core/common/paycom_consolidated_audit.py](core/common/paycom_consolidated_audit.py), and [core/census/sanity_check.py](core/census/sanity_check.py).

### Prior Payroll Sanity (`core/adp/prior_payroll_sanity.py`)

ADP-only tool ported from the Streamlit `apps/adp/prior_payroll_sanity.py`. Cleans a Prior Payroll export so it can be ingested by downstream APIs:

1. Drops the interleaved `Totals For Associate ID XYZ:` summary rows the ADP report emits between pay-period rows.
2. Detects + removes the bottom-of-file grand-total row where the last employee's ID got bled into the totals.
3. Aggregates per-pay-period exports back to one row per associate when the file has multiple rows per Associate ID.
4. Optionally swaps NET PAY ⇄ TAKE HOME values (default ON) — the Carvan-style API maps these reversed; column headers are NEVER renamed.

Critical: ADP money cells are stored as `=ROUND(x, 2.0)` Excel formulas. `pandas.read_excel` returns null for those, so this module reads with `openpyxl` and runs every cell through `_evaluate_cell` which extracts the literal value from the formula. If you add a new ADP-side reader anywhere else, use the same evaluator or you'll get all-null money columns.

`run_adp_prior_payroll_sanity(content, filename, swap_net_take=True, aggregation_strategy="ask")` returns `(csv_bytes, summary_dict)`.

**`aggregation_strategy="ask"` is now the default.** In ask-mode the orchestrator calls `detect_file_shape(df)` on the cleaned DataFrame and returns `csv_bytes=b""` plus a `summary_dict` whose `mode == "detection_only"`. The summary contains a `facts` block (associates, total_rows, rows_per_associate_max/avg, distinct_pay_dates, date_span_days, period_min/max), a `recommended_strategy` (`"full_quarter"` for ≥80-day per-pay-period files, `"preserve_pay_periods"` for ≤40-day partials, `None` when ambiguous or already aggregated), and a `recommendation_reason` sentence. The MCP handler returns this JSON directly so Claude can show it to the user, get confirmation, and re-call the tool with the explicit strategy. Never silently apply.

`aggregation_strategy="full_quarter"` collapses everything to one row per associate; `"preserve_pay_periods"` keeps distinct pay periods and only merges same-day duplicate row pairs. Output is CSV with the input's exact column headers and column order — the API expects ADP-shape, no renames.

Exposed both via FastAPI (`/audit/adp/prior-payroll-sanity`) and MCP (`adp_prior_payroll_sanity` tool).

### Prior Payroll Setup Helper (`core/adp/prior_payroll_setup_helper.py`)

Reverse-discovers what to configure in Uzio when migrating an ADP client. Given a sanitized ADP Prior Payroll file plus the State Tax Code master CSV, emits an Excel workbook plus a standalone Tax_Mapping CSV.

The Streamlit parent has a peer module at `../apps/adp/prior_payroll_setup_helper.py` (sidebar entry "ADP - Prior Payroll Setup Helper") with identical analysis logic and a UI for interactive review. When fixing analysis bugs (subset-sum tolerance, name heuristic, tax-token map, bonus FLSA test), update both modules; they are deliberately kept in sync.

Key sheets and the algorithms behind them:

- **Earnings_Codes**: every distinct `REGULAR EARNINGS / OVERTIME EARNINGS / ADDITIONAL EARNINGS : XXX-NAME` column with $ total, employee count, paired hours total, and avg rate.
- **Contributions** vs **Deductions**: `VOLUNTARY DEDUCTION :` columns split by name pattern (`401K|403B|457|ROTH|HSA|FSA|RETIRE|K-` → contribution; everything else → deduction).
- **Pre-tax / post-tax verdict** (the load-bearing bit): for each row, `gap_FIT = TOTAL EARNINGS - FEDERAL INCOME - EMPLOYEE TAXABLE`. Try every subset of that row's non-zero deductions; if any subset sums to `gap_FIT` within $0.02, every member is **pre-tax for FIT**. *One positive proof anywhere in the file = pre-tax for everyone* — the rule never varies per employee, per the user's hand-process. Same logic on FICA / MEDI / SIT taxables to derive the flavor: `section_125` (pre-FIT/FICA/MEDI/SIT — medical/dental/vision), `401k_traditional` (pre-FIT/SIT only, NOT pre-FICA/MEDI). Empirically validated against Carvan Q1 (`K-ADP 401K → 401k_traditional`, `75-SUPPORT → post_tax`) and Travel Mgmt Q1 (`MED/DEN/VIS → section_125`, `ADV/IPY/REV/75-SUPPORT → post_tax`). Falls back to a name heuristic only when zero rows are available to test.
- **Tax_Mapping**: produces rows in the exact `Payroll_Mappings_Tax_Mapping_CORRECTED.csv` column order. Federal taxes (FIT / MEDI / FICA / ER_MEDI / ER_FICA / ER_FUTA) get one row each; state-scoped taxes (SIT / SDI / ER_SUTA / FLI) get **one row per distinct WORKED IN STATE present in the file** (multi-state clients respect the SUTA-per-state rule). Lookups use a canonical regex `^\d{2}-000-0000-{TYPE}-000$` against `unique_tax_id` in the State Tax Code master, preferring entries with empty `sub_tax_desc`. `TOTAL EMPLOYEE TAX` / `TOTAL EMPLOYER TAX` aggregate columns are intentionally filtered out before mapping.
- **Bonus_Classification**: FLSA test. For every row with both `BNS-BONUS / BN*` earning AND overtime hours, compute `regular_rate = REGULAR EARNINGS / REGULAR HOURS` then compare actual OT rate to `1.5 × regular_rate`. Tolerance is 0.5%. **Any single row** showing actual OT rate materially above 1.5× → bonus is `non_discretionary` for the whole file (FLSA conservative — once a bonus has inflated the regular rate, it's non-discretionary by IRS rule).

State Tax Code master path defaults to `C:\Users\shobhit.sharma\Downloads\State Tax Code.csv`; can be overridden via `state_tax_master_path` or `state_tax_master_base64`.

Exposed via FastAPI (`/audit/adp/prior-payroll-setup-helper`) and MCP (`adp_prior_payroll_setup_helper` tool). Output also writes the Tax_Mapping CSV to the audit inbox alongside the Excel workbook so it can be uploaded directly to the next migration step.

### Prior Payroll Generator (`core/{adp,paycom}/prior_payroll_generator.py`)

Ports of the Streamlit `apps/{adp,paycom}/prior_payroll_generator.py` tools. Both fill a blank Uzio Prior Payroll Template (.xlsm) from up to 10 source files. Auto-mapping uses a fuzzy-string heuristic (`auto_guess_mapping`) with domain boosts for Medicare / Social Security / FIT / 401k / FUTA / SUI / SDI / regular / overtime / bonus / state-income. Each tool accepts an `override_mapping` parameter:

- ADP keys are simple `{adp_column_name: uzio_col_idx}`.
- Paycom keys are `{"type_code|type_description": uzio_col_idx}` (string with a `|` separator) since JSON object keys can't be tuples.

Negative `uzio_col_idx` force-skips that column. Net Pay is auto-routed to whichever Uzio column header contains `"net pay"`. Validation flags any employee-period where `Gross − Taxes − Deductions ≠ Net Pay` (returned in the response, capped at 200 rows).

### Selective Census Sync (`core/{adp,paycom}/selective_census_sync.py`)

Port of the Streamlit `apps/{adp,paycom}/census_generator.py`'s `render_selective_census_generator` entry point. Updates ONLY the columns named in `selected_uzio_cols` (keys from `UZIO_RAW_MAPPING`) in a pre-filled Uzio Census Template (.xlsm), leaving every other column / sheet / VBA macro untouched. Source-side IDs are normalized via `norm_key_series` for matching.

Job Title and Work Location are special: callers pass an explicit `{source_value: uzio_value}` dict, pass `{}` to seed automatically from the existing template (via `extract_mappings_from_uzio` which walks the current Uzio data to learn the convention), or omit to skip syncing those columns. `discover_only=true` short-circuits to return the seed mappings + unique source values for review.

### Paycom Consolidated Audit (`core/common/paycom_consolidated_audit.py`)

Port of the Streamlit `apps/common/paycom_combined_audit.py` tool. Runs Census + Payment + Emergency contact audits in one pass against the Uzio Master Custom Report (CSV with category labels in row 1, headers in row 2) and a Paycom Census export. Plus six anomaly extracts (salaried-driver exceptions, FLSA compliance, active-missing, terminated-missing, data quality, high-rate anomalies) and duplicate-SSN warnings. Output is 11 sheets via `save_results_to_excel`.

Internal helper `_detect_duplicate_ssns_with_ids(df, id_col, ssn_col)` lives inside this module rather than in `utils/audit_utils.py` because the existing `detect_duplicate_ssns(df, ssn_col)` in utils has a different signature kept stable for `core/adp/census_audit.py`. Don't merge them.

### Selective Extraction

The `selective_employee_extractor` tool in `mcp_server.py` allows for targeted audits by extracting specific employee rows from a large census or payroll file based on a list of IDs. This is critical for investigating "Active in Payroll but Missing in Uzio" cases flagged by the census audits.

### Utility Tools

- `list_audit_files`: Scans the `AUDIT_INBOX` or any specified directory to discover files.
- `read_audit_report`: Reads full Excel/CSV reports back into Claude. This is essential for analyzing the results of a previous audit without manually copying data.

### Misc-audit import-shadowing in main.py

`main.py` imports the misc audits **twice** — first from `core.adp.misc_audits` near the top, then later does `from core.misc_audits import run_adp_emergency_audit, run_paycom_emergency_audit, ...` which **overrides** the earlier names with the stub versions. So today the `/audit/adp/emergency`, `/audit/adp/license`, `/audit/adp/timeoff`, `/audit/paycom/emergency`, `/audit/paycom/timeoff`, and `/audit/paycom/payment` endpoints all return placeholder results. If you're asked to "fix" one of these audits, the work is to write the real logic in `core/misc_audits.py` (or port it from the Streamlit `apps/{adp,paycom}/*_audit.py` siblings).

### Field maps live next to the audit

`ADP_FIELD_MAP` is in [core/adp/census_audit.py](core/adp/census_audit.py:9), `PAYCOM_FIELD_MAP` is in [core/paycom/census_audit.py](core/paycom/census_audit.py:8). Other modules import these by name — keep them as module-level dicts, don't move them into a config file without updating the `mcp_server.py` and `main.py` imports.

## utils/audit_utils.py — shared engine

[utils/audit_utils.py](utils/audit_utils.py) is a slimmed-down version of the Streamlit project's helper module. It contains read/normalize utilities (`norm_col`, `norm_colname`, `norm_blank`, `norm_ssn_canonical`, `norm_id`, `norm_key_series`, `read_uzio_raw_file`, `find_header_and_data`, identity-matching helpers), the Uzio template injector (`inject_into_uzio_template`), and the selective-census-sync helpers (`read_uzio_template_df`, `extract_mappings_from_uzio`, `selective_update_uzio`).

It does **not** contain the full Uzio template generator (`generate_uzio_template`) — that's Streamlit-only.

If you find yourself wanting to import something from `utils/audit_utils.py` that isn't there, it probably exists in the Streamlit parent's `utils/audit_utils.py` and needs to be ported.

`detect_duplicate_ssns(df, ssn_col)` returns a DataFrame; the streamlit version has a different `(df, id_col, ssn_col)` signature returning a `{ssn: [ids]}` dict. Don't merge them — `core/adp/census_audit.py` depends on the current shape, and `core/common/paycom_consolidated_audit.py` defines its own `_detect_duplicate_ssns_with_ids` for the streamlit-style result.

## File I/O conventions (carried over from the Streamlit project)

- All source data is read with `dtype=str` to preserve leading zeros (SSN, zip, employee IDs).
- Excel/CSV uploads are sniffed by extension in `find_header_and_data()` and the per-tool loaders.
- `read_uzio_raw_file()` reads sheet `'Employee Details'` with `header=3` — Uzio raw exports always have a 3-row preamble before the column headers.
- `find_header_and_data()` scans the first 50 rows for `"employee id" / "employee name" / "associate id"` to locate the real header row. ADP exports often have a banner row above the data.

## Census Sanity auto-fix pipeline

[core/census/sanity_check.py](core/census/sanity_check.py) ports the Streamlit `render_auto_fix_options` toggles into a single function: `generate_corrected_census_xlsx(content, field_map_dict, fix_options=...)` returns `(xlsx_bytes, summary)`. The function is exposed both via FastAPI (`/audit/adp/census-sanity`) and MCP (`adp_census_sanity` tool).

Toggle keys (mirror the Streamlit checkbox keys): `fix_flsa`, `fix_emails`, `fix_job_title`, `fix_driver_smart`, `fix_license`, `fix_status`, `fix_inactive` (alias of `fix_status`), `fix_type`, `fix_dol_status`, `fix_leave_to_active`, `fix_blank_jt_to_driver`, `fix_std_hours`, `rename_std_hours`, `fix_zip`, `rename_zip_col`, `replace_gender_col`, plus `sort_by_manager`. All default `False`.

Note one intentional divergence from the Streamlit code: the Job-Title-from-Department fix honors **both** `fix_job_title` and `fix_position` keys because the Streamlit dict uses `fix_position` while the toggle UI uses `fix_job_title` — see comment in `generate_corrected_census_xlsx`.

The sanity validator (`run_census_sanity_check` / `validate_source_data`) is intentionally lightweight — it only flags hard errors (missing Employee ID, SSN, Employment Status). Per-row warnings are produced separately by `_validate_for_warnings()` and injected into a `CRITICAL_WARNINGS` column on the corrected output. Don't merge the two — sanity reporting must stay read-only, fixes are opt-in.

## CORS and deployment

`main.py` enables fully-open CORS (`allow_origins=["*"]`). This is intentional for the Vercel + Claude Desktop setup. If deploying anywhere internet-facing, lock this down before exposing any endpoint that mutates state.
