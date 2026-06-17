# CLAUDE.md (v2.0)

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Setting this up on a new machine?** See [SETUP.md](SETUP.md) — the step-by-step SOP for installing dependencies and registering the server in Claude Desktop. To package it as a one-click `.mcpb` extension for non-technical users, see [BUNDLE.md](BUNDLE.md).

## Scope of this directory

`audit_fast_api/` is a separate Python project from the parent `Deduction Tool/` Streamlit app — it has its own `.git`, its own `requirements.txt`, and its own `core/` reimplementations of the audit logic. The parent's [CLAUDE.md](../CLAUDE.md) and [README.md](../README.md) describe the Streamlit tool; this document covers only the **local MCP service**.

> **Architecture note (v2.0):** the FastAPI HTTP layer and the Vercel/SSE deployment were removed. This is now a **local, stdio-only MCP server** meant to run on the user's machine and connect to the Claude Desktop app. There is no web server, no network surface, and no `main.py`. The historical name `audit_fast_api` is kept only because that's the git remote.

**Three-repo topology (read the parent CLAUDE.md "Repository topology" section before pushing anything):** the parent working directory holds THREE independent git repos as nested folders — root `shobhit73/Unified_Audit_Tool`, `implementors_repo/` → `shobhitsharma-rgb/unified_audit_for_implementors` (a full byte-identical mirror of the Streamlit app, its own deployment), and this `audit_fast_api/` → `shobhit73/audit_fast_api_`. They are NOT submodules. A change that belongs in more than one repo must be committed and pushed to each remote separately — pushing only the root silently leaves the other deployments on stale code.

When fixing a bug that exists in both, check the Streamlit `apps/{adp,paycom}/*.py` modules — most of the audit semantics here were ported from there and may need the same fix in both trees (see commit `f254a50` for an example: the ADP Census Sanity auto-fix pipeline was mirrored from the Streamlit `census_generator.py` into [core/census/sanity_check.py](core/census/sanity_check.py)).

## Run

```bash
pip install -r requirements.txt

# Local MCP server over stdio. Claude Desktop launches this for you once it is
# registered in claude_desktop_config.json (see SETUP.md) — you rarely run it by
# hand, but this is the exact command Claude Desktop invokes:
python mcp_server.py
```

A bare `python mcp_server.py` will start and then sit silently waiting for a
client to speak the MCP protocol on stdin — that "hang" is correct, not a crash.
There are no tests, no linter, no build step.

## One entry point: [mcp_server.py](mcp_server.py) — local MCP server (stdio)

`mcp_server.py` is the whole service. It registers every audit as an MCP tool
(`@server.list_tools()`), dispatches calls (`@server.call_tool()`), and runs over
the **stdio** transport (`run_stdio()` under `if __name__ == "__main__"`) — the
transport Claude Desktop speaks to a local MCP server. There is no HTTP/SSE
transport and no FastAPI app any more.

**Protecting the stdout protocol channel — placement matters.** Stdio MCP uses
stdout as the JSON-RPC channel, so a stray `print()` to stdout would corrupt it.
The guard is `sys.stdout = sys.stderr`, but it **must run INSIDE `run_stdio()`,
after `stdio_server()` has been entered** — `stdio_server()` wraps `sys.stdout.buffer`
at entry, so redirecting *before* that (e.g. at module import) sends the protocol
itself to stderr and the client times out with "Could not attach to MCP server".
Never move this redirect to the top of the file. Route diagnostics to stderr or
`logging`, never `print` to stdout.

#### The audit-inbox drop-folder pattern

```python
AUDIT_INBOX = os.environ.get("AUDIT_INBOX") or os.path.join(
    os.path.expanduser("~"), "Desktop", "Audit Files"
)
```

The inbox is **portable**: it defaults to the *running user's* `Desktop/Audit Files`
folder and can be overridden with the `AUDIT_INBOX` environment variable (set it in
the `"env"` block of `claude_desktop_config.json`). Do NOT hardcode an absolute
user path here again — that was the old behavior and it broke on every machine that
wasn't the original author's.

**MANDATORY**: 100% of tool output files are consolidated in this folder. Claude
Desktop must always check it via `list_audit_files` to find generated reports.

#### Data Correction & Formatting
The `apply_data_corrections` tool uses `openpyxl` to perform row-level updates based on **Employee ID**. This tool is specifically designed to **preserve all original Excel formatting** (colors, fonts, borders). Always prioritize this for "Implementer Overrides" over standard Pandas-based re-writes.

#### File input convention (every MCP tool)

Each tool accepts both a local path *and* a base64 fallback — see `load_file()` and `load_files_list()` in [mcp_server.py](mcp_server.py). Path is preferred; base64 is for HTTP/remote callers. Don't add a tool that only accepts one of them — keep the pair.

#### Output convention

`save_results_to_excel()` writes results to `<audit_inbox>/<Prefix>_<timestamp>.xlsx` and returns a JSON summary with a top-10 preview. The MCP response is the summary — the full report is the file on disk. Keep this contract: streaming megabytes of audit results back through MCP will exceed token limits.

**Single-sheet tools also get a parallel CSV.** When the helper is called with a list-of-dicts (one logical sheet), it writes both `<Prefix>_<timestamp>.xlsx` AND `<Prefix>_<timestamp>.csv` to the audit inbox, and the returned summary includes a `csv_file_path` key. The CSV is **plain UTF-8 with NO byte-order mark** — see the "CSV output rule" section further down for the non-negotiable detail.

**Multi-sheet tools (dict-of-lists input) are XLSX-only.** Per product decision, exploding an 11-sheet workbook into 11 sibling CSVs creates more audit-inbox noise than it's worth, and downstream APIs that need machine-ingestable data should consume one of the per-sheet outputs upstream of the workbook. If you find yourself needing a CSV from a specific tab of a multi-sheet audit, refactor that tab's logic into its own single-sheet tool, don't expand `save_results_to_excel` to dump per-sheet CSVs.

`_json_default()` handles numpy/pandas/datetime serialization. Use it (`json.dumps(..., default=_json_default)`) anywhere you're returning audit results, since pandas leaks `np.int64`/`Timestamp` into dicts in non-obvious places.

## core/ — audit implementations

**Every audit wired into the MCP server has a real implementation.** Earlier versions shipped placeholder stubs for the "misc" audits; those are now real and are what the live tools call:

- [core/adp/misc_audits.py](core/adp/misc_audits.py) — `run_adp_emergency_audit`, `run_adp_license_audit`, `run_adp_timeoff_audit` (real comparison / balance-update logic).
- [core/paycom/misc_audits.py](core/paycom/misc_audits.py) — `run_paycom_emergency_audit`, `run_paycom_timeoff_audit` (real).

> **Dead file:** [core/misc_audits.py](core/misc_audits.py) still exists but is imported by **nothing**. It held the old placeholder stubs that the now-deleted `main.py` pulled in via import-shadowing. The MCP server uses the per-vendor `core/{adp,paycom}/misc_audits.py` modules above instead. Safe to delete.

Real implementations live in: [core/adp/census_audit.py](core/adp/census_audit.py), [core/adp/deduction_audit.py](core/adp/deduction_audit.py), [core/adp/payment_audit.py](core/adp/payment_audit.py), [core/adp/withholding_audit.py](core/adp/withholding_audit.py), [core/adp/total_comparison.py](core/adp/total_comparison.py), [core/adp/prior_payroll_sanity.py](core/adp/prior_payroll_sanity.py), [core/adp/payment_method_sanity.py](core/adp/payment_method_sanity.py), [core/adp/fit_sit_sanity.py](core/adp/fit_sit_sanity.py), [core/adp/prior_payroll_setup_helper.py](core/adp/prior_payroll_setup_helper.py), [core/adp/payroll_setup_agent.py](core/adp/payroll_setup_agent.py), [core/adp/selective_census_sync.py](core/adp/selective_census_sync.py), [core/paycom/census_audit.py](core/paycom/census_audit.py), [core/paycom/total_comparison.py](core/paycom/total_comparison.py), [core/paycom/withholding_audit.py](core/paycom/withholding_audit.py), [core/paycom/prior_payroll_setup_helper.py](core/paycom/prior_payroll_setup_helper.py), [core/paycom/selective_census_sync.py](core/paycom/selective_census_sync.py), [core/common/paycom_consolidated_audit.py](core/common/paycom_consolidated_audit.py), [core/common/adp_combined_audit.py](core/common/adp_combined_audit.py), [core/adp/misc_audits.py](core/adp/misc_audits.py), [core/paycom/misc_audits.py](core/paycom/misc_audits.py), and [core/census/sanity_check.py](core/census/sanity_check.py).

### Payment Method Sanity (`core/adp/payment_method_sanity.py`)

ADP-only single-file tool, a near-verbatim port of the Streamlit `apps/adp/payment_method_sanity.py` (root repo, sidebar entry "ADP - Payment Method Sanity Check"). The ONLY differences are I/O shape — this version takes `(content: bytes, filename: str)` and exposes `run_adp_payment_method_sanity(content, filename) -> (xlsx_bytes, csv_bytes, summary_dict)`. The distribution-rule engine (`_fix_employee` and friends) is kept byte-for-byte in sync with the Streamlit module — **fix rule bugs in BOTH places.**

Validates one ADP payment-method / direct-deposit export against Uzio's deposit-distribution rules and auto-corrects unsupported configs: R2 percent distribution (exactly one Full + Partial % rows summing to 100%), R3 amount distribution (exactly one Full remainder + Partial amount rows), R4 mixed percent+amount (Uzio-unsupported → keep % rows, split the remainder equally across the non-percent accounts), R5 lone Partial/Partial % → Full with amount+percent cleared. Output: a 4-sheet XLSX (Summary, Issues, Before_After, Corrected_Source) plus a Corrected_Source CSV (plain UTF-8, NO BOM — the CSV is for API ingestion, route Excel users to the XLSX). Both land in the audit inbox.

Exposed as the MCP tool `adp_payment_method_sanity`. This is NOT the two-file Uzio-vs-ADP comparison — that's `adp_payment_audit`. Guarded with `require_vendor(..., "adp", ...)`; an ADP payment export carries `Associate ID` (a decisive ADP marker) so it passes, while Paycom/Uzio files are rejected.

### FIT/SIT Sanity (`core/adp/fit_sit_sanity.py`)

ADP-only single-file tool, a verbatim port of the Streamlit `apps/adp/fit_sit_sanity.py` (sidebar entry "ADP - FIT/SIT Sanity Check"). Fills blanks in exactly three columns with the defaults Uzio expects so the file is API-ready: `Dependents → 0`, `Non-Resident Alien → No`, `State Marital Status Description → Single`. Everything else is left untouched; errors out if any of the three columns is missing. `run_adp_fit_sit_sanity(content, filename) -> (xlsx_bytes, csv_bytes, summary_dict)` — XLSX has Summary/Changes/Corrected_Source sheets, CSV is the corrected source (plain UTF-8, NO BOM). Exposed as the MCP tool `adp_fit_sit_sanity`, guarded with `require_vendor(..., "adp", ...)`. Keep the fill logic in sync with the Streamlit module.

### Payroll Setup Agent (`core/adp/payroll_setup_agent.py`)

ADP-only, the "ADP Payroll Analyzer" ported from the Streamlit `apps/adp/payroll_setup_agent.py` (sidebar entry "ADP - Payroll Setup Agent"). Three analyses: **Earnings Classifier** (Hourly vs Flat split + a *statistical* Discretionary/Non-Discretionary test: `avg(actual_OT − 1.5×base) > 0.15` AND `median > 0.05`), **Tax Mapping** (ADP tax columns → Uzio codes in the "Hansen format", federal + one row per worked-in state), and **Deduction Classifier** (Pre/Post-tax via per-row subset-sum on `GAP = Total Earnings − Federal Income Taxable`, decided by 60% majority across rows).

**This is deliberately distinct from `adp_prior_payroll_setup_helper`.** Same concerns, different algorithms and output format: the setup helper uses the conservative "one positive proof anywhere = pre-tax / non-discretionary for the whole file" rules and the `Payroll_Mappings_Tax_Mapping_CORRECTED` tax format; the agent uses statistical majority verdicts and the Hansen tax format. Both are kept.

**TWO-STEP, ASK-FIRST FLOW — do NOT assume or default.** This mirrors the Streamlit Tax Mapping tab's on-screen state multiselect. Called WITHOUT `selected_states`, the MCP tool returns a **discovery** payload (`step: "discover"` — detected states are a HINT only, plus the available states from the master, and the tax/earnings/deduction codes found) and writes nothing. The caller MUST get the user's explicit confirmed state list and re-call with `selected_states=[...]`. Nothing is defaulted to the detected states, and **no bundled State Tax CSV is silently used** — the master must be supplied via `state_tax_master_path` / `state_tax_master_base64` / `STATE_TAX_MASTER_PATH`; the apply call raises a clear error if it's missing. Exposed as `adp_payroll_setup_agent`.

Two intentional divergences from the naive Streamlit code, both required by repo conventions: (1) the payroll file is read with `read_input_bytes` (the `=ROUND()` formula evaluator) instead of a bare `pd.read_excel`, so money cells aren't all-null on real ADP exports; (2) the state selection is an explicit two-step ask instead of a defaulted multiselect. The classification math is otherwise a byte-for-byte port — `discover_adp_payroll_setup_agent(...)` and `run_adp_payroll_setup_agent(content, filename, selected_states, state_tax_master_content) -> (xlsx_bytes, csv_outputs, summary)`.

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

Exposed as the MCP tool `adp_prior_payroll_sanity`.

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

Exposed as the MCP tool `adp_prior_payroll_setup_helper`. Output also writes the Tax_Mapping CSV to the audit inbox alongside the Excel workbook so it can be uploaded directly to the next migration step.

### Prior Payroll Setup Helper -- Paycom (`core/paycom/prior_payroll_setup_helper.py`)

**Replaces the deleted `paycom_deduction_analyzer` tool.** Mirror of the ADP version with one big simplification: Paycom's Scheduled Deductions report has a `Tax Treatment` column that explicitly labels each deduction's tax handling, so the empirical subset-sum algorithm the ADP helper uses is unnecessary here. Read the column directly:

| Tax Treatment value | Verdict | Flavor |
|---|---|---|
| starts with `B` (e.g. `B - S125 Pre-Tax`) | PRE-TAX | Section 125 |
| starts with `H` (e.g. `H - FICA/FUTA/SUTA Taxable Only (401k)`) | PRE-TAX | 401k traditional |
| starts with `A` (e.g. `A - After Tax Deduction`) | POST-TAX | (none) |

`run_paycom_prior_payroll_setup_helper(prior_content, prior_filename, scheduled_content, scheduled_filename)` returns `(results_dict, xlsx_bytes)`. The xlsx is the same 3-tab simplified output as the ADP version (Tab 1 What to Set Up, Tab 2 Pre-Tax vs Post-Tax, Tab 3 Bonus Verdict).

**Bonus FLSA test (Strategy A+C)**: Paycom's Prior Payroll Register has no hours column, so the standard `OT_pay = 1.5 × (REG_pay / REG_hours) × OT_hours` test can't run. Instead use Paycom's own `WOT` (Weighted Overtime) calc as the signal: WOT is Paycom's FLSA-correct OT pay. If both plain `OT` and `WOT` lines exist for an employee+period AND they differ by >0.5%, Paycom internally rolled a bonus into the regular rate => **non-discretionary**. When the differential test cannot run (only WOT, only OT, no bonus codes), return `indeterminate` and tell the caller to supply a Payroll Register Detail with hours.

The deleted `core/paycom/deduction_analyzer.py` was a 54-line stub that returned a "Simplified logic for demonstration" message; the real logic lived in the Streamlit version (934 lines, very complex). The new tool replaces both with a tight ~350-line core module.

Exposed as the MCP tool `paycom_prior_payroll_setup_helper` (and in the Streamlit app as `apps/paycom/prior_payroll_setup_helper.py`, sidebar entry "Paycom - Prior Payroll Setup Helper").

### Selective Census Sync (`core/{adp,paycom}/selective_census_sync.py`)

Port of the Streamlit `apps/{adp,paycom}/census_generator.py`'s `render_selective_census_generator` entry point. Updates ONLY the columns named in `selected_uzio_cols` (keys from `UZIO_RAW_MAPPING`) in a pre-filled Uzio Census Template (.xlsm), leaving every other column / sheet / VBA macro untouched. Source-side IDs are normalized via `norm_key_series` for matching.

Job Title and Work Location are special: callers pass an explicit `{source_value: uzio_value}` dict, pass `{}` to seed automatically from the existing template (via `extract_mappings_from_uzio` which walks the current Uzio data to learn the convention), or omit to skip syncing those columns. `discover_only=true` short-circuits to return the seed mappings + unique source values for review.

### Paycom Consolidated Audit (`core/common/paycom_consolidated_audit.py`)

Port of the Streamlit `apps/common/paycom_combined_audit.py` tool. Runs Census + Payment + Emergency contact audits in one pass against the Uzio Master Custom Report (CSV with category labels in row 1, headers in row 2) and a Paycom Census export. Plus six anomaly extracts (salaried-driver exceptions, FLSA compliance, active-missing, terminated-missing, data quality, high-rate anomalies) and duplicate-SSN warnings. Output is 11 sheets via `save_results_to_excel`.

Internal helper `_detect_duplicate_ssns_with_ids(df, id_col, ssn_col)` lives inside this module rather than in `utils/audit_utils.py` because the existing `detect_duplicate_ssns(df, ssn_col)` in utils has a different signature kept stable for `core/adp/census_audit.py`. Don't merge them.

### ADP Consolidated Audit (`core/common/adp_combined_audit.py`)

Port of the Streamlit `apps/common/adp_combined_audit.py` tool (MCP tool `adp_consolidated_audit`). Unlike the self-contained Paycom consolidated module, this one is a thin **orchestrator**: it does NOT re-implement the audit logic. Instead it reshapes the Uzio HR Report into the per-audit file shapes each ADP core reader expects (`_adapt_census` → `read_uzio_raw_file`'s `Employee Details`/header-row-4 shape; `_adapt_payment`/`_adapt_emergency` → header-row-2; `_adapt_license` → header-row-1 with an `Employee ID` cell), then calls the existing `run_adp_census_audit`, `run_adp_payment_audit`, `run_adp_emergency_audit`, and `run_adp_license_audit` and merges their sheets under section prefixes (`CEN_` / `DD_` / `EC_` / `LIC_`). It reuses `read_uzio_master` from `paycom_consolidated_audit.py`.

The Uzio HR Report is required; each ADP file is optional (the Emergency + License Details report drives BOTH the emergency and license audits). Per-audit failures are isolated into an `_Errors` sheet rather than aborting the run. Roll-up summary sheets (`Summary_Metrics`, payment/emergency `Summary`) are dropped to match the Streamlit tool's no-summary output. The census adapter resolves the HR Report's `Reporting Manager` NAME to the manager's Employee ID (via `build_manager_name_to_id` / `resolve_manager_id`) so the census audit compares it ID-vs-ID. **No FastAPI endpoint** — MCP-only, same as the Paycom consolidated.

### Selective Extraction

The `selective_employee_extractor` tool in `mcp_server.py` allows for targeted audits by extracting specific employee rows from a large census or payroll file based on a list of IDs. This is critical for investigating "Active in Payroll but Missing in Uzio" cases flagged by the census audits.

### Utility Tools

- `list_audit_files`: Scans the `AUDIT_INBOX` or any specified directory to discover files.
- `read_audit_report`: Reads full Excel/CSV reports back into Claude. This is essential for analyzing the results of a previous audit without manually copying data.

### Field maps live next to the audit

`ADP_FIELD_MAP` is in [core/adp/census_audit.py](core/adp/census_audit.py:9), `PAYCOM_FIELD_MAP` is in [core/paycom/census_audit.py](core/paycom/census_audit.py:8). Other modules import these by name — keep them as module-level dicts, don't move them into a config file without updating the `mcp_server.py` imports.

## utils/audit_utils.py — shared engine

[utils/audit_utils.py](utils/audit_utils.py) is a slimmed-down version of the Streamlit project's helper module. It contains read/normalize utilities (`norm_col`, `norm_colname`, `norm_blank`, `norm_ssn_canonical`, `norm_id`, `norm_key_series`, `read_uzio_raw_file`, `find_header_and_data`, identity-matching helpers), the Uzio template injector (`inject_into_uzio_template`), and the selective-census-sync helpers (`read_uzio_template_df`, `extract_mappings_from_uzio`, `selective_update_uzio`).

It does **not** contain the full Uzio template generator (`generate_uzio_template`) — that's Streamlit-only.

If you find yourself wanting to import something from `utils/audit_utils.py` that isn't there, it probably exists in the Streamlit parent's `utils/audit_utils.py` and needs to be ported.

`detect_duplicate_ssns(df, ssn_col)` returns a DataFrame; the streamlit version has a different `(df, id_col, ssn_col)` signature returning a `{ssn: [ids]}` dict. Don't merge them — `core/adp/census_audit.py` depends on the current shape, and `core/common/paycom_consolidated_audit.py` defines its own `_detect_duplicate_ssns_with_ids` for the streamlit-style result.

## File I/O conventions (carried over from the Streamlit project)

- All source data is read with `dtype=str` to preserve leading zeros (SSN, zip, employee IDs).
- Excel/CSV uploads are sniffed by extension in `find_header_and_data()` and the per-tool loaders.
- `read_uzio_raw_file()` reads sheet `'Employee Details'` with `header=3` — Uzio raw exports always have a 3-row preamble before the column headers.
- `find_header_and_data()` scans the first 50 rows for `"employee id" / "employee name" / "associate id"` to locate the real header row. ADP exports often have a banner row above the data. **It defaults to pandas type inference (`dtype=None`)** — an all-numeric column like ROUTING NUMBER comes back as `int64`, so `011000015` loses its leading zero at read time, and a later `.astype(str)` is too late. Pass `dtype=str` for any leading-zero-sensitive consumer (the `selective_employee_extractor` does); leave the default for the SQL / money-math callers (`query_data_sql`, `get_file_schema`, `total_comparison`) that need numeric types.

### CSV output rule — NEVER WRITE A UTF-8 BOM. **NON-NEGOTIABLE.**

Same rule as the root repo. Every CSV this service produces (MCP-tool outputs, files written to the audit inbox) MUST be plain UTF-8 with NO byte-order mark. The downstream customer API matches the first column header *literally* (`Associate ID`, `Employee_Code`, ...), so a BOM smuggles `U+FEFF` in front of the first header and the column lookup silently misses. Customer-impacting incident already shipped (Skyland, May 2026).

- ❌ `df.to_csv(...).encode("utf-8-sig")` / `df.to_csv(path, encoding="utf-8-sig")`
- ✅ `df.to_csv(...).encode("utf-8")` / `df.to_csv(path, encoding="utf-8")` / `df.to_csv(...)` (pandas default is fine)

The "Excel needs the BOM" rationale is not valid: every tool that emits a CSV also emits an XLSX from the same DataFrame — route Excel users to the XLSX. The CSV is for API ingestion only.

Before merging any change that calls `to_csv` or writes a `.csv`, grep the diff for `utf-8-sig` / `utf_8_sig`. They must not appear in encoding arguments (comments referencing this rule are fine).

## Census Sanity auto-fix pipeline

[core/census/sanity_check.py](core/census/sanity_check.py) ports the Streamlit `render_auto_fix_options` toggles into a single function: `generate_corrected_census_xlsx(content, field_map_dict, fix_options=...)` returns `(xlsx_bytes, summary)`. The function is exposed as the MCP tool `adp_census_sanity`.

Toggle keys (mirror the Streamlit checkbox keys): `fix_flsa`, `fix_emails`, `fix_job_title`, `fix_driver_smart`, `fix_license`, `fix_status`, `fix_inactive` (alias of `fix_status`), `fix_type`, `fix_dol_status`, `fix_leave_to_active`, `fix_blank_jt_to_driver`, `fix_std_hours`, `rename_std_hours`, `fix_zip`, `rename_zip_col`, `replace_gender_col`, plus `sort_by_manager`. All default `False`.

Note one intentional divergence from the Streamlit code: the Job-Title-from-Department fix honors **both** `fix_job_title` and `fix_position` keys because the Streamlit dict uses `fix_position` while the toggle UI uses `fix_job_title` — see comment in `generate_corrected_census_xlsx`.

The sanity validator (`run_census_sanity_check` / `validate_source_data`) is intentionally lightweight — it only flags hard errors (missing Employee ID, SSN, Employment Status). Per-row warnings are produced separately by `_validate_for_warnings()` and injected into a `CRITICAL_WARNINGS` column on the corrected output. 

**Recent Critical Logic Updates:**
1.  **Leave/Inactive Handling**: `fix_leave_to_active` logic now converts "On Leave" or "Inactive" employees to "Active" if the termination date is missing, adding the comment *"Please make it exclude from payroll in Uzio"*. If a termination date is present, they are converted to "Terminated".
2.  **Forced Driver FLSA**: Any job title containing "Driver" or "Helper" is forced to **Non-Exempt** and **Hourly**, overriding source data.
3.  **Standardization**: Standardized "Full-Time" to "Full Time" and auto-fixes "Fian..." relationships to "Fiancee".

## Deployment

There is no deployment. This is a **local stdio MCP server** — Claude Desktop spawns
`python mcp_server.py` as a child process and talks to it over stdin/stdout. No web
server, no ports, no CORS, no network surface. Everything runs on the user's machine
against local files in the audit inbox. To put it on another machine, follow
[SETUP.md](SETUP.md); there is nothing to host.
