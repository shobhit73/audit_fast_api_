# Claude Desktop - Multi-Agent Payroll Migration SOP (v1.5)
Before starting any audit or analysis, you **must** verify the data location.
1.  **Data Access**: You can access files from any local path (e.g., `Downloads`, client folders). Using the `copy_to_audit_inbox` tool is optional but recommended to keep inputs and outputs in `C:\Users\shobhit.sharma\Desktop\Audit Files`.
2.  **Large Files (>1MB)**: Use the **Side-Car DB Strategy (DuckDB)**. Never attempt to read a file >1MB fully into context.
    *   Call `get_file_schema` to identify columns.
    *   Call `query_data_sql` to extract specific employees or calculate totals using SQL.
3.  **Confirm Consent**: Never apply `fix_` toggles in Sanity tools without explicit user approval for each correction.

## 2. Analysis Agent (Trigger & Orchestration)
**Trigger**: A new email from an implementer (e.g., Mercedes, Kadence) with census issues, or a manual request to "Audit Client X".
1.  **Monitor**: Scan Gmail for the latest issue logs or resolutions for the specified client.
2.  **Intelligence**: Parse the email to extract:
    *   List of affected **Employee IDs**.
    *   **Required Corrections** (e.g., "Change status to Inactive", "Fix FLSA for Driver roles").
3.  **Plan**: Identify which core audit tools are needed (e.g., `paycom_census_sanity`, `apply_data_corrections`).

## 2. Ingestion & Extraction Agent
**NOTE**: You can read from any local folder provided by the user. Use the `path` field from `list_audit_files` to ensure accuracy.
1.  **Copy**: Move master census/payroll files from the client's source folder (e.g., `Downloads/Happy Delivery`) to the local Desktop inbox using the **`copy_to_audit_inbox`** tool.
2.  **Verify**: Use `list_audit_files` to confirm files are ready in `C:\Users\shobhit.sharma\Desktop\Audit Files`.
3.  **Isolate**: Call `selective_employee_extractor` to pull only the problematic employees into a temporary "Working Set" CSV/Excel.

## 4. Correction & Sanity Agent
1.  **Sanity First**: Run `paycom_census_sanity` or `adp_census_sanity` with **all toggles OFF** first to identify standard errors.
2.  **Implementer Override**: 
    *   **Tool**: `apply_data_corrections`
    *   **Action**: Apply the manual resolutions from Gmail/Implementers directly to the master file or working set.
    *   **Formatting**: This tool **preserves 100% of formatting**, making it the "Source of Truth" for final uploads.
    *   **Strict ID**: You MUST provide an Employee ID for every correction.
3.  **Finalize**: Save the corrected report to the `Audit Files` folder with an `_OVERRIDDEN` suffix.

## 4. Communication & Reporting Agent
1.  **Deep Read**: Use `read_audit_report` to analyze the final corrected file for any remaining anomalies.
2.  **Summarize**: Create a concise summary of all changes made.
3.  **Reply**: Draft or send a Gmail reply to the implementer (Mercedes/Kadence):
    *   Confirming which IDs were fixed.
    *   Attaching/Referencing the corrected filename in the `Audit Files` folder.
    *   Highlighting any unresolved assumptions.

## 5. Advanced Workflows

### 5.1 Multi-Tool Cross-Reference (Deep Dive)
**Scenario**: User asks for a deep dive into a specific employee after multiple audits (e.g., "Check Shobhit's status in both Census and Payment audits").
1.  **Ingest**: Ensure all source files (Census ADP/Uzio, Payment ADP/Uzio) are copied to the `Audit Files` inbox.
2.  **Audit**: Run all relevant audits (e.g., `adp_census_audit`, `adp_payment_audit`).
3.  **Analyze**:
    *   Use `read_audit_report` to open the newly generated reports from the `Audit Files` folder.
    *   Search for the specific employee in both reports.
    *   Summarize mismatches across both domains (e.g., "Active in Census but unpaid in Payment report").
4.  **Resolve**: Draft a Gmail to the implementer (Mercedes) detailing the specific cross-domain discrepancies for that employee.

### 5.2 Gmail-Driven Corrections
**Scenario**: Implementers provide resolutions or data updates via Gmail.
1.  **Identify**: Use Gmail to find threads regarding census audits.
2.  **Extract**: Identify Employee IDs and required changes.
3.  **Override**: Use `apply_data_corrections` to surgically update the master file while preserving formatting.

### 5.3 API Error Handling
**Scenario**: The migration API returns a JSON error listing failing Employee IDs.
1.  **Parse**: Extract IDs from the error JSON.
2.  **Analyze**: Compare against the error message, identify the fix.
3.  **Correct**: Use `apply_data_corrections` to fix the IDs in the source file.

## 6. Client Specific Audits
If the user mentions a specific client (e.g., "Happy Delivery"):
1.  Scan the folder using `list_audit_files`.
2.  **CRITICAL**: Always move/copy files to the `Audit Files` folder on the Desktop first. **Never** audit files directly from remote locations.

## 7. Reporting & Communication
*   **Action**: Summarize all corrections made and the final status of problematic records.
*   **Verification**: All tool output reports are **MANDATORY** saved to the `Audit Files` folder on the Desktop. Use `list_audit_files` to verify the filename and then `read_audit_report` for analysis.

## 8. Prior Payroll Workflows (v1.4)

### 8.1 Prior Payroll Sanity Check (ADP)
**Trigger**: Implementer uploads an ADP `Prior Payroll Register Report_*.xlsx` that has interleaved `Totals For Associate ID XYZ:` summary rows, a bottom-of-file grand-total row, or multiple per-pay-period rows per employee.
1.  **Tool**: `adp_prior_payroll_sanity`
2.  **Inputs**:
    *   `file_path` (preferred) or `file_b64`
    *   `swap_net_take` (default `True`) — flips NET PAY ⇄ TAKE HOME values for the Carvan-style API. Headers are NEVER renamed.
    *   `aggregation_strategy`:
        *   `"ask"` (DEFAULT) — runs detection only, returns facts + a recommendation, **does NOT write a file**. Use this on the FIRST call unless the user has already told you which strategy they want.
        *   `"full_quarter"` — collapses everything to one row per associate. Use when the file is a full-quarter per-pay-period export the implementer left un-aggregated.
        *   `"preserve_pay_periods"` — keeps distinct pay periods, only merges same-day duplicate row pairs. Use for partial-period exports where the API expects per-period rows.
3.  **Two-step workflow** (mandatory unless the user pre-specified a strategy):
    *   **Step A**: Call with `aggregation_strategy="ask"` (or omit it). Read the response, which contains `facts` (associates, total_rows, date_span_days, rows_per_associate_max, distinct_pay_dates, period_min/max), `recommended_strategy`, and `recommendation_reason`. **Show all of it to the user**, surface the recommendation, and ask them to confirm or override.
    *   **Step B**: Re-call the tool with the user's chosen `aggregation_strategy="full_quarter"` or `"preserve_pay_periods"`. Now it produces the cleaned CSV.
4.  **Output**: Cleaned CSV in the `Audit Files` folder + summary dict (rows dropped, associates aggregated, merge events). When `mode == "detection_only"`, no file is written and `output_file` is absent.
5.  **CRITICAL**: ADP money cells are `=ROUND(x, 2.0)` Excel formulas — this tool reads them with `openpyxl` and evaluates the formula. Never use `pandas.read_excel` directly on these files; you'll get null money columns.

### 8.2 Prior Payroll Generator (ADP / Paycom)
**Trigger**: User wants to fill a blank Uzio Prior Payroll Template (.xlsm) from up to 10 ADP/Paycom source files.
1.  **Tools**: `adp_prior_payroll_generator`, `paycom_prior_payroll_generator`
2.  **Inputs**:
    *   `uzio_template_path` (preferred) or `uzio_template_b64` — blank Uzio template (.xlsm with VBA, preserved).
    *   `source_files` — list of `{file_path | file_b64, filename}` (max 10).
    *   `override_mapping` (optional):
        *   ADP: `{adp_column_name: uzio_col_idx}` — negative idx force-skips.
        *   Paycom: `{"type_code|type_description": uzio_col_idx}` — `|` separator since JSON keys can't be tuples.
3.  **Auto-mapping**: Fuzzy-string heuristic with domain boosts for Medicare / SS / FIT / 401k / FUTA / SUI / SDI / regular / overtime / bonus / state-income. Net Pay auto-routes to whichever Uzio header contains `"net pay"`.
4.  **Validation**: Flags any employee-period where `Gross − Taxes − Deductions ≠ Net Pay` (capped at 200 rows in response).

### 8.3 Selective Census Sync (ADP / Paycom)
**Trigger**: User has a pre-filled Uzio Census Template (.xlsm) and only wants to update specific columns from a fresh ADP/Paycom export — leaving every other column / sheet / VBA macro untouched.
1.  **Tools**: `adp_selective_census_sync`, `paycom_selective_census_sync`
2.  **Inputs**:
    *   `uzio_template_path` / `uzio_template_b64` — pre-filled Uzio template.
    *   `source_path` / `source_b64` — fresh ADP or Paycom export.
    *   `selected_uzio_cols` — list of Uzio column names (keys from `UZIO_RAW_MAPPING`) to overwrite.
    *   `job_title_mapping`, `work_location_mapping` — explicit `{source_value: uzio_value}` dicts. Pass `{}` to seed automatically from the existing template (via `extract_mappings_from_uzio`); omit to skip.
    *   `discover_only=true` — short-circuits to return seed mappings + unique source values for review before committing.

### 8.4 Paycom Consolidated Audit
**Trigger**: User wants Census + Payment + Emergency contact audits in one pass against the Uzio Master Custom Report (CSV with category labels in row 1, headers in row 2) and a Paycom Census export.
1.  **Tool**: `paycom_consolidated_audit`
2.  **Output**: 11-sheet Excel report — Summary, Census, Payment, Emergency, Salaried Driver Exceptions, FLSA Compliance, Active Missing, Terminated Missing, Data Quality, High Hourly Rate Anomalies, Duplicate SSN Warnings.
3.  **Use over individual audits** when running an end-to-end migration check; saves three round-trips.

### 8.5 Prior Payroll Setup Helper (ADP)
**Trigger**: Starting a fresh ADP prior payroll migration; need to know what to configure in Uzio (earnings, contributions, taxes, deductions) and how to map taxes/deductions correctly.
1.  **Pre-step**: Run `adp_prior_payroll_sanity` first if the file has interleaved `Totals For Associate ID` rows. The setup helper expects a clean, one-row-per-associate-per-period file.
2.  **Tool**: `adp_prior_payroll_setup_helper` (also available as a Streamlit tool under "ADP - Prior Payroll Setup Helper" in the parent Unified Audit Tool, with identical analysis but a UI for interactive review).
3.  **Inputs**:
    *   `file_path` (preferred) or `file_base64` — sanitized ADP prior payroll file (.xlsx / .csv).
    *   `state_tax_master_path` — defaults to `C:\Users\shobhit.sharma\Downloads\State Tax Code.csv`. Override only if the master is elsewhere.
    *   `state_tax_master_base64` — fallback for remote callers.
4.  **Output** (Excel workbook in `Audit Files` + standalone Tax_Mapping CSV):
    *   `Earnings_Codes` — every REGULAR/OVERTIME and `ADDITIONAL EARNINGS : XXX` code with $ total, employee count, hours, avg rate.
    *   `Contributions` — 401k/403b/457/Roth/HSA/FSA codes, each with pre-tax verdict and flavor.
    *   `Deductions` — every other voluntary deduction with **pre-tax vs post-tax verdict** (algorithm: subset-sum on `TOTAL EARNINGS − FIT_TAXABLE`; one positive proof = pre-tax for the whole file). Flavor distinguishes `section_125` (medical/dental/vision pre-FIT/FICA/MEDI/SIT) from `401k_traditional` (pre-FIT/SIT only).
    *   `Taxes_Discovered` — every `* - EMPLOYEE/EMPLOYER TAX` column.
    *   `Tax_Mapping` — output in `Payroll_Mappings_Tax_Mapping_CORRECTED.csv` format. Federal = 1 row per tax; state-scoped (SIT/SDI/SUTA/FLI) = **1 row per distinct WORKED IN STATE** (multi-state respects the SUTA-per-state rule).
    *   `Bonus_Classification` — FLSA test verdict (`discretionary` / `non_discretionary` / `indeterminate`). Compares actual OT rate to `1.5 × regular_rate`; any row showing inflation = non-discretionary for the whole file.
5.  **Standalone CSV**: `<filename>_Tax_Mapping_<timestamp>.csv` is also written to `Audit Files` so you can upload it directly into the next migration step.

### 8.6 Total Comparison (Prior Payroll Audit)
Both `adp_total_comparison` and `paycom_total_comparison` now produce three additional sheets beyond Full Comparison / Mismatches Only / Employee Mismatches:
*   **Duplicate Pay Periods** — UZIO-side skeleton-vs-detail row pairs.
*   **Pay Stub Counts** — per-employee distinct Pay Date count, ADP/Paycom combined vs UZIO.
*   **Tax Rate Verification** — SS / Medicare / FUTA + per-state SUTA, effective rate vs standard at 0.05% tolerance. SUTA is **always one row per state** — never lumped.
