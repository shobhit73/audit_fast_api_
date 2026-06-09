# Claude Desktop — Payroll Migration SOP (v2.0)

This is the agent SOP for the **local MCP server** in this repo (stdio, launched by
Claude Desktop — see [SETUP.md](SETUP.md)). It exposes the ADP/Paycom/Uzio payroll
audit toolset. Before starting any audit or analysis, you **must** verify the data
location.

1.  **Data Access**: You can access files from any local path (e.g., `Downloads`, client folders). Using the `copy_to_audit_inbox` tool is optional but recommended to keep inputs and outputs in the **Audit Files inbox** (the running user's `Desktop\Audit Files` folder by default, or wherever the `AUDIT_INBOX` env var points). Always discover the actual location with `list_audit_files` rather than assuming an absolute path.
2.  **Large Files (>1MB)**: Use the **Side-Car DB Strategy (DuckDB)**. Never attempt to read a file >1MB fully into context.
    *   Call `get_file_schema` to identify columns.
    *   Call `query_data_sql` to extract specific employees or calculate totals using SQL.
3.  **Confirm Consent**: Never apply `fix_` toggles in Sanity tools without explicit user approval for each correction.

## 0. File-routing matrix (READ THIS FIRST)

Every audit tool's name encodes its scope. Pick the wrong tool and the runtime guard will refuse the call with a clear error - but the right move is to pick correctly the first time.

**Vendor naming convention** (the prefix is the source vendor; the comparison side is always UZIO):
- `adp_*` -- expects an **ADP** export. Two-file audits (`adp_payment_audit`, `adp_census_audit`, etc.) take both the UZIO file and the ADP file in clearly-named slots.
- `paycom_*` -- expects a **Paycom** export. Same two-slot pattern for two-file audits.
- **Vendor-agnostic** (no prefix): `list_audit_files`, `read_audit_report`, `get_file_schema`, `query_data_sql`, `selective_employee_extractor`, `apply_data_corrections`, `copy_to_audit_inbox`.
- **`job_title_mapping`** is the one no-prefix tool that still needs a **`vendor`** argument (`"adp"` or `"paycom"`): it maps each distinct DSP job title in a post-sanity census to Amazon's 30-row standard catalog and writes a side-car CSV. Two-step: call once without `mapping` to get the distinct titles + catalog, then again with `mapping = {dsp_title: amazon_title}`.

**Critical: NEVER call an `adp_*` tool on a UZIO or Paycom file**, and never call a `paycom_*` tool on a UZIO or ADP file. The single-file tools (`*_prior_payroll_sanity`, `*_prior_payroll_setup_helper`, `*_census_sanity`, `*_census_generator`) have a runtime file-shape guard that detects the wrong vendor and refuses. Two-file audits don't have a guard, but the parameter slots are explicit (`uzio_file_path` vs `adp_file_path`/`paycom_file_path`) - putting an ADP file in the `uzio_file_path` slot will produce nonsense matches.

**File-shape recognition (how to tell what kind of file the user has)**:
- **ADP** exports often have a `Report Criteria` preamble sheet, columns like `ASSOCIATE ID`, `POSITION ID`, `WORKED IN STATE`, `ADDITIONAL EARNINGS  : XXX-CODE` (note the double-space-colon), `VOLUNTARY DEDUCTION : XX-CODE`. Money cells in xlsx are stored as `=ROUND(x, 2.0)` formulas.
- **Paycom** exports use snake_case: `Employee_Code`, `SS_Number`, `DOL_Status`, `Department_Desc`, `Exempt_Status`. Long-format prior payroll files use `Type_Code` / `Type_Description` / `Code_Description` / `Amount`.
- **UZIO** files use pipe-delimited section|field column names: `Personal|SSN`, `Job|Employee ID`, `Job|Department`. Raw exports have a 3-row preamble. The Custom Report has category labels in row 1, headers in row 2.

**The wrong-vendor mistake to never make**: do NOT call `adp_prior_payroll_sanity` (or any other ADP-specific tool) on a UZIO Prior Payroll Register / Master file. UZIO files do not have the interleaved totals rows or per-pay-period duplication that the sanity tool exists to fix; running it on a UZIO file will silently produce a corrupted aggregation. The runtime guard now refuses these explicitly.

> **Base64 inputs**: every file input also has a `*_base64` fallback (e.g. `file_base64`, `adp_file_base64`, `uzio_template_base64`). On this local server you almost always use the `*_path` form; the base64 fallbacks are only for the rare case where you hold raw bytes rather than a path.

## 1. Analysis Agent (Trigger & Orchestration)
**Trigger**: A new email from an implementer (e.g., Mercedes, Kadence) with census issues, or a manual request to "Audit Client X".
1.  **Monitor**: Scan Gmail for the latest issue logs or resolutions for the specified client. *(Requires a Gmail MCP server to be connected separately — not part of this repo.)*
2.  **Intelligence**: Parse the email to extract:
    *   List of affected **Employee IDs**.
    *   **Required Corrections** (e.g., "Change status to Inactive", "Fix FLSA for Driver roles").
3.  **Plan**: Identify which core audit tools are needed (e.g., `paycom_census_sanity`, `apply_data_corrections`).

## 2. Ingestion & Extraction Agent
**NOTE**: You can read from any local folder provided by the user. Use the `path` field from `list_audit_files` to ensure accuracy.
1.  **Copy**: Move master census/payroll files from the client's source folder (e.g., `Downloads/Happy Delivery`) to the Audit Files inbox using the **`copy_to_audit_inbox`** tool.
2.  **Verify**: Use `list_audit_files` to confirm files are ready in the Audit Files inbox.
3.  **Isolate**: Call `selective_employee_extractor` to pull only the problematic employees into a temporary "Working Set" CSV/Excel.

## 3. Correction & Sanity Agent
1.  **Sanity First**: Run `paycom_census_sanity` or `adp_census_sanity` with **all toggles OFF** first to identify standard errors.
2.  **Implementer Override**:
    *   **Tool**: `apply_data_corrections`
    *   **Action**: Apply the manual resolutions from Gmail/Implementers directly to the master file or working set.
    *   **Formatting**: This tool **preserves 100% of formatting**, making it the "Source of Truth" for final uploads.
    *   **Strict ID**: You MUST provide an Employee ID for every correction.
3.  **Finalize**: Save the corrected report to the Audit Files inbox with an `_OVERRIDDEN` suffix.

## 4. Communication & Reporting Agent
1.  **Deep Read**: Use `read_audit_report` to analyze the final corrected file for any remaining anomalies.
2.  **Summarize**: Create a concise summary of all changes made.
3.  **Reply**: Draft or send a Gmail reply to the implementer (Mercedes/Kadence): *(Gmail steps require a separately-connected Gmail MCP server.)*
    *   Confirming which IDs were fixed.
    *   Attaching/Referencing the corrected filename in the Audit Files inbox.
    *   Highlighting any unresolved assumptions.

## 5. Advanced Workflows

### 5.1 Multi-Tool Cross-Reference (Deep Dive)
**Scenario**: User asks for a deep dive into a specific employee after multiple audits (e.g., "Check this employee's status in both Census and Payment audits").
1.  **Ingest**: Ensure all source files (Census ADP/Uzio, Payment ADP/Uzio) are copied to the Audit Files inbox.
2.  **Audit**: Run all relevant audits (e.g., `adp_census_audit`, `adp_payment_audit`).
3.  **Analyze**:
    *   Use `read_audit_report` to open the newly generated reports from the Audit Files inbox.
    *   Search for the specific employee in both reports.
    *   Summarize mismatches across both domains (e.g., "Active in Census but unpaid in Payment report").
4.  **Resolve**: Draft a Gmail to the implementer detailing the specific cross-domain discrepancies for that employee.

### 5.2 Gmail-Driven Corrections
**Scenario**: Implementers provide resolutions or data updates via Gmail. *(Requires a Gmail MCP server.)*
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
2.  **CRITICAL**: Always move/copy files to the Audit Files inbox first. **Never** audit files directly from remote locations.

## 7. Reporting & Communication
*   **Action**: Summarize all corrections made and the final status of problematic records.
*   **Verification**: All tool output reports are **MANDATORY** saved to the Audit Files inbox. Use `list_audit_files` to verify the filename and then `read_audit_report` for analysis.

## 8. Prior Payroll & Consolidated Workflows

### 8.1 Prior Payroll Sanity Check (ADP)
**Trigger**: Implementer uploads an **ADP** `Prior Payroll Register Report_*.xlsx` that has interleaved `Totals For Associate ID XYZ:` summary rows, a bottom-of-file grand-total row, or multiple per-pay-period rows per employee.

**DO NOT call this tool on a UZIO Prior Payroll Register / Master file.** UZIO files do not have the issues this tool fixes; the runtime guard will refuse the call and tell you the file looks like a UZIO export. UZIO files don't need sanity cleanup at all - they're consumed as-is by audit tools that take a `uzio_file_path` slot.

1.  **Tool**: `adp_prior_payroll_sanity`
2.  **Inputs**:
    *   `file_path` (preferred) or `file_base64`
    *   `swap_net_take` (default `True`) — flips NET PAY ⇄ TAKE HOME values for the Carvan-style API. Headers are NEVER renamed.
    *   `aggregation_strategy`:
        *   `"ask"` (DEFAULT) — runs detection only, returns facts + a recommendation, **does NOT write a file**. Use this on the FIRST call unless the user has already told you which strategy they want.
        *   `"full_quarter"` — collapses everything to one row per associate. Use when the file is a full-quarter per-pay-period export the implementer left un-aggregated.
        *   `"preserve_pay_periods"` — keeps distinct pay periods, only merges same-day duplicate row pairs. Use for partial-period exports where the API expects per-period rows.
3.  **Two-step workflow** (mandatory unless the user pre-specified a strategy):
    *   **Step A**: Call with `aggregation_strategy="ask"` (or omit it). Read the response, which contains `facts` (associates, total_rows, date_span_days, rows_per_associate_max, distinct_pay_dates, period_min/max), `recommended_strategy`, and `recommendation_reason`. **Show all of it to the user**, surface the recommendation, and ask them to confirm or override.
    *   **Step B**: Re-call the tool with the user's chosen `aggregation_strategy="full_quarter"` or `"preserve_pay_periods"`. Now it produces the cleaned CSV.
4.  **Output**: Cleaned CSV in the Audit Files inbox + summary dict (rows dropped, associates aggregated, merge events). When `mode == "detection_only"`, no file is written and `output_file` is absent.
5.  **CRITICAL**: ADP money cells are `=ROUND(x, 2.0)` Excel formulas — this tool reads them with `openpyxl` and evaluates the formula. Never use `pandas.read_excel` directly on these files; you'll get null money columns.

### 8.2 Selective Census Sync (ADP / Paycom)
**Trigger**: User has a pre-filled Uzio Census Template (.xlsm) and only wants to update specific columns from a fresh ADP/Paycom export — leaving every other column / sheet / VBA macro untouched.
1.  **Tools**: `adp_selective_census_sync`, `paycom_selective_census_sync`
2.  **Inputs**:
    *   `uzio_template_path` (or `uzio_template_base64`) — the pre-filled Uzio template (.xlsm).
    *   `adp_file_path` (or `adp_file_base64`) for the ADP tool / `paycom_file_path` (or `paycom_file_base64`) for the Paycom tool — the fresh source export.
    *   `selected_uzio_cols` — list of Uzio column names (keys from `UZIO_RAW_MAPPING`) to overwrite. **Required.**
    *   `job_title_mapping`, `work_location_mapping` — explicit `{source_value: uzio_value}` dicts. Pass `{}` to seed automatically from the existing template (via `extract_mappings_from_uzio`); omit to skip those columns.
    *   `fix_options` — optional auto-fix toggles (same keys as the census sanity tool).
    *   `discover_only=true` — short-circuits to return seed mappings + unique source values for review before committing.

### 8.3 Consolidated Audits (Paycom & ADP)
**Trigger**: User wants several audits in one pass for an end-to-end migration check, instead of separate round-trips. There is a consolidated tool for each vendor.

**Paycom — `paycom_consolidated_audit`**
1.  **Inputs**: `uzio_file_path` (Uzio Master Custom Report CSV — category labels in row 1, headers in row 2) + `paycom_file_path` (Paycom Census export .xlsx/.csv). Optional `client_name` for the output filename.
2.  **Output**: an 11-sheet Excel report — Summary, Duplicate_SSN_Check, Census_Audit, Payment_Audit, Emergency_Audit, Salaried_Drivers, FLSA_Issues, Active_Missing, Terminated_Missing, Data_Quality, High_Rate_Anomalies. (Census + Payment + Emergency in one pass, plus the anomaly extracts.)

**ADP — `adp_consolidated_audit`**
1.  **Inputs**: `uzio_file_path` (Uzio Master / HR Report CSV — **required**) plus up to three **optional** ADP files; provide at least one:
    *   `adp_census_file_path` → drives the **Census** audit.
    *   `adp_dd_file_path` → drives the **Direct Deposit (Payment)** audit.
    *   `adp_em_file_path` → the ADP **Emergency + License Details Report**, which drives **both** the Emergency Contact audit AND the License audit.
    *   Optional `client_name` for the output filename.
2.  **Output**: one workbook with sheets prefixed by section — `CEN_*` (census detail + anomaly checks), `DD_*` (direct deposit), `EC_*` (emergency contact), `LIC_*` (license). Roll-up summary sheets are intentionally dropped. If any single audit fails it is isolated into an `_Errors` sheet rather than aborting the rest.
3.  **Use over individual audits** when running an end-to-end migration check; saves multiple round-trips.

### 8.4 Prior Payroll Setup Helper (Paycom) — replaces the deleted Deduction Analyzer
**Trigger**: Starting a fresh **Paycom** prior payroll migration; need to know what to configure in Uzio (earnings, contributions, deductions), which deductions are pre-tax vs post-tax, and whether bonuses are discretionary.
1.  **Tool**: `paycom_prior_payroll_setup_helper` (also available as a Streamlit tool under "Paycom - Prior Payroll Setup Helper").
2.  **Inputs (BOTH required)**:
    *   `prior_payroll_path` -- Paycom Prior Payroll Register, long format with columns `EE Code, Type Code, Type Description, Amount, Code Description`.
    *   `scheduled_deductions_path` -- Paycom Scheduled Deductions Report with columns `Deduction Code, Deduction Desc, Tax Treatment`.
    *   Both go through the runtime guard which refuses non-Paycom files.
3.  **Output** (3-tab Excel workbook in the Audit Files inbox):
    *   Tab 1 -- What to Set Up (Earnings | Contributions | Deductions, codes only).
    *   Tab 2 -- Pre-Tax vs Post-Tax. Read straight from the **Tax Treatment** column of the Scheduled Deductions report. `B = Section 125 pre-tax`, `H = 401k traditional pre-tax`, `A = post-tax`. No empirical algorithm needed -- Paycom labels each deduction directly.
    *   Tab 3 -- Bonus Verdict (FLSA). Strategy A+C: when both `OT` (plain) and `WOT` (Paycom's weighted overtime) lines exist for the same employee+period, compare them. WOT > OT means Paycom rolled a bonus into the regular rate => **non-discretionary**. When the differential test cannot run (file has only WOT, only OT, or no bonus codes at all), the verdict is `indeterminate` with a note asking for a Payroll Register Detail with hours.
4.  **Note**: Roth contributions are correctly classified POST-TAX (Roth's whole purpose is post-tax). Traditional 401(k) is correctly PRE-TAX FIT/SIT-only (Paycom's `H` Tax Treatment).
5.  **The deleted `paycom_deduction_analyzer` tool**: no longer exists. Calls to it return "Unknown tool". Use `paycom_prior_payroll_setup_helper` for the same use case.

### 8.5 Prior Payroll Setup Helper (ADP)
**Trigger**: Starting a fresh ADP prior payroll migration; need to know what to configure in Uzio (earnings, contributions, taxes, deductions) and how to map taxes/deductions correctly.
1.  **Pre-step**: Run `adp_prior_payroll_sanity` first if the file has interleaved `Totals For Associate ID` rows. The setup helper expects a clean, one-row-per-associate-per-period file.
2.  **Tool**: `adp_prior_payroll_setup_helper` (also available as a Streamlit tool under "ADP - Prior Payroll Setup Helper" in the parent Unified Audit Tool, with identical analysis but a UI for interactive review).
3.  **Inputs**:
    *   `file_path` (preferred) or `file_base64` — sanitized ADP prior payroll file (.xlsx / .csv).
    *   `state_tax_master_path` — path to the State Tax Code master CSV. If omitted, falls back to the `STATE_TAX_MASTER_PATH` env var; otherwise pass `state_tax_master_base64`.
    *   `state_tax_master_base64` — fallback if you only have the master as base64 bytes rather than a path.
4.  **Output** (Excel workbook in the Audit Files inbox + standalone Tax_Mapping CSV):
    *   `Earnings_Codes` — every REGULAR/OVERTIME and `ADDITIONAL EARNINGS : XXX` code with $ total, employee count, hours, avg rate.
    *   `Contributions` — 401k/403b/457/Roth/HSA/FSA codes, each with pre-tax verdict and flavor.
    *   `Deductions` — every other voluntary deduction with **pre-tax vs post-tax verdict** (algorithm: subset-sum on `TOTAL EARNINGS − FIT_TAXABLE`; one positive proof = pre-tax for the whole file). Flavor distinguishes `section_125` (medical/dental/vision pre-FIT/FICA/MEDI/SIT) from `401k_traditional` (pre-FIT/SIT only).
    *   `Taxes_Discovered` — every `* - EMPLOYEE/EMPLOYER TAX` column.
    *   `Tax_Mapping` — output in `Payroll_Mappings_Tax_Mapping_CORRECTED.csv` format. Federal = 1 row per tax; state-scoped (SIT/SDI/SUTA/FLI) = **1 row per distinct WORKED IN STATE** (multi-state respects the SUTA-per-state rule).
    *   `Bonus_Classification` — FLSA test verdict (`discretionary` / `non_discretionary` / `indeterminate`). Compares actual OT rate to `1.5 × regular_rate`; any row showing inflation = non-discretionary for the whole file.
5.  **Standalone CSV**: `<filename>_Tax_Mapping_<timestamp>.csv` is also written to the Audit Files inbox so you can upload it directly into the next migration step.

### 8.6 Total Comparison (Prior Payroll Audit)
Both `adp_total_comparison` and `paycom_total_comparison` produce three additional sheets beyond Full Comparison / Mismatches Only / Employee Mismatches:
*   **Duplicate Pay Periods** — UZIO-side skeleton-vs-detail row pairs.
*   **Pay Stub Counts** — per-employee distinct Pay Date count, ADP/Paycom combined vs UZIO.
*   **Tax Rate Verification** — SS / Medicare / FUTA + per-state SUTA, effective rate vs standard at 0.05% tolerance. SUTA is **always one row per state** — never lumped.
