# Payroll Audit Agent - Standard Operating Procedure (SOP)

This document defines the mandatory workflows for all payroll audits. Always follow these steps to ensure data integrity and prevent system timeouts.

## 1. Discovery Phase (The "Scan")
Before running any audit, you must identify the available files.
*   **Tool**: `list_audit_files`
*   **Action**: Scan the user's specified folder (e.g., `C:\Users\shobhit.sharma\Downloads\Happy Delivery`) or the default `C:\Users\shobhit.sharma\Desktop\Audit Files`.
*   **Output**: Identify the Paycom/ADP CSVs, Uzio Registers, and Mapping files.

## 2. Execution Phase (The "Audit")
Once files are identified, run the appropriate audit tool.
*   **Tools**: `paycom_total_comparison`, `adp_total_comparison`, `adp_census_audit`, etc.
*   **Action**: Pass the full local paths discovered in the Scan phase.
*   **Result**: The tool will return a summary and save a full `.xlsx` report to the **Audit Files** folder.

## 3. Analysis Phase (The "Deep Read")
**CRITICAL**: Do not rely only on the summary returned by the audit tool for deep analysis.
*   **Tool**: `read_audit_report`
*   **Action**: Read the specific `.xlsx` file that was just saved in the `Audit Files` folder.
*   **Benefit**: This prevents "JSON Overload" crashes and allows you to analyze 100% of the audit data safely.

## 4. Communication Phase (The "Action")
Use the insights gathered from the Deep Read to fulfill user requests via Gmail.
*   **Tool**: `gmail_create_draft` or `gmail_send_message`.
*   **Action**: Draft summaries of mismatches, root cause analysis, or missing employee reports.
*   **Reference**: Always mention the specific filename in the `Audit Files` folder so the user can verify.

## 5. Client Folders SOP
If a user mentions a client name (e.g., "Happy Delivery"), always check if they have a dedicated folder in `Downloads` or `Desktop` first using `list_audit_files`.

## 6. Error Handling
*   **Format Mismatch**: If you see "Excel format cannot be determined," it means the file is a CSV. All tools now support CSV, so ensure you are using the correct file extension in your logic.
*   **SSN Bug**: All SSN duplicate checks now require an `ssn_col` argument. The tools are pre-configured to find this, but always ensure you are passing the identified column name.
