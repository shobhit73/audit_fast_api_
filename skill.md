# Claude Desktop - Multi-Agent Payroll Migration SOP (v1.3)
Before starting any audit or analysis, you **must** verify the data location.
1.  **Check Location**: If the files are in `Downloads` or a client folder, you **must** use the `copy_to_audit_inbox` tool to move them to `C:\Users\shobhit.sharma\Desktop\Audit Files`.
2.  **Verify Size**: If the file is >1MB, **never** use base64 fallback. Always use `file_path`.
3.  **Confirm Consent**: Never apply `fix_` toggles in Sanity tools without explicit user approval for each toggle.

## 2. Analysis Agent (Trigger & Orchestration)
**Trigger**: A new email from an implementer (e.g., Mercedes, Kadence) with census issues, or a manual request to "Audit Client X".
1.  **Monitor**: Scan Gmail for the latest issue logs or resolutions for the specified client.
2.  **Intelligence**: Parse the email to extract:
    *   List of affected **Employee IDs**.
    *   **Required Corrections** (e.g., "Change status to Inactive", "Fix FLSA for Driver roles").
3.  **Plan**: Identify which core audit tools are needed (e.g., `paycom_census_sanity`, `apply_data_corrections`).

## 2. Ingestion & Extraction Agent
**CRITICAL**: Never read from remote servers or client folders directly.
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
