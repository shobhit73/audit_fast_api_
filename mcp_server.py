import json
import base64
import os
import numpy as np
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.server.sse import SseServerTransport
import mcp.types as types
import sys

# ── Drop-folder: user puts files here, Claude picks them up automatically ──
AUDIT_INBOX = r"C:\Users\shobhit.sharma\Desktop\Audit Files"

# --- Core Imports ---
from core.adp.total_comparison import run_adp_total_comparison
from core.adp.census_audit import run_adp_census_audit, ADP_FIELD_MAP
from core.adp.deduction_audit import run_adp_deduction_audit
from core.adp.payment_audit import run_adp_payment_audit
from core.adp.withholding_audit import run_adp_withholding_audit

from core.paycom.deduction_analyzer import run_paycom_deduction_analysis
from core.paycom.deduction_audit import run_paycom_deduction_audit
from core.paycom.total_comparison import run_paycom_total_comparison
from core.paycom.census_audit import run_paycom_census_audit, PAYCOM_FIELD_MAP
from core.paycom.withholding_audit import run_paycom_withholding_audit
from core.paycom.sql_master import run_paycom_sql_master
from core.paycom.payment_audit import run_paycom_payment_audit
from core.paycom.misc_audits import run_paycom_emergency_audit, run_paycom_timeoff_audit

from core.census.sanity_check import run_census_sanity_check, generate_corrected_census_xlsx
from core.adp.misc_audits import (
    run_adp_emergency_audit, run_adp_license_audit, run_adp_timeoff_audit
)
from core.adp.census_generator import run_adp_census_generation
from core.paycom.census_generator import run_paycom_census_generation
from core.adp.prior_payroll_sanity import run_adp_prior_payroll_sanity
from core.adp.prior_payroll_generator import run_adp_prior_payroll_generator
from core.paycom.prior_payroll_generator import run_paycom_prior_payroll_generator
from core.adp.selective_census_sync import run_adp_selective_census_sync, discover_mappings as adp_selective_discover
from core.paycom.selective_census_sync import run_paycom_selective_census_sync, discover_mappings as paycom_selective_discover
from core.common.paycom_consolidated_audit import run_paycom_consolidated_audit
from core.adp.prior_payroll_setup_helper import run_adp_prior_payroll_setup_helper

from starlette.applications import Starlette
from starlette.routing import Mount, Route

server = Server("audit-tool-server")

# ── Helpers ──────────────────────────────────────────────────────────────────

def _json_default(o):
    """Custom JSON encoder for numpy types and other non-serializable objects."""
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    if hasattr(o, "isoformat"):  # handles datetime, Timestamp, etc.
        return o.isoformat()
    # Handle pandas types specifically if numpy check isn't enough
    if hasattr(o, "item") and callable(o.item):
        return o.item()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

def save_results_to_excel(results, name_prefix):
    """Saves results (list of dicts OR dict of lists) to an Excel file on the Desktop."""
    import pandas as pd
    from datetime import datetime
    
    if not results:
        return {"summary": "No results", "file_path": None}
        
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"{name_prefix}_{stamp}.xlsx"
    
    # Ensure the Audit Files directory exists
    if not os.path.exists(AUDIT_INBOX):
        os.makedirs(AUDIT_INBOX, exist_ok=True)
        
    out_path = os.path.join(AUDIT_INBOX, filename)
    
    if isinstance(results, list):
        df = pd.DataFrame(results)
        df.to_excel(out_path, index=False)
        summary = {
            "total_rows": len(results),
            "file_path": out_path,
            "message": f"Full report saved to 'Audit Files' folder as {filename}.",
            "data": results if len(results) < 2000 else results[:500],
            "note": "Full data returned in response." if len(results) < 2000 else "Data truncated due to size."
        }
        if "Status" in df.columns:
            summary["counts_by_status"] = df["Status"].value_counts().to_dict()
        return summary
        
    elif isinstance(results, dict):
        # Handle dict of lists (multiple sheets)
        with pd.ExcelWriter(out_path) as writer:
            summary_info = {"file_path": out_path, "message": f"Report saved to 'Audit Files' folder as {filename}."}
            for sheet_name, data in results.items():
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
                    summary_info[f"{sheet_name}_count"] = len(data)
                    # Return full data to Claude if it's reasonably sized (< 2000 rows)
                    if len(data) < 2000:
                        summary_info[sheet_name] = data
                    else:
                        summary_info[sheet_name] = data[:500]
                        summary_info[f"{sheet_name}_note"] = "Data truncated in response due to size. Full data in Excel file."
                else:
                    summary_info[sheet_name] = data
            return summary_info
            
    return {"error": "Unsupported results format"}

def load_file(arguments: dict, path_key: str, b64_key: str) -> bytes:
    """Load a file from a local path OR fall back to base64-encoded content."""
    path = arguments.get(path_key)
    if path:
        path = path.strip().strip('"')
        with open(path, "rb") as f:
            return f.read()
    b64 = arguments.get(b64_key)
    if b64:
        return base64.b64decode(b64)
    return b""

def load_files_list(arguments: dict, paths_key: str, b64_key: str):
    """Load a list of files from local paths OR base64 list. Returns list of (bytes, name)."""
    paths = arguments.get(paths_key, [])
    if paths:
        result = []
        for p in paths:
            p = p.strip().strip('"')
            with open(p, "rb") as f:
                result.append((f.read(), os.path.basename(p)))
        return result
    b64_list = arguments.get(b64_key, [])
    return [(base64.b64decode(b), f"file_{i}.xlsx") for i, b in enumerate(b64_list)]

def load_mappings_from_paths(paths):
    """Loads and merges mappings from a list of local file paths (CSV or Excel)."""
    import pandas as pd
    mappings = []
    for p in paths:
        p = p.strip().strip('"')
        if not os.path.isfile(p): continue
        try:
            # Determine category from filename
            cat = "Earnings"
            fname = os.path.basename(p).lower()
            if "deduction" in fname: cat = "Deductions"
            elif "contribution" in fname: cat = "Contributions"
            elif "tax" in fname: cat = "Taxes"
            
            df = pd.read_csv(p) if p.lower().endswith('.csv') else pd.read_excel(p)
            
            # Find Source and Uzio columns
            s_col = next((c for c in df.columns if "source" in str(c).lower() and "name" in str(c).lower()), None)
            u_col = next((c for c in df.columns if "uzio" in str(c).lower() and ("name" in str(c).lower() or "description" in str(c).lower())), None)
            
            if s_col and u_col:
                for _, row in df.iterrows():
                    mappings.append({
                        "Category": cat,
                        "Source_Name": str(row[s_col]).strip(),
                        "UZIO_Name": str(row[u_col]).strip()
                    })
        except Exception as e:
            print(f"Error loading mapping {p}: {e}")
    return mappings

def copy_file_to_inbox(source_path):
    """Safely copies a file from anywhere on the system to the Audit Files inbox."""
    import shutil
    source_path = source_path.strip().strip('"')
    if not os.path.isfile(source_path):
        return {"error": f"Source file '{source_path}' not found."}
    
    # Ensure inbox exists
    if not os.path.exists(AUDIT_INBOX):
        os.makedirs(AUDIT_INBOX, exist_ok=True)
        
    dest_path = os.path.join(AUDIT_INBOX, os.path.basename(source_path))
    try:
        shutil.copy2(source_path, dest_path)
        return {
            "success": True,
            "message": f"File successfully copied to inbox.",
            "source": source_path,
            "destination": dest_path
        }
    except Exception as e:
        return {"error": f"Failed to copy file: {str(e)}"}

def apply_data_corrections(file_path, corrections_list):
    """
    Surgically updates specific cells in an Excel file using openpyxl to preserve formatting.
    corrections_list: list of dicts like {'id': '123', 'column': 'Status', 'value': 'Inactive'}
    """
    import openpyxl
    from utils.audit_utils import norm_id, norm_colname
    
    file_path = file_path.strip().strip('"')
    if not os.path.isfile(file_path):
        return {"error": f"File '{file_path}' not found."}
    
    try:
        wb = openpyxl.load_workbook(file_path)
        # Prefer the sanity-output data sheet when present; fall back to whatever
        # was active when the file was last saved.
        if "Corrected Census" in wb.sheetnames:
            ws = wb["Corrected Census"]
        else:
            ws = wb.active

        # 1. Identify header row (some files have junk rows at the top)
        header_row_idx = 1
        id_keywords = ["employee id", "employee code", "associate id", "file #", "id#"]
        
        for r in range(1, 20): # Peek first 20 rows
            row_vals = [norm_colname(cell.value).lower() for cell in ws[r]]
            if any(any(k in v for k in id_keywords) for v in row_vals):
                header_row_idx = r
                norm_headers = row_vals
                headers = [str(cell.value) for cell in ws[r]]
                break
        else:
            # Fallback to row 1 if no keywords found
            headers = [str(cell.value) for cell in ws[1]]
            norm_headers = [norm_colname(h).lower() for h in headers]
        
        # Find ID column index
        id_col_indices = [i for i, h in enumerate(norm_headers) if any(k in h for k in id_keywords)]
        if not id_col_indices:
            return {"error": f"Could not identify Employee ID column. Found headers in row {header_row_idx}: {headers}"}
        id_col_idx = id_col_indices[0]
        
        results = []
        for corr in corrections_list:
            target_id = norm_id(corr.get('id'))
            target_col = norm_colname(corr.get('column')).lower()
            new_val = corr.get('value')
            
            # Find target column - error on ambiguity rather than silently
            # picking the first match (e.g. "FLSA" matches both "FLSA Description"
            # and "FLSA Code").
            matches = [(i, headers[i]) for i, h in enumerate(norm_headers) if target_col in h]
            if not matches:
                results.append({"id": target_id, "status": "Error", "message": f"Column '{corr.get('column')}' not found."})
                continue
            if len(matches) > 1:
                cand_names = [m[1] for m in matches]
                results.append({
                    "id": target_id,
                    "status": "Error",
                    "message": f"Column '{corr.get('column')}' is ambiguous; matches multiple headers: {cand_names}. Use the full header name."
                })
                continue
            col_idx = matches[0][0]
            
            # 2. Find row and update
            found = False
            for row_idx in range(header_row_idx + 1, ws.max_row + 1):
                cell_val = norm_id(ws.cell(row=row_idx, column=id_col_idx + 1).value)
                if cell_val == target_id:
                    ws.cell(row=row_idx, column=col_idx + 1).value = new_val
                    results.append({"id": target_id, "column": corr.get('column'), "status": "Success"})
                    found = True
                    break
            
            if not found:
                results.append({"id": target_id, "status": "Error", "message": "Employee ID not found in file."})
        
        # 3. Save as new file with suffix
        stamp = datetime.now().strftime("%Y%m%d_%H%M")
        base, ext = os.path.splitext(file_path)
        out_path = f"{base}_OVERRIDDEN_{stamp}{ext}"
        wb.save(out_path)
        
        return {
            "success": True,
            "output_file": out_path,
            "applied_changes": results
        }
        
    except Exception as e:
        import traceback
        return {"error": f"Correction failed: {str(e)}\n{traceback.format_exc()}"}

# ── Tool Definitions ──────────────────────────────────────────────────────────

PATH_DESC = "Full local file path (e.g. C:\\Users\\...\\file.xlsx). Preferred over base64 for large files."

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        # --- UTILITY TOOLS ---
        types.Tool(
            name="list_audit_files",
            description=(
                "Lists all files available in a specified directory or the default audit drop-folder. "
                "Always call this first before running any audit to discover "
                "which files the user has available. Returns file names, "
                "full paths, sizes, and last-modified timestamps."
            ),
            inputSchema={
                "type": "object", 
                "properties": {
                    "directory_path": {
                        "type": "string",
                        "description": "Optional: Full local path to a folder to scan (e.g., C:\\Users\\...\\Happy Delivery). Defaults to Desktop/Audit Files."
                    }
                }
            },
        ),
        types.Tool(
            name="copy_to_audit_inbox",
            description=(
                "Copies a file from any local directory (e.g., Downloads) to the 'Audit Files' inbox. "
                "Use this to satisfy the SOP requirement of moving files locally before analysis."
            ),
            inputSchema={
                "type": "object", 
                "properties": {
                    "source_path": {
                        "type": "string",
                        "description": "Full local path to the source file (e.g., C:\\Users\\...\\Downloads\\report.xlsx)"
                    }
                },
                "required": ["source_path"]
            },
        ),
        types.Tool(
            name="apply_data_corrections",
            description=(
                "Performs surgical row-level overrides on a post-sanity census file, "
                "keyed off Employee ID (Paycom) or Associate ID (ADP). Preserves all "
                "original Excel formatting (colors, fonts, borders, column widths). "
                "Output: a NEW file '<base>_OVERRIDDEN_<timestamp>.xlsx' next to the "
                "input - the input is never modified in place.\n\n"
                "WORKFLOW (file_path resolution, in priority order):\n"
                "  1. If the prior turn ran 'adp_census_sanity' or "
                "'paycom_census_sanity', use the 'output_file' path returned in that "
                "response - that is the canonical post-sanity file.\n"
                "  2. Otherwise, call 'list_audit_files' and pick the most recent "
                "'<Vendor>_Cleaned_*.xlsx' (vendor = ADP or Paycom) by filename "
                "timestamp from the Audit Files inbox "
                "(C:\\Users\\<user>\\Desktop\\Audit Files).\n"
                "  3. Only ask the user if multiple vendors have recent cleaned files "
                "and the override list itself does not disambiguate.\n\n"
                "COLUMN-NAME RULE: Use the FULL header from the file (e.g. "
                "'FLSA Description', not 'FLSA'). Matching is substring-based, so "
                "ambiguous fragments will now error out listing all candidate headers "
                "rather than silently picking the first one."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": (
                            "Full local path to the post-sanity Excel file. Typically "
                            "the 'output_file' returned by adp_census_sanity / "
                            "paycom_census_sanity, or the most recent "
                            "<Vendor>_Cleaned_*.xlsx in the Audit Files inbox."
                        ),
                    },
                    "corrections": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "description": (
                                        "Employee ID (Paycom) or Associate ID (ADP). "
                                        "Leading zeros and trailing '.0' are normalized "
                                        "automatically."
                                    ),
                                },
                                "column": {
                                    "type": "string",
                                    "description": (
                                        "Full column header as it appears in the file's "
                                        "header row (e.g. 'FLSA Description', "
                                        "'Primary Address: Zip / Postal Code'). Partial "
                                        "names like 'FLSA' will error if they match "
                                        "multiple columns."
                                    ),
                                },
                                "value": {
                                    "type": "string",
                                    "description": "New value to write into the cell.",
                                },
                            },
                            "required": ["id", "column", "value"],
                        },
                        "description": (
                            "List of row-level overrides to apply. Each item targets "
                            "one cell, identified by (id, column)."
                        ),
                    },
                },
                "required": ["file_path", "corrections"],
            },
        ),

        # --- ADP TOOLS ---
        types.Tool(
            name="adp_total_comparison",
            description=(
                "Performs a complete payroll total comparison between ADP and Uzio reports. "
                "HINT: Always call 'list_audit_files' first to identify the correct paths."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "adp_file_paths": {"type": "array", "items": {"type": "string"}, "description": PATH_DESC},
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "mapping_file_paths": {"type": "array", "items": {"type": "string"}, "description": "Local paths to mapping files (Earnings, Deductions, etc.)"},
                    "mappings_json": {
                        "type": "string", 
                        "description": (
                            "Optional: Flat JSON array of mapping objects. Use this if mapping files aren't available."
                        )
                    },
                    "adp_files_base64": {"type": "array", "items": {"type": "string"}, "description": "Fallback: base64 encoded ADP files"},
                    "uzio_file_base64": {"type": "string", "description": "Fallback: base64 encoded Uzio file"},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="adp_census_audit",
            description="Audits employee census data between Uzio and ADP to find mismatches in names, emails, etc.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string", "description": "Fallback: base64 Uzio file"},
                    "adp_raw_base64": {"type": "string", "description": "Fallback: base64 ADP file"},
                },
            },
        ),
        types.Tool(
            name="adp_deduction_audit",
            description="Compares deduction amounts between Uzio and ADP reports.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "mapping_json": {"type": "string", "description": "JSON mapping of deduction codes"},
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                },
                "required": ["mapping_json"],
            },
        ),
        types.Tool(
            name="adp_payment_audit",
            description="Audits payment methods and amounts between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="adp_withholding_audit",
            description="Audits tax withholding settings between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="adp_census_sanity",
            description=(
                "Applies opt-in auto-corrections to an ADP Census export. "
                "MANDATORY: For stability, always use copy_to_audit_inbox first and then use 'file_path'. "
                "Do NOT use 'file_base64' for files > 1MB."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "file_base64": {"type": "string", "description": "Fallback: base64 encoded ADP Census export"},
                    "filename": {"type": "string"},
                    "fix_flsa": {"type": "boolean"},
                    "fix_emails": {"type": "boolean"},
                    "fix_job_title": {"type": "boolean"},
                    "fix_driver_smart": {"type": "boolean"},
                    "fix_license": {"type": "boolean"},
                    "fix_status": {"type": "boolean"},
                    "fix_type": {"type": "boolean"},
                    "fix_dol_status": {"type": "boolean"},
                    "fix_leave_to_active": {"type": "boolean"},
                    "fix_blank_jt_to_driver": {"type": "boolean"},
                    "fix_std_hours": {"type": "boolean"},
                    "rename_std_hours": {"type": "boolean"},
                    "fix_zip": {"type": "boolean"},
                    "rename_zip_col": {"type": "boolean"},
                    "replace_gender_col": {"type": "boolean"},
                    "sort_by_manager": {"type": "boolean"},
                },
            },
        ),
        types.Tool(
            name="adp_selective_census_sync",
            description=(
                "Updates ONLY the requested columns in a pre-filled Uzio Census Template "
                "(.xlsm) using a fresh ADP census export. Employees not present in the ADP "
                "source are left untouched; everything outside selected_uzio_cols is also "
                "preserved exactly as it was in the template (the .xlsm's VBA, instructions, "
                "and other sheets pass through unchanged).\n\n"
                "Selected columns must be keys of the Uzio raw mapping (e.g. 'Employee ID*', "
                "'Employee First Name*', 'Date of Hire*', 'Standard Hours*', 'Primary "
                "Address: Zip / Postal Code', etc.). Job Title and Work Location are special: "
                "if the caller wants those synced too, pass an explicit job_title_mapping / "
                "work_location_mapping dict (source_value -> Uzio_value). Pass {} to seed "
                "automatically from whatever mapping is already present in the template "
                "(extract_mappings_from_uzio walks the existing template to learn the "
                "convention). Pass null/omit to skip syncing those columns.\n\n"
                "fix_options carries the same toggle keys as adp_census_sanity (fix_emails, "
                "fix_license, fix_status, fix_type, fix_job_title, ...).\n\n"
                "Set discover_only=true to skip writing and instead return the seed "
                "mappings + unique source values so the caller can review before applying."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_base64": {"type": "string", "description": "Fallback: base64 ADP census file"},
                    "filename": {"type": "string", "description": "Optional filename hint when using base64."},
                    "uzio_template_path": {"type": "string", "description": PATH_DESC + " (the pre-filled .xlsm)"},
                    "uzio_template_base64": {"type": "string", "description": "Fallback: base64 Uzio template"},
                    "selected_uzio_cols": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Keys from UZIO_RAW_MAPPING -- the columns to overwrite (e.g. 'Employee SSN', 'Date of Hire*').",
                    },
                    "job_title_mapping": {
                        "type": "object",
                        "description": "Optional {source_job_title: uzio_job_title} dict. Pass {} to seed from template.",
                        "additionalProperties": {"type": "string"},
                    },
                    "work_location_mapping": {
                        "type": "object",
                        "description": "Optional {source_location: uzio_location} dict. Pass {} to seed from template.",
                        "additionalProperties": {"type": "string"},
                    },
                    "fix_options": {
                        "type": "object",
                        "description": "Optional auto-fix toggles (fix_emails, fix_license, fix_status, fix_type, fix_job_title, ...).",
                        "additionalProperties": {"type": "boolean"},
                    },
                    "discover_only": {
                        "type": "boolean",
                        "description": "If true, return only the seed mappings without writing the template.",
                    },
                },
                "required": ["selected_uzio_cols"],
            },
        ),
        types.Tool(
            name="adp_prior_payroll_generator",
            description=(
                "Generates a filled Uzio Prior Payroll Template (.xlsx) from 1-10 ADP "
                "Prior Payroll History files. Each ADP dynamic column is auto-mapped to a "
                "Uzio target column via a fuzzy-string heuristic (handles Medicare, Social "
                "Security, FIT, 401k, FUTA, SUI/SDI, regular/overtime/bonus, state income, "
                "etc.). The auto-mapping can be overridden per-column with override_mapping. "
                "Records are aggregated per (employee, pay-period-start), Net Pay is routed "
                "to the column whose Uzio header contains 'net pay', and a validation pass "
                "flags any employee-period where Gross - Taxes - Deductions != Net Pay.\n\n"
                "WORKFLOW: copy both the blank Uzio template and the ADP file(s) to the "
                "Audit Files inbox first, then pass the resulting paths. The output xlsx is "
                "written to the same inbox and its path returned in the response."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_template_path": {"type": "string", "description": PATH_DESC},
                    "uzio_template_base64": {"type": "string", "description": "Fallback: base64-encoded Uzio Prior Payroll Template (headers only)."},
                    "adp_file_paths": {
                        "type": "array", "items": {"type": "string"},
                        "description": "List of local paths to ADP Prior Payroll History .xlsx files (max 10).",
                    },
                    "adp_files_base64": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Fallback: base64-encoded ADP files (max 10).",
                    },
                    "override_mapping": {
                        "type": "object",
                        "description": (
                            "Optional {adp_column_name: uzio_column_index} override. Use a "
                            "negative integer to force-skip a column. Auto-guessed pairs are "
                            "kept for any ADP column not present in this object."
                        ),
                        "additionalProperties": {"type": "integer"},
                    },
                    "client_name": {
                        "type": "string",
                        "description": "Optional client name; used in the output filename.",
                    },
                },
            },
        ),
        types.Tool(
            name="adp_prior_payroll_sanity",
            description=(
                "Cleans an ADP Prior Payroll export so it can be ingested by downstream APIs. "
                "Three independent fix-ups are applied as needed:\n"
                "  1. Drops interleaved 'Totals For Associate ID XYZ:' summary rows.\n"
                "  2. Detects and removes the bottom-of-file grand-total row where the last "
                "employee's ID got bled into the totals row.\n"
                "  3. Auto-detects per-pay-period exports (multiple rows per associate) and "
                "aggregates to one row per associate -- money/hours SUMmed, period dates "
                "MIN/MAX'd, identity columns kept as-is. Same-pay-date duplicates (real "
                "distinct paychecks in ADP) are also folded in by the SUM aggregation, which "
                "is correct for ADP.\n\n"
                "Optional swap: NET PAY <-> TAKE HOME values can be exchanged (default ON) "
                "because the Carvan-style API maps them reversed. Column headers are NEVER "
                "renamed -- only the data is swapped.\n\n"
                "Output: a CSV file with the input's exact column headers and column order, "
                "saved to the Audit Files inbox. Input accepts .xlsx / .xls / .csv. ADP money "
                "cells are stored as =ROUND(x, 2.0) Excel formulas; this tool evaluates them "
                "with openpyxl + a small formula parser, so values come through correctly."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "file_base64": {"type": "string", "description": "Fallback: base64 encoded ADP Prior Payroll file"},
                    "filename": {"type": "string", "description": "Optional filename hint, used for extension dispatch when file_base64 is supplied."},
                    "swap_net_take": {
                        "type": "boolean",
                        "description": "Swap NET PAY <-> TAKE HOME values (the API expects them reversed). Default true.",
                    },
                    "aggregation_strategy": {
                        "type": "string",
                        "enum": ["full_quarter", "preserve_pay_periods"],
                        "description": (
                            "How to handle multi-row-per-associate files. "
                            "'full_quarter' (default) collapses all rows for an associate "
                            "into one (matches the Streamlit 'Full Quarter (Default)' radio). "
                            "'preserve_pay_periods' keeps distinct pay periods intact and "
                            "only merges same-day duplicate row pairs (matches the Streamlit "
                            "'Preserve Pay Periods' radio)."
                        ),
                    },
                },
            },
        ),
        types.Tool(
            name="adp_prior_payroll_setup_helper",
            description=(
                "Discovers what to configure in Uzio for an ADP Prior Payroll migration. "
                "Given a (sanitized) ADP Prior Payroll file plus the State Tax Code master "
                "CSV, produces an Excel workbook with:\n"
                "  - Earnings catalog (REGULAR / OVERTIME + every ADDITIONAL EARNINGS code)\n"
                "  - Contributions catalog (401k / 403b / 457 / Roth / HSA / FSA codes)\n"
                "  - Deductions catalog with pre-tax vs post-tax verdict per code\n"
                "  - Taxes discovered (every '* - EMPLOYEE/EMPLOYER TAX' column)\n"
                "  - Tax_Mapping sheet (and a CSV alongside) in the "
                "'Payroll_Mappings_Tax_Mapping_CORRECTED' format, with one row per "
                "(tax_type, state) - federal taxes get 1 row, state-scoped taxes (SIT, SDI, "
                "SUTA, FLI) get 1 row per distinct WORKED IN STATE present in the file\n"
                "  - Bonus_Classification (FLSA test: actual OT pay vs 1.5 x regular rate; "
                "any single row showing inflation = bonus is non-discretionary)\n\n"
                "Pre/post-tax algorithm: for each row, gap_FIT = TOTAL EARNINGS minus "
                "FEDERAL INCOME - EMPLOYEE TAXABLE. Find any subset of non-zero deductions "
                "summing to gap_FIT; every member is pre-tax for FIT. One positive proof in "
                "the whole file = pre-tax for everyone (the rule never varies per employee). "
                "Same logic for FICA / MEDI / SIT to derive flavor: section_125 (medical/"
                "dental/vision pre-FIT/FICA/MEDI/SIT) vs 401k_traditional (pre-FIT/SIT only)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "file_base64": {"type": "string", "description": "Fallback: base64 encoded ADP Prior Payroll file (sanitized)."},
                    "filename": {"type": "string", "description": "Optional filename hint, used for extension dispatch when file_base64 is supplied."},
                    "state_tax_master_path": {
                        "type": "string",
                        "description": (
                            "Local path to the State Tax Code master CSV "
                            "(default: C:/Users/shobhit.sharma/Downloads/State Tax Code.csv). "
                            "Required to populate Uzio Tax Code / Unique Tax ID columns."
                        ),
                    },
                    "state_tax_master_base64": {
                        "type": "string",
                        "description": "Fallback: base64 encoded State Tax Code master CSV.",
                    },
                },
            },
        ),
        types.Tool(
            name="adp_census_generator",
            description=(
                "Generates a Uzio Census Template (.xlsm) from an ADP Census export. "
                "Reads ADP columns (e.g. 'Associate ID', 'Legal First Name', "
                "'FLSA Description'), maps them to the Uzio template's expected headers, "
                "applies the same auto-correction toggles as the Streamlit "
                "'ADP - Full Census Generation' tool, and writes the output as a .xlsm to "
                "the Audit Files inbox.\n\n"
                "WORKFLOW: copy the ADP file with copy_to_audit_inbox first, then pass the "
                "resulting path as 'file_path'. The output preserves the Uzio template's "
                "VBA macros, instructions, and all non-data sheets."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "file_base64": {"type": "string", "description": "Fallback: base64-encoded ADP Census export (use only if path unavailable)."},
                    "filename": {"type": "string", "description": "Optional filename hint, used when file_base64 is provided so the loader can pick the right reader (.xlsx vs .csv)."},
                    "fix_flsa": {"type": "boolean", "description": "Enforce FLSA/Pay Type alignment (Hourly→Non-Exempt, Salaried→Exempt; blank→Non-Exempt)."},
                    "fix_emails": {"type": "boolean", "description": "Use Personal Email when Work Email is blank."},
                    "fix_status": {"type": "boolean", "description": "Map Position Status to ACTIVE / TERMINATED / EXCLUDE; 'not hired' rows are dropped."},
                    "fix_inactive": {"type": "boolean", "description": "Map 'Inactive' to TERMINATED only when a Termination Date exists, else ACTIVE."},
                    "fix_type": {"type": "boolean", "description": "Map Worker Category to Full Time / Part Time / Seasonal / Other."},
                    "fix_zip": {"type": "boolean", "description": "Pad 4-digit zips and trim to 5 digits."},
                    "fix_license": {"type": "boolean", "description": "Clear license expiration when license number is missing or 00/00/0000."},
                    "fix_dol_status": {"type": "boolean", "description": "Default blank Employment Type to 'Full Time'."},
                },
            },
        ),
        types.Tool(
            name="paycom_census_generator",
            description=(
                "Generates a Uzio Census Template (.xlsm) from a Paycom Census export. "
                "Reads Paycom columns (e.g. 'Employee_Code', 'Legal_Firstname', "
                "'Exempt_Status'), maps them to the Uzio template's expected headers, "
                "applies the same auto-correction toggles as the Streamlit "
                "'Paycom - Full Census Generation' tool, and writes the output as a "
                ".xlsm to the Audit Files inbox.\n\n"
                "WORKFLOW: copy the Paycom file with copy_to_audit_inbox first, then "
                "pass the resulting path as 'file_path'. The output preserves the Uzio "
                "template's VBA macros, instructions, and all non-data sheets."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "file_base64": {"type": "string", "description": "Fallback: base64-encoded Paycom Census export (use only if path unavailable)."},
                    "filename": {"type": "string", "description": "Optional filename hint, used when file_base64 is provided."},
                    "fix_flsa": {"type": "boolean", "description": "Enforce FLSA/Pay Type alignment."},
                    "fix_emails": {"type": "boolean", "description": "Use Personal_Email when Work_Email is blank."},
                    "fix_status": {"type": "boolean", "description": "Map Employee_Status to ACTIVE / TERMINATED / EXCLUDE."},
                    "fix_inactive": {"type": "boolean", "description": "Map 'Inactive' to TERMINATED only when a Termination_Date exists, else ACTIVE."},
                    "fix_type": {"type": "boolean", "description": "Map DOL_Status to Full Time / Part Time / Seasonal / Other."},
                    "fix_position": {"type": "boolean", "description": "Auto-fill blank Job Title (Position) from Department_Desc."},
                    "fix_dol_status": {"type": "boolean", "description": "Default blank DOL_Status (Employment Type) to 'Full Time'."},
                    "fix_zip": {"type": "boolean", "description": "Pad 4-digit zips and trim to 5 digits."},
                    "fix_license": {"type": "boolean", "description": "Clear license expiration when DriversLicense is missing or 00/00/0000."},
                },
            },
        ),
        types.Tool(
            name="adp_emergency_audit",
            description="Audits emergency contact information between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="adp_license_audit",
            description="Audits professional licenses between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="adp_timeoff_audit",
            description="Audits time-off balances between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "adp_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                },
            },
        ),

        # --- PAYCOM TOOLS ---
        types.Tool(
            name="paycom_selective_census_sync",
            description=(
                "Updates ONLY the requested columns in a pre-filled Uzio Census Template "
                "(.xlsm) using a fresh Paycom census export. Same shape as "
                "adp_selective_census_sync -- selected_uzio_cols are the Uzio raw mapping "
                "keys to overwrite, employees not present in the Paycom source are left "
                "untouched, and the .xlsm's VBA / instruction sheets / unselected columns "
                "all pass through unchanged.\n\n"
                "Job Title and Work Location: pass an explicit dict {source_value: "
                "uzio_value}, pass {} to seed automatically from the existing template, or "
                "omit/null to skip syncing those columns.\n\n"
                "Set discover_only=true to skip writing and just return the seed mappings + "
                "unique source values for caller review."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_base64": {"type": "string", "description": "Fallback: base64 Paycom census file"},
                    "filename": {"type": "string", "description": "Optional filename hint when using base64."},
                    "uzio_template_path": {"type": "string", "description": PATH_DESC + " (the pre-filled .xlsm)"},
                    "uzio_template_base64": {"type": "string", "description": "Fallback: base64 Uzio template"},
                    "selected_uzio_cols": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Keys from UZIO_RAW_MAPPING -- the columns to overwrite.",
                    },
                    "job_title_mapping": {
                        "type": "object",
                        "description": "Optional {source_job_title: uzio_job_title} dict. Pass {} to seed from template.",
                        "additionalProperties": {"type": "string"},
                    },
                    "work_location_mapping": {
                        "type": "object",
                        "description": "Optional {source_location: uzio_location} dict. Pass {} to seed from template.",
                        "additionalProperties": {"type": "string"},
                    },
                    "fix_options": {
                        "type": "object",
                        "description": "Optional auto-fix toggles.",
                        "additionalProperties": {"type": "boolean"},
                    },
                    "discover_only": {
                        "type": "boolean",
                        "description": "If true, return only the seed mappings without writing the template.",
                    },
                },
                "required": ["selected_uzio_cols"],
            },
        ),
        types.Tool(
            name="paycom_prior_payroll_generator",
            description=(
                "Generates a filled Uzio Prior Payroll Template (.xlsx) from 1-10 Paycom "
                "Prior Payroll files (long format with Type Code / Type Description / Code "
                "Description / Amount). Each (type_code, type_description) pair is auto-"
                "mapped to a Uzio target column via a fuzzy-string heuristic. "
                "'Net Pay Distribution' rows are auto-summed into the Uzio 'Net Pay' column; "
                "'Employee Benefits' rows are skipped. Pay-period dates are pulled from the "
                "filename pattern 'Pay Period MMDDYYYY MMDDYYYY Pay Date MMDDYYYY'.\n\n"
                "Records are aggregated per (employee, pay-period); a validation pass flags "
                "any employee-period where Gross - Taxes - Deductions != Net Pay. The auto-"
                "mapping can be overridden per-pair with override_mapping (key format "
                "'type_code|type_description', value is the Uzio column index, or a "
                "negative integer to skip).\n\n"
                "WORKFLOW: copy both the blank Uzio template and the Paycom file(s) to the "
                "Audit Files inbox first, then pass the resulting paths."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_template_path": {"type": "string", "description": PATH_DESC},
                    "uzio_template_base64": {"type": "string", "description": "Fallback: base64-encoded Uzio Prior Payroll Template (headers only)."},
                    "paycom_file_paths": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Local paths to Paycom Prior Payroll .xlsx files (max 10).",
                    },
                    "paycom_files_base64": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Fallback: base64-encoded Paycom files (max 10).",
                    },
                    "override_mapping": {
                        "type": "object",
                        "description": (
                            "Optional {'type_code|type_description': uzio_col_idx} override. "
                            "Use a negative integer to force-skip a pair. Auto-guessed pairs "
                            "are kept for any (tc, td) not present in this object."
                        ),
                        "additionalProperties": {"type": "integer"},
                    },
                    "client_name": {"type": "string", "description": "Optional client name; used in the output filename."},
                },
            },
        ),
        types.Tool(
            name="paycom_deduction_analyzer",
            description="Analyzes Paycom deductions and consolidation plans.",
            inputSchema={
                "type": "object",
                "properties": {
                    "scheduled_report_path": {"type": "string", "description": PATH_DESC},
                    "prior_payroll_path": {"type": "string", "description": PATH_DESC},
                    "config_file_path": {"type": "string", "description": "Optional config file path"},
                    "scheduled_report_base64": {"type": "string"},
                    "prior_payroll_base64": {"type": "string"},
                    "config_file_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="paycom_total_comparison",
            description=(
                "Performs a complete payroll total comparison between Paycom and Uzio reports. "
                "HINT: Always call 'list_audit_files' first to identify the correct paths."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "paycom_file_paths": {"type": "array", "items": {"type": "string"}, "description": PATH_DESC},
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "mapping_file_paths": {"type": "array", "items": {"type": "string"}, "description": "Local paths to mapping files (Earnings, Deductions, etc.)"},
                    "mappings_json": {
                        "type": "string",
                        "description": (
                            "Optional: Flat JSON array of mapping objects. Use this if mapping files aren't available."
                        )
                    },
                    "paycom_files_base64": {"type": "array", "items": {"type": "string"}},
                    "uzio_file_base64": {"type": "string"},
                },
                "required": [],
            },
        ),
        types.Tool(
            name="paycom_deduction_audit",
            description="Compares deduction amounts between Uzio and Paycom reports.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "mapping_json": {"type": "string", "description": "JSON mapping of Paycom Deduction Description -> Uzio Deduction Name"},
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"},
                },
                "required": ["mapping_json"],
            },
        ),
        types.Tool(
            name="paycom_census_audit",
            description="Audits employee census data between Uzio and Paycom to find mismatches in names, emails, etc.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string", "description": "Fallback: base64 Uzio file"},
                    "paycom_raw_base64": {"type": "string", "description": "Fallback: base64 Paycom file"},
                },
            },
        ),
        types.Tool(
            name="paycom_sql_master",
            description="Processes a Paycom SQL Master file into a structured audit report.",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "sql_file_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="paycom_payment_audit",
            description="Audits payment methods and amounts between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="paycom_emergency_audit",
            description="Audits emergency contact information between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="paycom_timeoff_audit",
            description="Audits time-off balances between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="paycom_withholding_audit",
            description="Audits tax withholding settings between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC},
                    "mapping_file_path": {"type": "string"},
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"},
                    "mapping_file_base64": {"type": "string"},
                },
            },
        ),
        types.Tool(
            name="paycom_census_sanity",
            description=(
                "Applies opt-in auto-corrections to a Paycom Census export. "
                "MANDATORY: For stability, always use copy_to_audit_inbox first and then use 'file_path'. "
                "Do NOT use 'file_base64' for files > 1MB."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "file_base64": {"type": "string", "description": "Fallback: base64 encoded Paycom Census export"},
                    "filename": {"type": "string"},
                    "fix_flsa": {"type": "boolean", "description": "Enforce FLSA/Pay Type alignment — fill blank FLSA from Hourly/Salaried."},
                    "fix_emails": {"type": "boolean", "description": "Use Personal Email as fallback when Work Email is blank."},
                    "fix_driver_smart": {"type": "boolean", "description": "Smart Driver Correction: if Dept/Job indicates Driver, fill Job/FLSA/Pay Type."},
                    "fix_license": {"type": "boolean", "description": "Strict license validation — clear license dates if number is missing."},
                    "fix_status": {"type": "boolean", "description": "Auto-map Employment Status (e.g. Inactive -> Terminated)."},
                    "fix_type": {"type": "boolean", "description": "Auto-map Worker Category (e.g. Intern -> Part Time)."},
                    "fix_position": {"type": "boolean", "description": "Auto-Fill blank Position with Department Description."},
                    "fix_dol_status": {"type": "boolean", "description": "Auto-fill blank DOL_Status to 'Full-Time' for active employees."},
                    "fix_zip": {"type": "boolean", "description": "Auto-fix Zip Code (pad 4-digits and trim to 5-digits)."},
                    "sort_by_manager": {"type": "boolean", "description": "Cluster managers and their reportees at the top of the output."},
                },
            },
        ),
        types.Tool(
            name="paycom_consolidated_audit",
            description=(
                "Runs Census + Payment + Emergency contact audits in one pass against the "
                "Uzio Master Custom Report (CSV with category labels in row 1, headers in "
                "row 2) and a Paycom Census export (.xlsx or .csv). Produces a single "
                "consolidated report with these sheets: Summary (per-metric counts), "
                "Duplicate_SSN_Check, Census_Audit, Payment_Audit, Emergency_Audit, plus "
                "anomaly extracts -- Salaried_Drivers, FLSA_Issues, Active_Missing, "
                "Terminated_Missing, Data_Quality, High_Rate_Anomalies.\n\n"
                "Identity matching is done by Employee ID with SSN as a fallback; the "
                "audit honors the same canonical comparisons used by the per-vendor "
                "audits (pay-type aware, termination-reason flexible matching, "
                "phone/zip/SSN/date/money normalizers).\n\n"
                "WORKFLOW: copy both files to the Audit Files inbox first, then pass the "
                "resulting paths."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_file_path": {"type": "string", "description": PATH_DESC + " (Uzio Master Custom Report CSV)"},
                    "paycom_file_path": {"type": "string", "description": PATH_DESC + " (Paycom Census Export xlsx/csv)"},
                    "uzio_raw_base64": {"type": "string", "description": "Fallback: base64-encoded Uzio Master CSV"},
                    "paycom_raw_base64": {"type": "string", "description": "Fallback: base64-encoded Paycom Census Export"},
                    "client_name": {"type": "string", "description": "Optional client name; used in the output filename."},
                },
            },
        ),
        types.Tool(
            name="selective_employee_extractor",
            description="Extracts specific employee records from a payroll/census file based on a list of IDs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "employee_ids": {"type": "array", "items": {"type": "string"}, "description": "List of Employee IDs to extract"},
                    "file_base64": {"type": "string", "description": "Fallback: base64 encoded file"},
                },
                "required": ["employee_ids"],
            },
        ),
        types.Tool(
            name="read_audit_report",
            description=(
                "Reads the contents of any Excel or CSV audit report from the Desktop or Audit Files folder. "
                "Use this to analyze reports that were previously generated or to 'see' the data in a file."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": PATH_DESC},
                    "sheet_name": {"type": "string", "description": "Optional: Name of the sheet to read (defaults to first sheet)"},
                },
                "required": ["file_path"],
            },
        ),
    ]

# ── Tool Handlers ─────────────────────────────────────────────────────────────

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None):
    arguments = arguments or {}
    try:
        if name == "list_audit_files":
            files = []
            scan_dir = arguments.get("directory_path", AUDIT_INBOX).strip().strip('"')
            
            if not os.path.isdir(scan_dir):
                return [types.TextContent(type="text", text=f"Error: The directory '{scan_dir}' does not exist or is not a folder.")]

            try:
                from datetime import datetime
                count = 0
                for root, dirs, filenames in os.walk(scan_dir):
                    for fname in sorted(filenames):
                        if count > 500: break # Safety limit
                        fpath = os.path.join(root, fname)
                        try:
                            stat = os.stat(fpath)
                            files.append({
                                "name": fname,
                                "path": fpath,
                                "size_kb": round(stat.st_size / 1024, 1),
                                "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                            })
                            count += 1
                        except: continue
                    if count > 500: break
            except Exception as e:
                return [types.TextContent(type="text", text=f"Error scanning directory: {str(e)}")]

            result = {
                "scanned_directory": scan_dir,
                "file_count": len(files),
                "files": files,
                "note": "Limited to first 500 files for performance." if len(files) >= 500 else "All files listed.",
                "instruction": "Use the 'path' field from these files as arguments in other tools.",
            }
            return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=_json_default))]

        elif name == "copy_to_audit_inbox":
            source = arguments.get("source_path")
            result = copy_file_to_inbox(source)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=_json_default))]

        elif name == "apply_data_corrections":
            path = arguments.get("file_path")
            corrs = arguments.get("corrections", [])
            result = apply_data_corrections(path, corrs)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=_json_default))]

        elif name == "adp_total_comparison":
            adp_data = load_files_list(arguments, "adp_file_paths", "adp_files_base64")
            uzio_data = (load_file(arguments, "uzio_file_path", "uzio_file_base64"), "uzio.xlsx")
            
            mapping_paths = arguments.get("mapping_file_paths", [])
            if mapping_paths:
                mappings = load_mappings_from_paths(mapping_paths)
            else:
                mappings_raw = arguments.get("mappings_json", "[]")
                try:
                    mappings = json.loads(mappings_raw)
                    if isinstance(mappings, dict):
                        # Flatten if passed as a dict of categories
                        flattened = []
                        for cat, items in mappings.items():
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict):
                                        item["Category"] = item.get("Category", cat)
                                        flattened.append(item)
                        mappings = flattened
                except:
                    return [types.TextContent(type="text", text="Error: mappings_json is not valid JSON.")]

            results = run_adp_total_comparison(adp_data, uzio_data, mappings)
            summary = save_results_to_excel(results, "ADP_Total_Comparison")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_census_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            results = run_adp_census_audit(uzio, adp)
            summary = save_results_to_excel(results, "ADP_Census_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_deduction_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            mapping = json.loads(arguments.get("mapping_json", "{}"))
            results = run_adp_deduction_audit(uzio, adp, mapping)
            summary = save_results_to_excel(results, "ADP_Deduction_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_payment_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            results = run_adp_payment_audit(uzio, adp)
            summary = save_results_to_excel(results, "ADP_Payment_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_withholding_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            results = run_adp_withholding_audit(uzio, adp)
            summary = save_results_to_excel(results, "ADP_Withholding_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_census_sanity":
            content = load_file(arguments, "file_path", "file_base64")
            filename = arguments.get("filename") or "upload.xlsx"
            toggle_keys = [
                "fix_flsa", "fix_emails", "fix_job_title", "fix_driver_smart", "fix_license",
                "fix_status", "fix_type", "fix_dol_status", "fix_leave_to_active",
                "fix_blank_jt_to_driver", "fix_std_hours", "rename_std_hours",
                "fix_zip", "rename_zip_col", "replace_gender_col",
            ]
            fix_options = {k: bool(arguments.get(k, False)) for k in toggle_keys}
            fix_options["fix_inactive"] = fix_options["fix_status"]
            sort_by_manager = bool(arguments.get("sort_by_manager", False))
            xlsx_bytes, summary = generate_corrected_census_xlsx(
                content, ADP_FIELD_MAP, fix_options=fix_options,
                filename=filename, sort_by_manager=sort_by_manager,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            # Save output to Audit Files folder for consistency
            out_path = os.path.join(AUDIT_INBOX, f"ADP_Cleaned_{stamp}.xlsx")
            with open(out_path, "wb") as f:
                f.write(xlsx_bytes)
            payload = {
                "output_file": out_path,
                "summary": summary,
                "applied_toggles": {k: v for k, v in fix_options.items() if v},
            }
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "paycom_consolidated_audit":
            uzio_content = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom_content = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            paycom_path = arguments.get("paycom_file_path")
            paycom_filename = (
                os.path.basename(paycom_path) if paycom_path else "paycom.xlsx"
            )
            client_name = (arguments.get("client_name") or "").strip()
            results = run_paycom_consolidated_audit(uzio_content, paycom_content, paycom_filename)
            prefix = f"Paycom_Consolidated_Audit_{client_name}".rstrip("_") if client_name else "Paycom_Consolidated_Audit"
            summary = save_results_to_excel(results, prefix)
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_selective_census_sync":
            paycom_content = load_file(arguments, "paycom_file_path", "paycom_file_base64")
            uzio_content = load_file(arguments, "uzio_template_path", "uzio_template_base64")
            paycom_path = arguments.get("paycom_file_path")
            filename = arguments.get("filename") or (
                os.path.basename(paycom_path) if paycom_path else "census.xlsx"
            )
            selected = arguments.get("selected_uzio_cols") or []
            job_map = arguments.get("job_title_mapping")
            loc_map = arguments.get("work_location_mapping")
            fix_options = arguments.get("fix_options") or {}

            if arguments.get("discover_only"):
                info = paycom_selective_discover(paycom_content, filename, uzio_content)
                return [types.TextContent(type="text", text=json.dumps(info, indent=2, default=_json_default))]

            xlsm_bytes, summary = run_paycom_selective_census_sync(
                paycom_content, filename, uzio_content,
                selected_uzio_cols=selected,
                job_title_mapping=job_map,
                work_location_mapping=loc_map,
                fix_options=fix_options,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            out_name = f"Uzio_Updated_Paycom_{stamp}.xlsm"
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            out_path = os.path.join(AUDIT_INBOX, out_name)
            with open(out_path, "wb") as f:
                f.write(xlsm_bytes)
            payload = {"output_file": out_path, "summary": summary}
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "paycom_prior_payroll_generator":
            uzio_bytes = load_file(arguments, "uzio_template_path", "uzio_template_base64")
            paycom_files = load_files_list(arguments, "paycom_file_paths", "paycom_files_base64")
            if not paycom_files:
                return [types.TextContent(type="text", text="Error: provide paycom_file_paths or paycom_files_base64.")]
            if len(paycom_files) > 10:
                return [types.TextContent(type="text", text="Error: maximum 10 Paycom files supported.")]
            override_mapping = arguments.get("override_mapping") or None
            client_name = (arguments.get("client_name") or "").strip()
            xlsx_bytes, summary = run_paycom_prior_payroll_generator(
                uzio_bytes, paycom_files, override_mapping=override_mapping,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            base = (client_name + "_" if client_name else "")
            out_name = f"Uzio_Prior_Payroll_Paycom_{base}{stamp}.xlsx".replace(" ", "_")
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            out_path = os.path.join(AUDIT_INBOX, out_name)
            with open(out_path, "wb") as f:
                f.write(xlsx_bytes)
            payload = {"output_file": out_path, "summary": summary}
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "adp_selective_census_sync":
            adp_content = load_file(arguments, "adp_file_path", "adp_file_base64")
            uzio_content = load_file(arguments, "uzio_template_path", "uzio_template_base64")
            adp_path = arguments.get("adp_file_path")
            filename = arguments.get("filename") or (
                os.path.basename(adp_path) if adp_path else "census.xlsx"
            )
            selected = arguments.get("selected_uzio_cols") or []
            job_map = arguments.get("job_title_mapping")
            loc_map = arguments.get("work_location_mapping")
            fix_options = arguments.get("fix_options") or {}

            if arguments.get("discover_only"):
                info = adp_selective_discover(adp_content, filename, uzio_content)
                return [types.TextContent(type="text", text=json.dumps(info, indent=2, default=_json_default))]

            xlsm_bytes, summary = run_adp_selective_census_sync(
                adp_content, filename, uzio_content,
                selected_uzio_cols=selected,
                job_title_mapping=job_map,
                work_location_mapping=loc_map,
                fix_options=fix_options,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            out_name = f"Uzio_Updated_ADP_{stamp}.xlsm"
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            out_path = os.path.join(AUDIT_INBOX, out_name)
            with open(out_path, "wb") as f:
                f.write(xlsm_bytes)
            payload = {"output_file": out_path, "summary": summary}
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "adp_prior_payroll_generator":
            uzio_bytes = load_file(arguments, "uzio_template_path", "uzio_template_base64")
            adp_files = load_files_list(arguments, "adp_file_paths", "adp_files_base64")
            if not adp_files:
                return [types.TextContent(type="text", text="Error: provide adp_file_paths or adp_files_base64.")]
            if len(adp_files) > 10:
                return [types.TextContent(type="text", text="Error: maximum 10 ADP files supported.")]
            override_mapping = arguments.get("override_mapping") or None
            client_name = (arguments.get("client_name") or "").strip()
            xlsx_bytes, summary = run_adp_prior_payroll_generator(
                uzio_bytes, adp_files, override_mapping=override_mapping,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            base = (client_name + "_" if client_name else "")
            out_name = f"Uzio_Prior_Payroll_{base}{stamp}.xlsx".replace(" ", "_")
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            out_path = os.path.join(AUDIT_INBOX, out_name)
            with open(out_path, "wb") as f:
                f.write(xlsx_bytes)
            payload = {"output_file": out_path, "summary": summary}
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "adp_prior_payroll_sanity":
            content = load_file(arguments, "file_path", "file_base64")
            file_path_arg = arguments.get("file_path")
            filename = arguments.get("filename") or (
                os.path.basename(file_path_arg) if file_path_arg else "upload.xlsx"
            )
            swap_net_take = bool(arguments.get("swap_net_take", True))
            agg_strategy = arguments.get("aggregation_strategy") or "full_quarter"
            csv_bytes, summary = run_adp_prior_payroll_sanity(
                content,
                filename=filename,
                swap_net_take=swap_net_take,
                aggregation_strategy=agg_strategy,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            base = os.path.splitext(filename)[0] or "ADP_Prior_Payroll"
            out_name = f"{base}_Sanity_Cleaned_{stamp}.csv"
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            out_path = os.path.join(AUDIT_INBOX, out_name)
            with open(out_path, "wb") as f:
                f.write(csv_bytes)
            payload = {
                "output_file": out_path,
                "summary": summary,
                "swap_applied": summary.get("swap_applied", False),
            }
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "adp_prior_payroll_setup_helper":
            content = load_file(arguments, "file_path", "file_base64")
            file_path_arg = arguments.get("file_path")
            filename = arguments.get("filename") or (
                os.path.basename(file_path_arg) if file_path_arg else "adp_prior_payroll.xlsx"
            )
            master_path = arguments.get("state_tax_master_path") or r"C:\Users\shobhit.sharma\Downloads\State Tax Code.csv"
            master_b64 = arguments.get("state_tax_master_base64")
            master_content = b""
            if master_path and os.path.isfile(master_path.strip().strip('"')):
                with open(master_path.strip().strip('"'), "rb") as f:
                    master_content = f.read()
            elif master_b64:
                master_content = base64.b64decode(master_b64)
            results, csv_bytes = run_adp_prior_payroll_setup_helper(
                content, adp_filename=filename, state_tax_master_content=master_content,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            base = os.path.splitext(filename)[0] or "ADP_Prior_Payroll"
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            tax_csv_path = os.path.join(AUDIT_INBOX, f"{base}_Tax_Mapping_{stamp}.csv")
            with open(tax_csv_path, "wb") as f:
                f.write(csv_bytes)
            summary_info = save_results_to_excel(results, f"{base}_Setup_Helper")
            summary_info["tax_mapping_csv"] = tax_csv_path
            return [types.TextContent(type="text", text=json.dumps(summary_info, indent=2, default=_json_default))]

        elif name == "adp_census_generator":
            content = load_file(arguments, "file_path", "file_base64")
            filename = arguments.get("filename") or os.path.basename(
                (arguments.get("file_path") or "adp_census.xlsx").strip().strip('"')
            )
            toggle_keys = [
                "fix_flsa", "fix_emails", "fix_status", "fix_inactive",
                "fix_type", "fix_zip", "fix_license", "fix_dol_status",
            ]
            fix_options = {k: bool(arguments.get(k, False)) for k in toggle_keys}
            xlsm_bytes, summary = run_adp_census_generation(content, filename, fix_options=fix_options)
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            out_path = os.path.join(AUDIT_INBOX, f"ADP_Uzio_Census_{stamp}.xlsm")
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(xlsm_bytes)
            payload = {
                "output_file": out_path,
                "summary": summary,
                "applied_toggles": {k: v for k, v in fix_options.items() if v},
                "message": f"Uzio Census Template written to {out_path}",
            }
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "paycom_census_generator":
            content = load_file(arguments, "file_path", "file_base64")
            filename = arguments.get("filename") or os.path.basename(
                (arguments.get("file_path") or "paycom_census.xlsx").strip().strip('"')
            )
            toggle_keys = [
                "fix_flsa", "fix_emails", "fix_status", "fix_inactive",
                "fix_type", "fix_position", "fix_dol_status", "fix_zip", "fix_license",
            ]
            fix_options = {k: bool(arguments.get(k, False)) for k in toggle_keys}
            xlsm_bytes, summary = run_paycom_census_generation(content, filename, fix_options=fix_options)
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            out_path = os.path.join(AUDIT_INBOX, f"Paycom_Uzio_Census_{stamp}.xlsm")
            if not os.path.exists(AUDIT_INBOX):
                os.makedirs(AUDIT_INBOX, exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(xlsm_bytes)
            payload = {
                "output_file": out_path,
                "summary": summary,
                "applied_toggles": {k: v for k, v in fix_options.items() if v},
                "message": f"Uzio Census Template written to {out_path}",
            }
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "adp_emergency_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            results = run_adp_emergency_audit(uzio, adp)
            summary = save_results_to_excel(results, "ADP_Emergency_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_license_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            results = run_adp_license_audit(uzio, adp)
            summary = save_results_to_excel(results, "ADP_License_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "adp_timeoff_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            adp = load_file(arguments, "adp_file_path", "adp_raw_base64")
            results = run_adp_timeoff_audit(uzio, adp)
            # Timeoff usually returns a message if it's a template update
            if isinstance(results, dict) and "message" in results:
                return [types.TextContent(type="text", text=json.dumps(results, indent=2, default=_json_default))]
            summary = save_results_to_excel(results, "ADP_Timeoff_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_deduction_analyzer":
            sched = load_file(arguments, "scheduled_report_path", "scheduled_report_base64")
            prior = load_file(arguments, "prior_payroll_path", "prior_payroll_base64")
            config = load_file(arguments, "config_file_path", "config_file_base64") or None
            results = run_paycom_deduction_analysis(sched, prior, config)
            summary = save_results_to_excel(results, "Paycom_Deduction_Analysis")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_total_comparison":
            paycom_data = load_files_list(arguments, "paycom_file_paths", "paycom_files_base64")
            uzio_data = (load_file(arguments, "uzio_file_path", "uzio_file_base64"), "uzio.xlsx")
            
            mapping_paths = arguments.get("mapping_file_paths", [])
            if mapping_paths:
                mappings = load_mappings_from_paths(mapping_paths)
            else:
                mappings_raw = arguments.get("mappings_json", "[]")
                try:
                    mappings = json.loads(mappings_raw)
                    if isinstance(mappings, dict):
                        # Flatten if passed as a dict of categories
                        flattened = []
                        for cat, items in mappings.items():
                            if isinstance(items, list):
                                for item in items:
                                    if isinstance(item, dict):
                                        item["Category"] = item.get("Category", cat)
                                        flattened.append(item)
                        mappings = flattened
                except:
                    return [types.TextContent(type="text", text="Error: mappings_json is not valid JSON.")]

            results = run_paycom_total_comparison(paycom_data, uzio_data, mappings)
            summary = save_results_to_excel(results, "Paycom_Total_Comparison")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_deduction_audit":
            uzio_data = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom_data = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            mapping = json.loads(arguments.get("mapping_json", "{}"))
            results = run_paycom_deduction_audit(uzio_data, paycom_data, mapping)
            summary = save_results_to_excel(results, "Paycom_Deduction_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_census_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            results = run_paycom_census_audit(uzio, paycom)
            summary = save_results_to_excel(results, "Paycom_Census_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_sql_master":
            content = load_file(arguments, "file_path", "sql_file_base64")
            results = run_paycom_sql_master(content)
            summary = save_results_to_excel(results, "Paycom_SQL_Master")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_payment_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            results = run_paycom_payment_audit(uzio, paycom)
            summary = save_results_to_excel(results, "Paycom_Payment_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_emergency_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            results = run_paycom_emergency_audit(uzio, paycom)
            summary = save_results_to_excel(results, "Paycom_Emergency_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_timeoff_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            results = run_paycom_timeoff_audit(uzio, paycom)
            summary = save_results_to_excel(results, "Paycom_Timeoff_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_withholding_audit":
            uzio = load_file(arguments, "uzio_file_path", "uzio_raw_base64")
            paycom = load_file(arguments, "paycom_file_path", "paycom_raw_base64")
            mapping = load_file(arguments, "mapping_file_path", "mapping_file_base64") or None
            results = run_paycom_withholding_audit(uzio, paycom, mapping)
            summary = save_results_to_excel(results, "Paycom_Withholding_Audit")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "paycom_census_sanity":
            content = load_file(arguments, "file_path", "file_base64")
            filename = arguments.get("filename") or "upload.xlsx"
            toggle_keys = [
                "fix_flsa", "fix_emails", "fix_driver_smart", "fix_license",
                "fix_status", "fix_type", "fix_position", "fix_dol_status", "fix_zip",
            ]
            fix_options = {k: bool(arguments.get(k, False)) for k in toggle_keys}
            fix_options["fix_inactive"] = fix_options["fix_status"]
            fix_options["fix_job_title"] = fix_options["fix_position"]
            sort_by_manager = bool(arguments.get("sort_by_manager", False))
            xlsx_bytes, summary = generate_corrected_census_xlsx(
                content, PAYCOM_FIELD_MAP, fix_options=fix_options,
                filename=filename, sort_by_manager=sort_by_manager,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            # Save output to Audit Files folder for consistency
            out_path = os.path.join(AUDIT_INBOX, f"Paycom_Cleaned_{stamp}.xlsx")
            with open(out_path, "wb") as f:
                f.write(xlsx_bytes)
            payload = {
                "output_file": out_path,
                "summary": summary,
                "applied_toggles": {k: v for k, v in fix_options.items() if v},
            }
            return [types.TextContent(type="text", text=json.dumps(payload, indent=2, default=_json_default))]

        elif name == "selective_employee_extractor":
            content = load_file(arguments, "file_path", "file_base64")
            import pandas as pd, io
            from utils.audit_utils import norm_id
            
            # Load file (try Excel then CSV)
            try: df = pd.read_excel(io.BytesIO(content), dtype=str)
            except: df = pd.read_csv(io.BytesIO(content), dtype=str)
            
            target_ids = [norm_id(eid) for eid in arguments.get("employee_ids", [])]
            
            # Find ID column (inlined normalization — no external dependency)
            id_col = next((c for c in df.columns if any(k in str(c).strip().lower() for k in ["employee id", "employee code", "associate id", "ee code", "employee_code"])), df.columns[0])
            
            # Filter
            filtered_df = df[df[id_col].apply(norm_id).isin(target_ids)]
            
            results = filtered_df.to_dict(orient="records")
            summary = save_results_to_excel(results, "Selective_Extraction")
            return [types.TextContent(type="text", text=json.dumps(summary, indent=2, default=_json_default))]

        elif name == "read_audit_report":
            path = arguments.get("file_path", "").strip().strip('"')
            sheet = arguments.get("sheet_name")
            import pandas as pd
            
            # Load all sheets if no specific sheet requested
            if not sheet:
                xls = pd.ExcelFile(path)
                result = {}
                for s in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name=s)
                    result[s] = df.to_dict(orient="records")
            else:
                df = pd.read_excel(path, sheet_name=sheet) if path.lower().endswith(('.xlsx', '.xls')) else pd.read_csv(path)
                result = df.to_dict(orient="records")
                
            return [types.TextContent(type="text", text=json.dumps(result, indent=2, default=_json_default))]

        raise ValueError(f"Unknown tool: {name}")

    except Exception as e:
        import traceback
        return [types.TextContent(type="text", text=f"Error in '{name}': {e}\n\n{traceback.format_exc()}")]


# ── SSE transport (for Vercel / HTTP) ────────────────────────────────────────
sse = SseServerTransport("/messages")

async def handle_sse(request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await server.run(
            streams[0], streams[1],
            InitializationOptions(
                server_name="audit-tool-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

mcp_app = Starlette(routes=[
    Route("/sse", endpoint=handle_sse),
    Mount("/messages", app=sse.handle_post_message),
])

# ── Stdio transport (for local Claude Desktop) ────────────────────────────────
import asyncio
from mcp.server.stdio import stdio_server

async def run_stdio():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream, write_stream,
            InitializationOptions(
                server_name="audit-tool-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(run_stdio())
