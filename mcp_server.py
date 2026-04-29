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
        # Use first sheet if not specified (could be enhanced later)
        ws = wb.active
        
        # 1. Identify columns
        headers = [str(cell.value) for cell in ws[1]]
        norm_headers = [norm_colname(h) for h in headers]
        
        # Find ID column
        id_col_indices = [i for i, h in enumerate(norm_headers) if any(k in h for k in ["employee id", "employee code", "associate id"])]
        if not id_col_indices:
            return {"error": "Could not identify Employee ID column in the file."}
        id_col_idx = id_col_indices[0] # 0-indexed
        
        results = []
        for corr in corrections_list:
            target_id = norm_id(corr.get('id'))
            target_col = norm_colname(corr.get('column'))
            new_val = corr.get('value')
            
            # Find target column index
            col_idx = next((i for i, h in enumerate(norm_headers) if target_col in h), None)
            if col_idx is None:
                results.append({"id": target_id, "status": "Error", "message": f"Column '{corr.get('column')}' not found."})
                continue
            
            # 2. Find row and update
            found = False
            for row_idx in range(2, ws.max_row + 1):
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
                "Performs surgical row-level updates to an Excel file using an Employee ID. "
                "Preserves all original formatting (colors, fonts, borders). "
                "Use this for 'Implementer Overrides' after a sanity check."
            ),
            inputSchema={
                "type": "object", 
                "properties": {
                    "file_path": {
                        "type": "string",
                        "description": "Full local path to the Excel file to modify."
                    },
                    "corrections": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string", "description": "Mandatory Employee ID"},
                                "column": {"type": "string", "description": "Exact or normalized column name to update"},
                                "value": {"type": "string", "description": "New value to write into the cell"}
                            },
                            "required": ["id", "column", "value"]
                        },
                        "description": "List of specific corrections to apply."
                    }
                },
                "required": ["file_path", "corrections"]
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
