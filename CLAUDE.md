# CLAUDE.md

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

This path is **hardcoded to the original developer's machine**. The `list_audit_files` tool reads this folder so Claude Desktop can discover files without the user pasting paths. If you're working in a different environment, this needs to be configurable (env var) before it's useful elsewhere.

#### File input convention (every MCP tool)

Each tool accepts both a local path *and* a base64 fallback — see `load_file()` and `load_files_list()` in [mcp_server.py](mcp_server.py). Path is preferred; base64 is for HTTP/remote callers. Don't add a tool that only accepts one of them — keep the pair.

#### Output convention

`save_results_to_excel()` writes results to `~/Desktop/<Prefix>_<timestamp>.xlsx` and returns a JSON summary with a top-10 preview. The MCP response is the summary — the full report is the file on disk. Keep this contract: streaming megabytes of audit results back through MCP will exceed token limits.

`_json_default()` handles numpy/pandas/datetime serialization. Use it (`json.dumps(..., default=_json_default)`) anywhere you're returning audit results, since pandas leaks `np.int64`/`Timestamp` into dicts in non-obvious places.

## core/ — audit implementations

**Many modules in `core/` are stubs.** Don't trust the directory listing — check the file. Known stubs that just return `{"message": "...", "results": []}`:

- [core/adp/misc_audits.py](core/adp/misc_audits.py) — emergency, license, timeoff
- [core/misc_audits.py](core/misc_audits.py) — emergency (both vendors), license, timeoff, paycom payment

Real implementations live in: [core/adp/census_audit.py](core/adp/census_audit.py), [core/adp/deduction_audit.py](core/adp/deduction_audit.py), [core/adp/payment_audit.py](core/adp/payment_audit.py), [core/adp/withholding_audit.py](core/adp/withholding_audit.py), [core/adp/total_comparison.py](core/adp/total_comparison.py), [core/paycom/census_audit.py](core/paycom/census_audit.py), [core/paycom/deduction_analyzer.py](core/paycom/deduction_analyzer.py), [core/paycom/total_comparison.py](core/paycom/total_comparison.py), [core/paycom/withholding_audit.py](core/paycom/withholding_audit.py), [core/paycom/sql_master.py](core/paycom/sql_master.py), and [core/census/sanity_check.py](core/census/sanity_check.py).

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

[utils/audit_utils.py](utils/audit_utils.py) is a slimmed-down version of the Streamlit project's helper module. It only contains read/normalize utilities (`norm_colname`, `norm_blank`, `norm_ssn_canonical`, `read_uzio_raw_file`, `find_header_and_data`, identity-matching helpers, etc.). It does **not** contain the Uzio template generator (`generate_uzio_template`, `inject_into_uzio_template`) — those are Streamlit-only.

If you find yourself wanting to import something from `utils/audit_utils.py` that isn't there, it probably exists in the Streamlit parent's `utils/audit_utils.py` and needs to be ported.

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
