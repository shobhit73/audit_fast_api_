import os
import sys
from typing import List, Optional
import traceback
import io
import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

# Define app at top level for Vercel detection
app = FastAPI(title="Audit Tool API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Startup Logic Wrapper
try:
    # ADP Imports
    from core.adp.total_comparison import run_adp_total_comparison
    from core.adp.census_audit import run_adp_census_audit
    from core.adp.deduction_audit import run_adp_deduction_audit
    from core.adp.payment_audit import run_adp_payment_audit
    from core.adp.withholding_audit import run_adp_withholding_audit
    from core.adp.misc_audits import run_adp_emergency_audit, run_adp_license_audit, run_adp_timeoff_audit

    # Paycom Imports
    from core.paycom.deduction_analyzer import run_paycom_deduction_analysis
    from core.paycom.total_comparison import run_paycom_total_comparison
    from core.paycom.census_audit import run_paycom_census_audit
    from core.paycom.withholding_audit import run_paycom_withholding_audit
    from core.paycom.sql_master import run_paycom_sql_master

    from utils.audit_utils import norm_colname

    # MCP Server Integration
    try:
        from mcp_server import mcp_app
        app.mount("/mcp", mcp_app)
    except Exception as e:
        print(f"Warning: Failed to mount MCP app. Error: {e}")

    def load_mapping_from_file(content, filename, cat_name, adp_col, uzio_col):
        try:
            file_io = io.BytesIO(content)
            if filename.lower().endswith('.csv'):
                df = pd.read_csv(file_io)
            else:
                df = pd.read_excel(file_io)
            df.columns = [norm_colname(c) for c in df.columns]
            actual_adp_col = next((c for c in df.columns if adp_col.lower() in c.lower()), None)
            actual_uzio_col = next((c for c in df.columns if uzio_col.lower() in c.lower()), None)
            if not actual_adp_col or not actual_uzio_col: return []
            mappings = []
            for _, row in df.iterrows():
                a_val = str(row[actual_adp_col]).strip()
                u_val = str(row[actual_uzio_col]).strip()
                if a_val and u_val and a_val.lower() != 'nan' and u_val.lower() != 'nan':
                    mappings.append({"Category": cat_name, "ADP_Name": a_val, "UZIO_Name": u_val})
            return mappings
        except: return []

    @app.get("/")
    async def root():
        return {"message": "Audit Tool API is running", "available_endpoints": [
            "/audit/adp/total-comparison", "/audit/adp/census", "/audit/adp/deduction", "/audit/adp/payment", "/audit/adp/withholding",
            "/audit/paycom/total-comparison", "/audit/paycom/census", "/audit/paycom/deduction-analyzer"
        ]}

    # --- ADP ENDPOINTS ---
    @app.post("/audit/adp/total-comparison")
    async def adp_total_comparison(adp_files: List[UploadFile] = File(...), uzio_file: UploadFile = File(...), earn_mapping: UploadFile = File(...), ded_mapping: UploadFile = File(...), cont_mapping: UploadFile = File(...), tax_mapping: UploadFile = File(...)):
        try:
            adp_data = [(await f.read(), f.filename) for f in adp_files]
            uzio_data = (await uzio_file.read(), uzio_file.filename)
            mappings = []
            mappings.extend(load_mapping_from_file(await earn_mapping.read(), earn_mapping.filename, "Earnings", "Source Earning Code Name", "Uzio Earning Code Name"))
            mappings.extend(load_mapping_from_file(await ded_mapping.read(), ded_mapping.filename, "Deductions", "Source Deduction Code Name", "Uzio Deduction Code Name"))
            mappings.extend(load_mapping_from_file(await cont_mapping.read(), cont_mapping.filename, "Contributions", "Source Contribution Code Name", "Uzio Contribution Code Name"))
            mappings.extend(load_mapping_from_file(await tax_mapping.read(), tax_mapping.filename, "Taxes", "Source Tax Code Name", "Uzio Tax Code Description"))
            return run_adp_total_comparison(adp_data, uzio_data, mappings)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/census")
    async def adp_census_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...)):
        try: return {"mismatches": run_adp_census_audit(await uzio_raw.read(), await adp_raw.read())}
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/deduction")
    async def adp_deduction_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...), mapping_json: str = Form("{}")):
        try:
            import json
            mapping = json.loads(mapping_json)
            return run_adp_deduction_audit(await uzio_raw.read(), await adp_raw.read(), mapping)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/payment")
    async def adp_payment_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...)):
        try: return run_adp_payment_audit(await uzio_raw.read(), await adp_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/withholding")
    async def adp_withholding_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...)):
        try: return run_adp_withholding_audit(await uzio_raw.read(), await adp_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    # --- PAYCOM ENDPOINTS ---
    @app.post("/audit/paycom/deduction-analyzer")
    async def paycom_deduction_analyzer(scheduled_report: UploadFile = File(...), prior_payroll: UploadFile = File(...), config_file: Optional[UploadFile] = File(None)):
        try:
            config_content = await config_file.read() if config_file else None
            return run_paycom_deduction_analysis(await scheduled_report.read(), await prior_payroll.read(), config_content)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/total-comparison")
    async def paycom_total_comparison(paycom_files: List[UploadFile] = File(...), uzio_file: UploadFile = File(...), earn_mapping: UploadFile = File(...), ded_mapping: UploadFile = File(...), cont_mapping: UploadFile = File(...), tax_mapping: UploadFile = File(...)):
        try:
            paycom_data = [(await f.read(), f.filename) for f in paycom_files]
            uzio_data = (await uzio_file.read(), uzio_file.filename)
            mappings = [] 
            return run_paycom_total_comparison(paycom_data, uzio_data, mappings)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    from core.census.sanity_check import run_census_sanity_check, generate_corrected_census_xlsx
    from fastapi.responses import StreamingResponse

    @app.post("/audit/adp/census-sanity")
    async def adp_census_sanity(
        file: UploadFile = File(...),
        # Auto-correction toggles (mirror of Streamlit ADP Sanity tool — all default OFF)
        fix_flsa: bool = Form(False),
        fix_emails: bool = Form(False),
        fix_job_title: bool = Form(False),
        fix_driver_smart: bool = Form(False),
        fix_license: bool = Form(False),
        fix_status: bool = Form(False),
        fix_type: bool = Form(False),
        fix_dol_status: bool = Form(False),
        fix_leave_to_active: bool = Form(False),
        fix_blank_jt_to_driver: bool = Form(False),
        fix_std_hours: bool = Form(False),
        rename_std_hours: bool = Form(False),
        fix_zip: bool = Form(False),
        rename_zip_col: bool = Form(False),
        replace_gender_col: bool = Form(False),
        sort_by_manager: bool = Form(False),
    ):
        try:
            content = await file.read()
            from core.adp.census_audit import ADP_FIELD_MAP
            fix_options = {
                'fix_flsa': fix_flsa, 'fix_emails': fix_emails, 'fix_job_title': fix_job_title,
                'fix_driver_smart': fix_driver_smart, 'fix_license': fix_license,
                'fix_status': fix_status, 'fix_inactive': fix_status, 'fix_type': fix_type,
                'fix_dol_status': fix_dol_status, 'fix_leave_to_active': fix_leave_to_active,
                'fix_blank_jt_to_driver': fix_blank_jt_to_driver,
                'fix_std_hours': fix_std_hours, 'rename_std_hours': rename_std_hours,
                'fix_zip': fix_zip, 'rename_zip_col': rename_zip_col,
                'replace_gender_col': replace_gender_col,
            }
            xlsx_bytes, summary = generate_corrected_census_xlsx(
                content, ADP_FIELD_MAP, fix_options=fix_options,
                filename=file.filename or "upload.xlsx",
                sort_by_manager=sort_by_manager,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            headers = {
                "Content-Disposition": f'attachment; filename="ADP_Cleaned_{stamp}.xlsx"',
                "X-Sanity-Summary": f'rows={summary["rows_total"]}; warnings={summary["rows_with_warnings"]}; changes={summary["changes_logged"]}',
            }
            return StreamingResponse(
                io.BytesIO(xlsx_bytes),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/census-sanity")
    async def paycom_census_sanity(
        file: UploadFile = File(...),
        # Auto-correction toggles (mirror of Streamlit Paycom Sanity tool — all default OFF)
        fix_flsa: bool = Form(False),
        fix_emails: bool = Form(False),
        fix_driver_smart: bool = Form(False),
        fix_license: bool = Form(False),
        fix_status: bool = Form(False),
        fix_type: bool = Form(False),
        fix_position: bool = Form(False),
        fix_dol_status: bool = Form(False),
        fix_zip: bool = Form(False),
        sort_by_manager: bool = Form(False),
    ):
        try:
            content = await file.read()
            from core.paycom.census_audit import PAYCOM_FIELD_MAP
            fix_options = {
                'fix_flsa': fix_flsa, 'fix_emails': fix_emails,
                'fix_driver_smart': fix_driver_smart, 'fix_license': fix_license,
                'fix_status': fix_status, 'fix_inactive': fix_status, 'fix_type': fix_type,
                'fix_position': fix_position, 'fix_job_title': fix_position,
                'fix_dol_status': fix_dol_status, 'fix_zip': fix_zip,
            }
            xlsx_bytes, summary = generate_corrected_census_xlsx(
                content, PAYCOM_FIELD_MAP, fix_options=fix_options,
                filename=file.filename or "upload.xlsx",
                sort_by_manager=sort_by_manager,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            headers = {
                "Content-Disposition": f'attachment; filename="Paycom_Cleaned_{stamp}.xlsx"',
                "X-Sanity-Summary": f'rows={summary["rows_total"]}; warnings={summary["rows_with_warnings"]}; changes={summary["changes_logged"]}',
            }
            return StreamingResponse(
                io.BytesIO(xlsx_bytes),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers=headers,
            )
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    from core.adp.prior_payroll_sanity import run_adp_prior_payroll_sanity

    @app.post("/audit/adp/prior-payroll-sanity")
    async def adp_prior_payroll_sanity(
        file: UploadFile = File(...),
        swap_net_take: bool = Form(True),
        aggregation_strategy: str = Form("full_quarter"),
    ):
        try:
            content = await file.read()
            csv_bytes, summary = run_adp_prior_payroll_sanity(
                content,
                filename=file.filename or "upload.xlsx",
                swap_net_take=swap_net_take,
                aggregation_strategy=aggregation_strategy,
            )
            from datetime import datetime
            stamp = datetime.now().strftime("%Y%m%d_%H%M")
            base = os.path.splitext(file.filename or "ADP_Prior_Payroll")[0]
            headers = {
                "Content-Disposition": f'attachment; filename="{base}_Sanity_Cleaned_{stamp}.csv"',
                "X-Sanity-Mode": str(summary.get("mode", "none")),
                "X-Swap-Applied": str(summary.get("swap_applied", False)).lower(),
            }
            return StreamingResponse(io.BytesIO(csv_bytes), media_type="text/csv", headers=headers)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    from core.misc_audits import run_adp_emergency_audit, run_paycom_emergency_audit, run_adp_license_audit, run_adp_timeoff_audit, run_paycom_timeoff_audit, run_paycom_payment_audit

    @app.post("/audit/paycom/payment")
    async def paycom_payment_audit(uzio_raw: UploadFile = File(...), paycom_raw: UploadFile = File(...)):
        try: return run_paycom_payment_audit(await uzio_raw.read(), await paycom_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/emergency")
    async def adp_emergency_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...)):
        try: return run_adp_emergency_audit(await uzio_raw.read(), await adp_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/emergency")
    async def paycom_emergency_audit(uzio_raw: UploadFile = File(...), paycom_raw: UploadFile = File(...)):
        try: return run_paycom_emergency_audit(await uzio_raw.read(), await paycom_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/license")
    async def adp_license_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...)):
        try: return run_adp_license_audit(await uzio_raw.read(), await adp_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/adp/timeoff")
    async def adp_timeoff_audit(uzio_raw: UploadFile = File(...), adp_raw: UploadFile = File(...)):
        try: return run_adp_timeoff_audit(await uzio_raw.read(), await adp_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/timeoff")
    async def paycom_timeoff_audit(uzio_raw: UploadFile = File(...), paycom_raw: UploadFile = File(...)):
        try: return run_paycom_timeoff_audit(await uzio_raw.read(), await paycom_raw.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/withholding")
    async def paycom_withholding_audit(uzio_raw: UploadFile = File(...), paycom_raw: UploadFile = File(...), mapping_file: Optional[UploadFile] = File(None)):
        try:
            mapping_content = await mapping_file.read() if mapping_file else None
            return run_paycom_withholding_audit(await uzio_raw.read(), await paycom_raw.read(), mapping_content)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/sql-master")
    async def paycom_sql_master(sql_file: UploadFile = File(...)):
        try: return run_paycom_sql_master(await sql_file.read())
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

except Exception as e:
    startup_error = f"{str(e)}\n{traceback.format_exc()}"
    @app.get("/")
    async def root_error():
        return {"status": "error", "message": "Startup failed", "details": startup_error}
    @app.get("/{path:path}")
    async def catch_all_error(path: str):
        return {"status": "error", "message": f"Startup failed, cannot handle path: {path}", "details": startup_error}
