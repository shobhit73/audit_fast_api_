import os
import sys
from typing import List, Optional

# Add the parent directory to sys.path to allow imports from core and utils
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root not in sys.path:
    sys.path.append(root)

startup_error = None
try:
    from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Header
    from fastapi.middleware.cors import CORSMiddleware
    import pandas as pd
    import io
    import traceback

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

    app = FastAPI(title="Audit Tool API")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

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
    async def adp_total_comparison(
        adp_files: List[UploadFile] = File(...),
        uzio_file: UploadFile = File(...),
        earn_mapping: UploadFile = File(...),
        ded_mapping: UploadFile = File(...),
        cont_mapping: UploadFile = File(...),
        tax_mapping: UploadFile = File(...)
    ):
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
            mappings = [] # Mapping logic same as ADP
            return run_paycom_total_comparison(paycom_data, uzio_data, mappings)
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    from core.census.sanity_check import run_census_sanity_check

    @app.post("/audit/adp/census-sanity")
    async def adp_census_sanity(file: UploadFile = File(...)):
        try:
            content = await file.read()
            from core.adp.census_audit import ADP_FIELD_MAP
            resolved_map = {k: v for k, v in ADP_FIELD_MAP.items()} 
            df = pd.read_excel(io.BytesIO(content), dtype=str)
            results = run_census_sanity_check(df, resolved_map)
            return results
        except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    @app.post("/audit/paycom/census-sanity")
    async def paycom_census_sanity(file: UploadFile = File(...)):
        try:
            content = await file.read()
            from core.paycom.census_audit import PAYCOM_FIELD_MAP
            resolved_map = {k: v for k, v in PAYCOM_FIELD_MAP.items()}
            df = pd.read_excel(io.BytesIO(content), dtype=str)
            results = run_census_sanity_check(df, resolved_map)
            return results
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
    from fastapi import FastAPI
    import traceback
    startup_error = f"{str(e)}\n{traceback.format_exc()}"
    app = FastAPI(title="Audit Tool API (Startup Error)")

    @app.get("/")
    async def root():
        return {
            "status": "error",
            "message": "Startup failed",
            "details": startup_error,
            "sys_path": sys.path,
            "cwd": os.getcwd()
        }

    @app.get("/{path:path}")
    async def catch_all(path: str):
        return {
            "status": "error",
            "message": f"Startup failed, cannot handle path: {path}",
            "details": startup_error
        }
