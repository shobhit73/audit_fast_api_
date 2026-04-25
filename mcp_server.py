import json
import base64
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.server.sse import SseServerTransport
import mcp.types as types

# --- Core Imports ---
from core.adp.total_comparison import run_adp_total_comparison
from core.adp.census_audit import run_adp_census_audit
from core.adp.deduction_audit import run_adp_deduction_audit
from core.adp.payment_audit import run_adp_payment_audit
from core.adp.withholding_audit import run_adp_withholding_audit

from core.paycom.deduction_analyzer import run_paycom_deduction_analysis
from core.paycom.total_comparison import run_paycom_total_comparison
from core.paycom.census_audit import run_paycom_census_audit, PAYCOM_FIELD_MAP
from core.paycom.withholding_audit import run_paycom_withholding_audit
from core.paycom.sql_master import run_paycom_sql_master

from core.adp.census_audit import ADP_FIELD_MAP
from core.census.sanity_check import run_census_sanity_check
from core.misc_audits import (
    run_adp_emergency_audit, run_paycom_emergency_audit, 
    run_adp_license_audit, run_adp_timeoff_audit, 
    run_paycom_timeoff_audit, run_paycom_payment_audit
)

from starlette.applications import Starlette
from starlette.routing import Mount, Route

server = Server("audit-tool-server")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        # --- ADP TOOLS ---
        types.Tool(
            name="adp_total_comparison",
            description="Performs a complete payroll total comparison between ADP and Uzio reports (Earnings, Deductions, Taxes, Contributions).",
            inputSchema={
                "type": "object",
                "properties": {
                    "adp_files_base64": {"type": "array", "items": {"type": "string"}, "description": "Base64 encoded ADP payroll files"},
                    "uzio_file_base64": {"type": "string", "description": "Base64 encoded Uzio payroll file"},
                    "mappings_json": {"type": "string", "description": "JSON string of code mappings"}
                },
                "required": ["adp_files_base64", "uzio_file_base64", "mappings_json"],
            },
        ),
        types.Tool(
            name="adp_census_audit",
            description="Audits employee census data between Uzio and ADP to find mismatches in names, emails, etc.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string", "description": "Base64 encoded Uzio raw export"},
                    "adp_raw_base64": {"type": "string", "description": "Base64 encoded ADP raw export"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64"],
            },
        ),
        types.Tool(
            name="adp_deduction_audit",
            description="Compares deduction amounts between Uzio and ADP reports.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"},
                    "mapping_json": {"type": "string", "description": "JSON mapping of deduction codes"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64", "mapping_json"],
            },
        ),
        types.Tool(
            name="adp_payment_audit",
            description="Audits payment methods and amounts between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64"],
            },
        ),
        types.Tool(
            name="adp_withholding_audit",
            description="Audits tax withholding settings between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64"],
            },
        ),
        types.Tool(
            name="adp_census_sanity",
            description="Performs a sanity check on ADP census files.",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_base64": {"type": "string"}
                },
                "required": ["file_base64"],
            },
        ),
        types.Tool(
            name="adp_emergency_audit",
            description="Audits emergency contact information between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64"],
            },
        ),
        types.Tool(
            name="adp_license_audit",
            description="Audits professional licenses between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64"],
            },
        ),
        types.Tool(
            name="adp_timeoff_audit",
            description="Audits time-off balances between Uzio and ADP.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "adp_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "adp_raw_base64"],
            },
        ),

        # --- PAYCOM TOOLS ---
        types.Tool(
            name="paycom_deduction_analyzer",
            description="Analyzes Paycom deductions and consolidation plans. Identifies overlaps and provides merge reasoning.",
            inputSchema={
                "type": "object",
                "properties": {
                    "scheduled_report_base64": {"type": "string", "description": "Base64 encoded Paycom Scheduled Report"},
                    "prior_payroll_base64": {"type": "string", "description": "Base64 encoded Prior Payroll report"},
                    "config_file_base64": {"type": "string", "description": "Optional Base64 encoded config file"}
                },
                "required": ["scheduled_report_base64", "prior_payroll_base64"],
            },
        ),
        types.Tool(
            name="paycom_total_comparison",
            description="Performs a complete payroll total comparison between Paycom and Uzio reports.",
            inputSchema={
                "type": "object",
                "properties": {
                    "paycom_files_base64": {"type": "array", "items": {"type": "string"}},
                    "uzio_file_base64": {"type": "string"},
                    "mappings_json": {"type": "string"}
                },
                "required": ["paycom_files_base64", "uzio_file_base64", "mappings_json"],
            },
        ),
        types.Tool(
            name="paycom_sql_master",
            description="Processes a Paycom SQL Master file into a structured audit report.",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql_file_base64": {"type": "string"}
                },
                "required": ["sql_file_base64"],
            },
        ),
        types.Tool(
            name="paycom_payment_audit",
            description="Audits payment methods and amounts between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "paycom_raw_base64"],
            },
        ),
        types.Tool(
            name="paycom_emergency_audit",
            description="Audits emergency contact information between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "paycom_raw_base64"],
            },
        ),
        types.Tool(
            name="paycom_timeoff_audit",
            description="Audits time-off balances between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "paycom_raw_base64"],
            },
        ),
        types.Tool(
            name="paycom_withholding_audit",
            description="Audits tax withholding settings between Uzio and Paycom.",
            inputSchema={
                "type": "object",
                "properties": {
                    "uzio_raw_base64": {"type": "string"},
                    "paycom_raw_base64": {"type": "string"},
                    "mapping_file_base64": {"type": "string"}
                },
                "required": ["uzio_raw_base64", "paycom_raw_base64"],
            },
        ),
        types.Tool(
            name="paycom_census_sanity",
            description="Performs a sanity check on Paycom census files.",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_base64": {"type": "string"}
                },
                "required": ["file_base64"],
            },
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None):
    # Helper to decode and prepare file data
    def decode_file(b64, default_name="file.xlsx"):
        return base64.b64decode(b64) if b64 else b""

    try:
        if name == "adp_total_comparison":
            adp_data = [(decode_file(b64), f"adp_{i}.xlsx") for i, b64 in enumerate(arguments.get("adp_files_base64", []))]
            uzio_data = (decode_file(arguments.get("uzio_file_base64")), "uzio.xlsx")
            mappings = json.loads(arguments.get("mappings_json", "[]"))
            results = run_adp_total_comparison(adp_data, uzio_data, mappings)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_census_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            results = run_adp_census_audit(uzio_content, adp_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_deduction_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            mapping = json.loads(arguments.get("mapping_json", "{}"))
            results = run_adp_deduction_audit(uzio_content, adp_content, mapping)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_deduction_analyzer":
            sched = decode_file(arguments.get("scheduled_report_base64"))
            prior = decode_file(arguments.get("prior_payroll_base64"))
            config = decode_file(arguments.get("config_file_base64")) if "config_file_base64" in arguments else None
            results = run_paycom_deduction_analysis(sched, prior, config)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_total_comparison":
            paycom_data = [(decode_file(b64), f"paycom_{i}.xlsx") for i, b64 in enumerate(arguments.get("paycom_files_base64", []))]
            uzio_data = (decode_file(arguments.get("uzio_file_base64")), "uzio.xlsx")
            mappings = json.loads(arguments.get("mappings_json", "[]"))
            results = run_paycom_total_comparison(paycom_data, uzio_data, mappings)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_sql_master":
            content = decode_file(arguments.get("sql_file_base64"))
            results = run_paycom_sql_master(content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_payment_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            results = run_adp_payment_audit(uzio_content, adp_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_withholding_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            results = run_adp_withholding_audit(uzio_content, adp_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_census_sanity":
            content = decode_file(arguments.get("file_base64"))
            import pandas as pd
            import io
            df = pd.read_excel(io.BytesIO(content), dtype=str)
            results = run_census_sanity_check(df, ADP_FIELD_MAP)
            # Convert DataFrame to list of dicts for JSON serialization
            if hasattr(results, "to_dict"): results = results.to_dict(orient="records")
            elif isinstance(results, dict) and "hard_errors" in results:
                if hasattr(results["hard_errors"], "to_dict"):
                    results["hard_errors"] = results["hard_errors"].to_dict(orient="records")
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_emergency_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            results = run_adp_emergency_audit(uzio_content, adp_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_license_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            results = run_adp_license_audit(uzio_content, adp_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "adp_timeoff_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            adp_content = decode_file(arguments.get("adp_raw_base64"))
            results = run_adp_timeoff_audit(uzio_content, adp_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_payment_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            paycom_content = decode_file(arguments.get("paycom_raw_base64"))
            results = run_paycom_payment_audit(uzio_content, paycom_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_emergency_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            paycom_content = decode_file(arguments.get("paycom_raw_base64"))
            results = run_paycom_emergency_audit(uzio_content, paycom_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_timeoff_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            paycom_content = decode_file(arguments.get("paycom_raw_base64"))
            results = run_paycom_timeoff_audit(uzio_content, paycom_content)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_withholding_audit":
            uzio_content = decode_file(arguments.get("uzio_raw_base64"))
            paycom_content = decode_file(arguments.get("paycom_raw_base64"))
            mapping = decode_file(arguments.get("mapping_file_base64")) if "mapping_file_base64" in arguments else None
            results = run_paycom_withholding_audit(uzio_content, paycom_content, mapping)
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        elif name == "paycom_census_sanity":
            content = decode_file(arguments.get("file_base64"))
            import pandas as pd
            import io
            df = pd.read_excel(io.BytesIO(content), dtype=str)
            results = run_census_sanity_check(df, PAYCOM_FIELD_MAP)
            if hasattr(results, "to_dict"): results = results.to_dict(orient="records")
            elif isinstance(results, dict) and "hard_errors" in results:
                if hasattr(results["hard_errors"], "to_dict"):
                    results["hard_errors"] = results["hard_errors"].to_dict(orient="records")
            return [types.TextContent(type="text", text=json.dumps(results, indent=2))]

        raise ValueError(f"Unknown tool: {name}")
    except Exception as e:
        import traceback
        error_msg = f"Error executing tool '{name}': {str(e)}\n\nTraceback:\n{traceback.format_exc()}"
        return [types.TextContent(type="text", text=error_msg)]

# ── SSE transport ──
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
