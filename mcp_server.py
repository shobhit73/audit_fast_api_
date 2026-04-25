import json
import base64
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.server.sse import SseServerTransport
import mcp.types as types
from core.adp.total_comparison import run_adp_total_comparison
from starlette.applications import Starlette
from starlette.routing import Mount, Route

server = Server("audit-tool-server")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="adp_total_comparison",
            description="Performs a payroll total comparison between ADP and Uzio reports.",
            inputSchema={
                "type": "object",
                "properties": {
                    "adp_files_base64": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Base64 encoded ADP payroll files"
                    },
                    "uzio_file_base64": {
                        "type": "string",
                        "description": "Base64 encoded Uzio payroll file"
                    },
                    "mappings_json": {
                        "type": "string",
                        "description": "JSON string of mappings"
                    }
                },
                "required": ["adp_files_base64", "uzio_file_base64", "mappings_json"],
            },
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict | None):
    if name == "adp_total_comparison":
        adp_files_b64 = arguments.get("adp_files_base64", [])
        uzio_file_b64 = arguments.get("uzio_file_base64", "")
        mappings = json.loads(arguments.get("mappings_json", "[]"))

        adp_data = [(base64.b64decode(b64), f"adp_file_{i}.xlsx") for i, b64 in enumerate(adp_files_b64)]
        uzio_content = base64.b64decode(uzio_file_b64)
        results = run_adp_total_comparison(adp_data, (uzio_content, "uzio_file.xlsx"), mappings)

        return [types.TextContent(type="text", text=json.dumps(results, indent=2))]
    raise ValueError(f"Unknown tool: {name}")


# ── SSE transport (for remote MCP clients like Claude Desktop) ──
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

# Starlette app — mount this inside your FastAPI app
mcp_app = Starlette(routes=[
    Route("/sse", endpoint=handle_sse),
    Mount("/messages", app=sse.handle_post_message),
])
