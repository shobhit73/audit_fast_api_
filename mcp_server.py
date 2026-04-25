import asyncio
from mcp.server.models import InitializationOptions
from mcp.server import Notification, Server
from mcp.server.stdio import stdio_server
import mcp.types as types
from core.adp.total_comparison import run_adp_total_comparison
import json
import base64

server = Server("audit-tool-server")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """
    List available tools.
    """
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
                        "description": "Base64 encoded contents of ADP payroll files"
                    },
                    "uzio_file_base64": {
                        "type": "string",
                        "description": "Base64 encoded content of Uzio payroll file"
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
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """
    Handle tool calls.
    """
    if name == "adp_total_comparison":
        adp_files_b64 = arguments.get("adp_files_base64", [])
        uzio_file_b64 = arguments.get("uzio_file_base64", "")
        mappings = json.loads(arguments.get("mappings_json", "[]"))

        adp_data = []
        for i, b64 in enumerate(adp_files_b64):
            content = base64.b64decode(b64)
            adp_data.append((content, f"adp_file_{i}.xlsx"))
        
        uzio_content = base64.b64decode(uzio_file_b64)
        
        results = run_adp_total_comparison(adp_data, (uzio_content, "uzio_file.xlsx"), mappings)
        
        return [
            types.TextContent(
                type="text",
                text=json.dumps(results, indent=2)
            )
        ]
    
    raise ValueError(f"Unknown tool: {name}")

async def main():
    # Run the server using stdin/stdout streams
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="audit-tool-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=Notification(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
