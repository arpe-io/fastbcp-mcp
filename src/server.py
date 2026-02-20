#!/usr/bin/env python3
"""
FastBCP MCP Server

A Model Context Protocol (MCP) server that exposes FastBCP functionality
for exporting data from databases to files (CSV, TSV, JSON, BSON, Parquet,
XLSX, Binary) with optional cloud storage targets.

This server provides six tools:
1. preview_export_command - Build and preview command without executing
2. execute_export - Execute a previously previewed command with confirmation
3. validate_connection - Validate source connection parameters
4. list_supported_formats - Show supported sources, formats, and storage targets
5. suggest_parallelism_method - Recommend parallelism method
6. get_version - Report FastBCP version and capabilities
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import Any, Dict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    from mcp.server import Server
    from mcp.types import Tool, TextContent
    from pydantic import ValidationError
except ImportError as e:
    print(f"Error: Required package not found: {e}", file=sys.stderr)
    print("Please run: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)

from src.validators import (
    ExportRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
    SourceConnectionType,
    OutputFormat,
    ParallelismMethod,
    StorageTarget,
    ParquetCompression,
    LoadMode,
    MapMethod,
    LogLevel,
    DecimalSeparator,
    ApplicationIntent,
    BoolFormat,
)
from src.fastbcp import (
    CommandBuilder,
    FastBCPError,
    get_supported_formats,
    suggest_parallelism_method,
)


# Load environment variables
load_dotenv()

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger(__name__)

# Configuration
FASTBCP_PATH = os.getenv("FASTBCP_PATH", "./fastbcp/FastBCP")
FASTBCP_TIMEOUT = int(os.getenv("FASTBCP_TIMEOUT", "1800"))
FASTBCP_LOG_DIR = Path(os.getenv("FASTBCP_LOG_DIR", "./logs"))

# Initialize MCP server
app = Server("fastbcp")

# Global command builder instance
try:
    command_builder = CommandBuilder(FASTBCP_PATH)
    version_info = command_builder.get_version()
    logger.info(f"FastBCP binary found at: {FASTBCP_PATH}")
    if version_info["detected"]:
        logger.info(f"FastBCP version: {version_info['version']}")
    else:
        logger.warning("FastBCP version could not be detected")
except FastBCPError as e:
    logger.error(f"Failed to initialize CommandBuilder: {e}")
    command_builder = None


@app.list_tools()
async def list_tools() -> list[Tool]:
    """List all available MCP tools."""
    return [
        Tool(
            name="preview_export_command",
            description=(
                "Build and preview a FastBCP export command WITHOUT executing it. "
                "This shows the exact command that will be run, with passwords masked. "
                "Use this FIRST before executing any export."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": [e.value for e in SourceConnectionType],
                                "description": "Source database connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address (host:port or host\\instance)",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "schema": {
                                "type": "string",
                                "description": "Schema name (optional)",
                            },
                            "table": {
                                "type": "string",
                                "description": "Table name (optional if query provided)",
                            },
                            "query": {
                                "type": "string",
                                "description": "SQL query (alternative to table)",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                                "default": False,
                            },
                            "connect_string": {
                                "type": "string",
                                "description": "Full connection string (alternative to server/user/password)",
                            },
                            "dsn": {
                                "type": "string",
                                "description": "ODBC DSN name",
                            },
                            "provider": {
                                "type": "string",
                                "description": "OleDB provider name",
                            },
                            "application_intent": {
                                "type": "string",
                                "enum": [e.value for e in ApplicationIntent],
                                "description": "SQL Server application intent",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "output": {
                        "type": "object",
                        "properties": {
                            "format": {
                                "type": "string",
                                "enum": [e.value for e in OutputFormat],
                                "description": "Output file format",
                            },
                            "file_output": {
                                "type": "string",
                                "description": "Output file path",
                            },
                            "directory": {
                                "type": "string",
                                "description": "Output directory path",
                            },
                            "storage_target": {
                                "type": "string",
                                "enum": [e.value for e in StorageTarget],
                                "description": "Storage target for output",
                                "default": "local",
                            },
                            "delimiter": {
                                "type": "string",
                                "description": "Field delimiter (CSV/TSV)",
                            },
                            "quotes": {
                                "type": "string",
                                "description": "Quote character",
                            },
                            "encoding": {
                                "type": "string",
                                "description": "Output file encoding",
                            },
                            "no_header": {
                                "type": "boolean",
                                "description": "Omit header row (CSV/TSV)",
                                "default": False,
                            },
                            "decimal_separator": {
                                "type": "string",
                                "enum": [e.value for e in DecimalSeparator],
                                "description": "Decimal separator for numeric values",
                            },
                            "date_format": {
                                "type": "string",
                                "description": "Date format string",
                            },
                            "bool_format": {
                                "type": "string",
                                "enum": [e.value for e in BoolFormat],
                                "description": "Boolean output format",
                            },
                            "parquet_compression": {
                                "type": "string",
                                "enum": [e.value for e in ParquetCompression],
                                "description": "Parquet compression algorithm",
                            },
                            "timestamped": {
                                "type": "boolean",
                                "description": "Add timestamp to output filename",
                                "default": False,
                            },
                            "merge": {
                                "type": "boolean",
                                "description": "Merge parallel output files",
                                "default": False,
                            },
                        },
                        "required": ["format"],
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "method": {
                                "type": "string",
                                "enum": [e.value for e in ParallelismMethod],
                                "description": "Parallelism method",
                                "default": "None",
                            },
                            "distribute_key_column": {
                                "type": "string",
                                "description": "Column for data distribution",
                            },
                            "degree": {
                                "type": "integer",
                                "description": "Parallelism degree",
                                "default": 1,
                            },
                            "load_mode": {
                                "type": "string",
                                "enum": [e.value for e in LoadMode],
                                "description": "Load mode",
                                "default": "Append",
                            },
                            "batch_size": {
                                "type": "integer",
                                "description": "Batch size for export operations",
                            },
                            "map_method": {
                                "type": "string",
                                "enum": [e.value for e in MapMethod],
                                "description": "Column mapping method",
                                "default": "Position",
                            },
                            "run_id": {
                                "type": "string",
                                "description": "Run ID for logging",
                            },
                            "data_driven_query": {
                                "type": "string",
                                "description": "Custom SQL query for DataDriven parallelism method",
                            },
                            "settings_file": {
                                "type": "string",
                                "description": "Path to custom settings JSON file",
                            },
                            "log_level": {
                                "type": "string",
                                "enum": [e.value for e in LogLevel],
                                "description": "Override log level",
                            },
                            "no_banner": {
                                "type": "boolean",
                                "description": "Suppress the FastBCP banner",
                            },
                            "license_path": {
                                "type": "string",
                                "description": "Path or URL to license file",
                            },
                            "cloud_profile": {
                                "type": "string",
                                "description": "Cloud storage profile name",
                            },
                        },
                    },
                },
                "required": ["source", "output"],
            },
        ),
        Tool(
            name="execute_export",
            description=(
                "Execute a FastBCP export command that was previously previewed. "
                "IMPORTANT: You must set confirmation=true to execute. "
                "This is a safety mechanism to prevent accidental execution."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "The exact command from preview_export_command (including actual passwords)",
                    },
                    "confirmation": {
                        "type": "boolean",
                        "description": "Must be true to execute. This confirms the user has reviewed the command.",
                    },
                },
                "required": ["command", "confirmation"],
            },
        ),
        Tool(
            name="validate_connection",
            description=(
                "Validate source database connection parameters. "
                "This checks that all required parameters are provided but does NOT "
                "actually test connectivity (would require database access)."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "connection": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "description": "Connection type",
                            },
                            "server": {
                                "type": "string",
                                "description": "Server address",
                            },
                            "database": {
                                "type": "string",
                                "description": "Database name",
                            },
                            "user": {"type": "string", "description": "Username"},
                            "password": {"type": "string", "description": "Password"},
                            "connect_string": {
                                "type": "string",
                                "description": "Full connection string (alternative to server/user/password)",
                            },
                            "dsn": {
                                "type": "string",
                                "description": "ODBC DSN name",
                            },
                            "provider": {
                                "type": "string",
                                "description": "OleDB provider name",
                            },
                            "trusted_auth": {
                                "type": "boolean",
                                "description": "Use trusted authentication",
                            },
                        },
                        "required": ["type", "database"],
                    },
                    "side": {
                        "type": "string",
                        "enum": ["source", "target"],
                        "description": "Connection side",
                    },
                },
                "required": ["connection", "side"],
            },
        ),
        Tool(
            name="list_supported_formats",
            description="List all supported source databases, output formats, and storage targets.",
            inputSchema={"type": "object", "properties": {}},
        ),
        Tool(
            name="suggest_parallelism_method",
            description=(
                "Suggest the optimal parallelism method based on source database type "
                "and table characteristics. Provides recommendations for best performance."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "description": "Source database type (e.g., 'pgsql', 'oraodp', 'mssql')",
                    },
                    "has_numeric_key": {
                        "type": "boolean",
                        "description": "Whether the table has a numeric key column",
                    },
                    "has_identity_column": {
                        "type": "boolean",
                        "description": "Whether the table has an identity/auto-increment column",
                        "default": False,
                    },
                    "table_size_estimate": {
                        "type": "string",
                        "enum": ["small", "medium", "large"],
                        "description": "Estimated table size",
                    },
                },
                "required": ["source_type", "has_numeric_key", "table_size_estimate"],
            },
        ),
        Tool(
            name="get_version",
            description=(
                "Get the detected FastBCP binary version, capabilities, "
                "and supported source types, output formats, and storage targets."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Any) -> list[TextContent]:
    """Handle tool calls."""
    try:
        if name == "preview_export_command":
            return await handle_preview_export(arguments)
        elif name == "execute_export":
            return await handle_execute_export(arguments)
        elif name == "validate_connection":
            return await handle_validate_connection(arguments)
        elif name == "list_supported_formats":
            return await handle_list_formats(arguments)
        elif name == "suggest_parallelism_method":
            return await handle_suggest_parallelism(arguments)
        elif name == "get_version":
            return await handle_get_version(arguments)
        else:
            return [TextContent(type="text", text=f"Error: Unknown tool '{name}'")]

    except Exception as e:
        logger.exception(f"Error handling tool '{name}': {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_preview_export(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle preview_export_command tool."""
    if command_builder is None:
        return [
            TextContent(
                type="text",
                text=(
                    "Error: FastBCP binary not found or not accessible.\n"
                    f"Expected location: {FASTBCP_PATH}\n"
                    "Please set FASTBCP_PATH environment variable correctly."
                ),
            )
        ]

    try:
        # Validate and parse request
        request = ExportRequest(**arguments)

        # Build command
        command = command_builder.build_command(request)

        # Format for display (with masked passwords)
        display_command = command_builder.format_command_display(command, mask=True)

        # Create explanation
        explanation = _build_export_explanation(request)

        # Build response
        response = [
            "# FastBCP Command Preview",
            "",
            "## What this command will do:",
            explanation,
            "",
            "## Command (passwords masked):",
            "```bash",
            display_command,
            "```",
            "",
            "## To execute this export:",
            "1. Review the command carefully",
            "2. Use the `execute_export` tool with the FULL command (not the masked version)",
            "3. Set `confirmation: true` to proceed",
            "",
            "## Security Notice:",
            "- Passwords are masked in this preview (shown as ******)",
            "- The actual execution will use the real passwords you provided",
            "- All executions are logged (with masked passwords) to: "
            + str(FASTBCP_LOG_DIR),
            "",
            "## Full command for execution:",
            "```",
            " ".join(command),
            "```",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    except ValidationError as e:
        error_msg = [
            "# Validation Error",
            "",
            "The provided parameters are invalid:",
            "",
        ]
        for error in e.errors():
            field = " -> ".join(str(x) for x in error["loc"])
            error_msg.append(f"- **{field}**: {error['msg']}")
        return [TextContent(type="text", text="\n".join(error_msg))]

    except FastBCPError as e:
        return [TextContent(type="text", text=f"Error: {str(e)}")]


async def handle_execute_export(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle execute_export tool."""
    if command_builder is None:
        return [
            TextContent(
                type="text",
                text="Error: FastBCP binary not found. Please check FASTBCP_PATH.",
            )
        ]

    # Check confirmation
    if not arguments.get("confirmation", False):
        return [
            TextContent(
                type="text",
                text=(
                    "# Execution Blocked\n\n"
                    "You must set `confirmation: true` to execute an export.\n"
                    "This safety mechanism ensures commands are only executed with explicit approval.\n\n"
                    "Please review the command carefully and confirm by setting:\n"
                    "```json\n"
                    '{"confirmation": true}\n'
                    "```"
                ),
            )
        ]

    # Get command
    command_str = arguments.get("command", "")
    if not command_str:
        return [
            TextContent(
                type="text",
                text="Error: No command provided. Please provide the command from preview_export_command.",
            )
        ]

    # Parse command string into list
    import shlex

    try:
        command = shlex.split(command_str)
    except ValueError as e:
        return [TextContent(type="text", text=f"Error parsing command: {str(e)}")]

    # Execute
    try:
        logger.info("Starting FastBCP execution...")
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=FASTBCP_TIMEOUT, log_dir=FASTBCP_LOG_DIR
        )

        # Format response
        success = return_code == 0

        response = [
            f"# FastBCP Export {'Completed' if success else 'Failed'}",
            "",
            f"**Status**: {'Success' if success else 'Failed'}",
            f"**Return Code**: {return_code}",
            f"**Log Location**: {FASTBCP_LOG_DIR}",
            "",
            "## Output:",
            "```",
            stdout if stdout else "(no output)",
            "```",
        ]

        if stderr:
            response.extend(["", "## Error Output:", "```", stderr, "```"])

        if not success:
            response.extend(
                [
                    "",
                    "## Troubleshooting:",
                    "- Check database credentials and connectivity",
                    "- Verify table/schema names exist",
                    "- Check output path is writable",
                    "- Check FastBCP documentation for error details",
                    "- Review the full log file for more information",
                ]
            )

        return [TextContent(type="text", text="\n".join(response))]

    except FastBCPError as e:
        return [TextContent(type="text", text=f"# Execution Failed\n\nError: {str(e)}")]


async def handle_validate_connection(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle validate_connection tool."""
    try:
        # Validate request
        request = ConnectionValidationRequest(**arguments)

        # Build validation response
        connection = request.connection
        issues = []

        # Check for required fields based on connection type
        if (
            not connection.trusted_auth
            and not connection.connect_string
            and not connection.dsn
        ):
            if not connection.user:
                issues.append(
                    "- Username is required (unless using trusted authentication, connect_string, or dsn)"
                )

        # Check server format (only if server is provided)
        if (
            connection.server
            and ":" not in connection.server
            and "\\" not in connection.server
        ):
            issues.append(
                f"- Server '{connection.server}' may need port (e.g., localhost:5432) or instance name"
            )

        if issues:
            response = [
                f"# Connection Validation - {request.side.upper()}",
                "",
                "**Issues Found:**",
                "",
                *issues,
                "",
                "Note: This is a parameter check only. Actual connectivity is tested during export execution.",
            ]
        else:
            auth_method = "Trusted"
            if connection.connect_string:
                auth_method = "Connection String"
            elif connection.dsn:
                auth_method = "DSN"
            elif connection.trusted_auth:
                auth_method = "Trusted"
            else:
                auth_method = "Username/Password"

            response = [
                f"# Connection Validation - {request.side.upper()}",
                "",
                "**All required parameters present**",
                "",
                f"- Connection Type: {connection.type}",
                f"- Server: {connection.server or '(not specified)'}",
                f"- Database: {connection.database}",
                f"- Authentication: {auth_method}",
                "",
                "Note: This validates parameters only. Actual connectivity will be tested during export.",
            ]

        return [TextContent(type="text", text="\n".join(response))]

    except ValidationError as e:
        error_msg = ["# Validation Error", ""]
        for error in e.errors():
            field = " -> ".join(str(x) for x in error["loc"])
            error_msg.append(f"- **{field}**: {error['msg']}")
        return [TextContent(type="text", text="\n".join(error_msg))]


async def handle_list_formats(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle list_supported_formats tool."""
    formats = get_supported_formats()

    response = [
        "# Supported Formats and Sources",
        "",
        "FastBCP supports exporting from the following database systems to files:",
        "",
    ]

    # Database sources
    response.append("## Database Sources")
    response.append("")
    for source, output_formats in formats["Database Sources"].items():
        response.append(f"### {source}")
        response.append(f"Supported formats: {', '.join(output_formats)}")
        response.append("")

    # Output formats
    response.append("## Output Formats")
    response.append("")
    for fmt in formats["Output Formats"]:
        response.append(f"- `{fmt}`")
    response.append("")

    # Storage targets
    response.append("## Storage Targets")
    response.append("")
    for target in formats["Storage Targets"]:
        response.append(f"- `{target}`")
    response.append("")

    response.extend(
        [
            "## Notes:",
            "- All source databases support all output formats",
            "- Parallelism method availability depends on source database type",
            "- Cloud storage targets require a cloud_profile configuration",
        ]
    )

    return [TextContent(type="text", text="\n".join(response))]


async def handle_suggest_parallelism(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle suggest_parallelism_method tool."""
    try:
        # Validate request
        request = ParallelismSuggestionRequest(**arguments)

        # Get suggestion
        suggestion = suggest_parallelism_method(
            request.source_type,
            request.has_numeric_key,
            request.has_identity_column,
            request.table_size_estimate,
        )

        response = [
            "# Parallelism Method Recommendation",
            "",
            f"**Recommended Method**: `{suggestion['method']}`",
            "",
            "## Explanation:",
            suggestion["explanation"],
            "",
            "## Your Table Characteristics:",
            f"- Source Database: {request.source_type}",
            f"- Has Numeric Key: {'Yes' if request.has_numeric_key else 'No'}",
            f"- Has Identity Column: {'Yes' if request.has_identity_column else 'No'}",
            f"- Table Size: {request.table_size_estimate.capitalize()}",
            "",
            "## Other Considerations:",
            "- **Ctid**: Best for PostgreSQL (no key column needed)",
            "- **Rowid**: Best for Oracle (no key column needed)",
            "- **Physloc**: Best for SQL Server without numeric key",
            "- **RangeId**: Requires numeric key with good distribution",
            "- **Random**: Requires numeric key, uses modulo distribution",
            "- **DataDriven**: Works with any data type, uses distinct values",
            "- **Ntile**: Even distribution, works with numeric/date/string columns",
            "- **None**: Single-threaded, best for small tables or troubleshooting",
        ]

        return [TextContent(type="text", text="\n".join(response))]

    except ValidationError as e:
        error_msg = ["# Validation Error", ""]
        for error in e.errors():
            field = " -> ".join(str(x) for x in error["loc"])
            error_msg.append(f"- **{field}**: {error['msg']}")
        return [TextContent(type="text", text="\n".join(error_msg))]


async def handle_get_version(arguments: Dict[str, Any]) -> list[TextContent]:
    """Handle get_version tool."""
    if command_builder is None:
        return [
            TextContent(
                type="text",
                text=(
                    "Error: FastBCP binary not found or not accessible.\n"
                    f"Expected location: {FASTBCP_PATH}\n"
                    "Please set FASTBCP_PATH environment variable correctly."
                ),
            )
        ]

    version_info = command_builder.get_version()
    caps = version_info["capabilities"]

    response = [
        "# FastBCP Version Information",
        "",
        f"**Version**: {version_info['version'] or 'Unknown'}",
        f"**Detected**: {'Yes' if version_info['detected'] else 'No'}",
        f"**Binary Path**: {version_info['binary_path']}",
        "",
        "## Supported Source Types:",
        ", ".join(f"`{t}`" for t in caps["source_types"]),
        "",
        "## Supported Output Formats:",
        ", ".join(f"`{f}`" for f in caps["output_formats"]),
        "",
        "## Supported Parallelism Methods:",
        ", ".join(f"`{m}`" for m in caps["parallelism_methods"]),
        "",
        "## Supported Storage Targets:",
        ", ".join(f"`{t}`" for t in caps["storage_targets"]),
        "",
        "## Feature Flags:",
        f"- No Banner: {'Yes' if caps['supports_nobanner'] else 'No'}",
        f"- Version Flag: {'Yes' if caps['supports_version_flag'] else 'No'}",
        f"- Cloud Profile: {'Yes' if caps['supports_cloud_profile'] else 'No'}",
        f"- Merge: {'Yes' if caps['supports_merge'] else 'No'}",
    ]

    return [TextContent(type="text", text="\n".join(response))]


def _build_export_explanation(request: ExportRequest) -> str:
    """Build a human-readable explanation of what the export will do."""
    parts = []

    # Source
    if request.source.query:
        server_info = (
            f" ({request.source.server}/{request.source.database})"
            if request.source.server
            else f" ({request.source.database})"
        )
        parts.append(f"Execute query on {request.source.type}{server_info}")
    else:
        source_table = (
            f"{request.source.schema}.{request.source.table}"
            if request.source.schema
            else request.source.table
        )
        parts.append(
            f"Read from {request.source.type} table: {request.source.database}.{source_table}"
        )

    # Output format and destination
    output = request.output
    dest = output.file_output or output.directory or "(not specified)"
    parts.append(f"Export to {output.format.value.upper()} format: {dest}")

    # Storage target
    if output.storage_target.value != "local":
        parts.append(f"Storage target: {output.storage_target.value}")

    # Load mode
    if request.options.load_mode.value == "Truncate":
        parts.append("Mode: TRUNCATE before export")
    else:
        parts.append("Mode: APPEND to existing output")

    # Parallelism
    if request.options.method.value != "None":
        parallel_desc = f"Parallelism: {request.options.method.value} method"
        if request.options.distribute_key_column:
            parallel_desc += f" on column '{request.options.distribute_key_column}'"
        parallel_desc += f" with degree {request.options.degree}"
        parts.append(parallel_desc)
    else:
        parts.append("Parallelism: None (single-threaded export)")

    # Special options
    if output.timestamped:
        parts.append("Timestamped output filename enabled")
    if output.merge:
        parts.append("Merge parallel output files enabled")
    if output.parquet_compression:
        parts.append(f"Parquet compression: {output.parquet_compression.value}")
    if output.no_header:
        parts.append("Header row omitted")

    return "\n".join(f"{i+1}. {part}" for i, part in enumerate(parts))


async def _run():
    """Async server startup logic."""
    logger.info("Starting FastBCP MCP Server...")
    logger.info(f"FastBCP binary: {FASTBCP_PATH}")
    logger.info(f"Execution timeout: {FASTBCP_TIMEOUT}s")
    logger.info(f"Log directory: {FASTBCP_LOG_DIR}")

    from mcp.server.stdio import stdio_server

    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


def main():
    """Entry point for the MCP server (console script)."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
