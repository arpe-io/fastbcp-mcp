# FastBCP MCP Server

<!-- mcp-name: io.github.aetperf/fastbcp-mcp -->

A [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that exposes [FastBCP](https://fastbcp.arpe.io/docs/latest/) functionality for exporting data from databases to files (CSV, TSV, JSON, BSON, Parquet, XLSX, Binary) with optional cloud storage targets.

## Overview

FastBCP is a high-performance CLI tool for exporting data from databases to files. This MCP server wraps FastBCP functionality and provides:

- **Safety-first approach**: Preview commands before execution with user confirmation required
- **Password masking**: Credentials and connection strings are never displayed in logs or output
- **Intelligent validation**: Parameter validation with database-specific compatibility checks
- **Smart suggestions**: Automatic parallelism method recommendations
- **Version detection**: Automatic binary version detection with capability registry
- **Comprehensive logging**: Full execution logs with timestamps and results

## MCP Tools

### 1. `preview_export_command`
Build and preview a FastBCP export command WITHOUT executing it. Shows the exact command with passwords masked. Always use this first.

### 2. `execute_export`
Execute a previously previewed command. Requires `confirmation: true` as a safety mechanism.

### 3. `validate_connection`
Validate source database connection parameters (parameter check only, does not test actual connectivity).

### 4. `list_supported_formats`
List all supported source databases, output formats, and storage targets.

### 5. `suggest_parallelism_method`
Recommend the optimal parallelism method based on source database type and table characteristics.

### 6. `get_version`
Report the detected FastBCP binary version, supported types, and feature flags.

## Installation

### Prerequisites

- Python 3.10 or higher
- FastBCP binary v0.29+ (obtain from [Arpe.io](https://arpe.io))
- Claude Code or another MCP client

### Setup

1. **Clone or download this repository**:
   ```bash
   cd /path/to/fastbcp-mcp
   ```

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your FastBCP path
   ```

4. **Add to Claude Code configuration** (`~/.claude.json`):
   ```json
   {
     "mcpServers": {
       "fastbcp": {
         "type": "stdio",
         "command": "python",
         "args": ["/absolute/path/to/fastbcp-mcp/src/server.py"],
         "env": {
           "FASTBCP_PATH": "/absolute/path/to/FastBCP"
         }
       }
     }
   }
   ```

5. **Restart Claude Code** to load the MCP server.

6. **Verify installation**:
   ```
   # In Claude Code, run:
   /mcp
   # You should see "fastbcp: connected"
   ```

## Configuration

### Environment Variables

Edit `.env` to configure:

```bash
# Path to FastBCP binary (required)
FASTBCP_PATH=./fastbcp/FastBCP

# Execution timeout in seconds (default: 1800 = 30 minutes)
FASTBCP_TIMEOUT=1800

# Log directory (default: ./logs)
FASTBCP_LOG_DIR=./logs

# Log level (default: INFO)
LOG_LEVEL=INFO
```

## Connection Options

The server supports multiple ways to authenticate and connect:

| Parameter | Description |
|-----------|-------------|
| `server` | Host:port or host\instance (optional with `connect_string` or `dsn`) |
| `user` / `password` | Standard credentials |
| `trusted_auth` | Windows trusted authentication |
| `connect_string` | Full connection string (excludes server/user/password/dsn) |
| `dsn` | ODBC DSN name (excludes server/provider) |
| `provider` | OleDB provider name |
| `application_intent` | SQL Server application intent (ReadOnly/ReadWrite) |

## Output Options

| Option | CLI Flag | Description |
|--------|----------|-------------|
| `format` | `--format` | Output format: csv, tsv, json, bson, parquet, xlsx, binary |
| `file_output` | `--fileoutput` | Output file path |
| `directory` | `--directory` | Output directory path |
| `storage_target` | `--storagetarget` | Storage: local, s3, s3compatible, azure_blob, azure_datalake, fabric_onelake |
| `delimiter` | `--delimiter` | Field delimiter (CSV/TSV) |
| `quotes` | `--quotes` | Quote character |
| `encoding` | `--encoding` | Output encoding |
| `no_header` | `--noheader` | Omit header row (CSV/TSV) |
| `decimal_separator` | `--decimalseparator` | Decimal separator (. or ,) |
| `date_format` | `--dateformat` | Date format string |
| `bool_format` | `--boolformat` | Boolean format: TrueFalse, OneZero, YesNo |
| `parquet_compression` | `--parquetcompression` | Parquet compression: None, Snappy, Gzip, Lz4, Lzo, Zstd |
| `timestamped` | `--timestamped` | Add timestamp to output filename |
| `merge` | `--merge` | Merge parallel output files |

## Export Options

| Option | CLI Flag | Description |
|--------|----------|-------------|
| `method` | `--method` | Parallelism method |
| `distribute_key_column` | `--distributeKeyColumn` | Column for data distribution |
| `degree` | `--degree` | Parallelism degree (default: 1) |
| `load_mode` | `--loadmode` | Append or Truncate |
| `batch_size` | `--batchsize` | Batch size for export operations |
| `map_method` | `--mapmethod` | Column mapping: Position or Name |
| `run_id` | `--runid` | Run ID for logging |
| `data_driven_query` | `--datadrivenquery` | Custom SQL for DataDriven method |
| `settings_file` | `--settingsfile` | Custom settings JSON file |
| `log_level` | `--loglevel` | Override log level (Information/Debug) |
| `no_banner` | `--nobanner` | Suppress banner output |
| `license_path` | `--license` | License file path or URL |
| `cloud_profile` | `--cloudprofile` | Cloud storage profile name |

## Usage Examples

### PostgreSQL to CSV Export

```
User: "Export the 'orders' table from PostgreSQL (localhost:5432, database: sales_db,
       schema: public) to CSV file at /tmp/orders.csv. Use parallel export."

Claude Code will:
1. Call suggest_parallelism_method to recommend Ctid for PostgreSQL
2. Call preview_export_command with your parameters
3. Show the command with masked passwords
4. Explain what will happen
5. Ask for confirmation
6. Execute with execute_export when you approve
```

### Export to Parquet with Compression

```
User: "Export the 'transactions' table from SQL Server to Parquet format
       with Snappy compression, saved to /data/exports/."

Claude Code will use parquet format with parquet_compression set to Snappy.
```

### Export to S3

```
User: "Export the 'users' table from PostgreSQL to CSV on S3 bucket
       s3://my-bucket/exports/ using my AWS profile."

Claude Code will use storage_target=s3 with cloud_profile.
```

### Check Version and Capabilities

```
User: "What version of FastBCP is installed?"

Claude Code will call get_version and display the detected version,
supported source types, output formats, and available features.
```

## Two-Step Safety Process

This server implements a mandatory two-step process:

1. **Preview** - Always use `preview_export_command` first
2. **Execute** - Use `execute_export` with `confirmation: true`

You cannot execute without previewing first and confirming.

## Security

- Passwords and connection strings are masked in all output and logs
- Sensitive flags masked: `--sourcepassword`, `--sourceconnectstring`, `-x`, `-g`
- Use environment variables for sensitive configuration
- Review commands carefully before executing
- Use minimum required database permissions

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## Project Structure

```
fastbcp-mcp/
  src/
    __init__.py
    server.py          # MCP server (tool definitions, handlers)
    fastbcp.py         # Command builder, executor, suggestions
    validators.py      # Pydantic models, enums, validation
    version.py         # Version detection and capabilities registry
  tests/
    __init__.py
    test_command_builder.py
    test_validators.py
    test_version.py
  .env.example
  requirements.txt
  CHANGELOG.md
  README.md
```

## License

This MCP server wrapper is provided as-is. FastBCP itself is a separate product from Arpe.io.

## Related Links

- [FastBCP Documentation](https://fastbcp.arpe.io/docs/latest/)
- [Model Context Protocol](https://modelcontextprotocol.io/)
