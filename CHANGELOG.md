# Changelog

All notable changes to the FastBCP MCP Server will be documented in this file.

## [0.1.3] - 2026-02-24

### Added
- PyPI, License, and MCP Registry badges in README
- GitHub Actions workflow for automated PyPI publishing on release
- Missing environment variables (`FASTBCP_LOG_DIR`, `LOG_LEVEL`) in server.json
- GitHub repository topics for MCP Registry discoverability

### Fixed
- Documentation URL in pyproject.toml

## [0.1.1] - 2026-02-23

### Added

- `server.json` MCP Registry configuration file with package metadata, transport settings, and environment variable definitions

### Changed

- GitHub repository URL updated from `aetperf/fastbcp-mcp` to `arpe-io/fastbcp-mcp` in `pyproject.toml`

## [0.1.0] - 2026-02-20

### Added
- Initial release of FastBCP MCP Server
- 6 MCP tools: preview_export_command, execute_export, validate_connection, list_supported_formats, suggest_parallelism_method, get_version
- Support for 14 source database types: clickhouse, hana, mssql, msoledbsql, mysql, nzcopy, nzoledb, nzsql, odbc, oledb, oraodp, pgcopy, pgsql, teradata
- Support for 7 output formats: csv, tsv, json, bson, parquet, xlsx, binary
- Support for 9 parallelism methods: Ctid, DataDriven, Ntile, NZDataSlice, None, Physloc, Random, RangeId, Rowid
- Support for 6 storage targets: local, s3, s3compatible, azure_blob, azure_datalake, fabric_onelake
- Version detection for FastBCP v0.29.1.0
- Password and connection string masking in all output
- Two-step safety process (preview then execute with confirmation)
- Comprehensive input validation with Pydantic models
- Parallelism method compatibility checks (Ctid/PostgreSQL, Rowid/Oracle, NZDataSlice/Netezza, Physloc/SQL Server)
- Smart parallelism method suggestions based on source type and table characteristics
- Output format validation (parquet_compression only with parquet, delimiter/no_header only with CSV/TSV)
- Cloud storage support with cloud_profile parameter
- Execution logging with timestamps and masked credentials
