"""
Input validation for FastBCP MCP Server.

This module provides Pydantic models and enums for validating
all FastBCP parameters and ensuring parameter compatibility.
"""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator


class SourceConnectionType(str, Enum):
    """Source database connection types supported by FastBCP."""

    CLICKHOUSE = "clickhouse"
    HANA = "hana"
    MSSQL = "mssql"
    MSOLEDBSQL = "msoledbsql"
    MYSQL = "mysql"
    NETEZZA_COPY = "nzcopy"
    NETEZZA_OLEDB = "nzoledb"
    NETEZZA_SQL = "nzsql"
    ODBC = "odbc"
    OLEDB = "oledb"
    ORACLE_ODP = "oraodp"
    POSTGRES_COPY = "pgcopy"
    POSTGRES = "pgsql"
    TERADATA = "teradata"


class OutputFormat(str, Enum):
    """Output file formats supported by FastBCP."""

    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    BSON = "bson"
    PARQUET = "parquet"
    XLSX = "xlsx"
    BINARY = "binary"


class ParallelismMethod(str, Enum):
    """Parallelism methods for data distribution."""

    CTID = "Ctid"  # PostgreSQL-specific
    DATA_DRIVEN = "DataDriven"  # Distribute by distinct key values
    NTILE = "Ntile"  # Even distribution
    NZ_DATA_SLICE = "NZDataSlice"  # Netezza-specific
    NONE = "None"  # No parallelism
    PHYSLOC = "Physloc"  # Physical location
    RANDOM = "Random"  # Random distribution using modulo
    RANGE_ID = "RangeId"  # Numeric range distribution
    ROWID = "Rowid"  # Oracle-specific


class StorageTarget(str, Enum):
    """Storage targets for output files."""

    LOCAL = "local"
    S3 = "s3"
    S3_COMPATIBLE = "s3compatible"
    AZURE_BLOB = "azure_blob"
    AZURE_DATALAKE = "azure_datalake"
    FABRIC_ONELAKE = "fabric_onelake"


class ParquetCompression(str, Enum):
    """Parquet compression algorithms."""

    NONE = "None"
    SNAPPY = "Snappy"
    GZIP = "Gzip"
    LZ4 = "Lz4"
    LZO = "Lzo"
    ZSTD = "Zstd"


class LogLevel(str, Enum):
    """Log level for FastBCP output."""

    INFORMATION = "Information"
    DEBUG = "Debug"


class LoadMode(str, Enum):
    """Load mode for export."""

    APPEND = "Append"
    TRUNCATE = "Truncate"


class MapMethod(str, Enum):
    """Column mapping method."""

    POSITION = "Position"  # Map by position
    NAME = "Name"  # Map by name (case-insensitive)


class DecimalSeparator(str, Enum):
    """Decimal separator for numeric output."""

    DOT = "."
    COMMA = ","


class ApplicationIntent(str, Enum):
    """SQL Server application intent."""

    READ_ONLY = "ReadOnly"
    READ_WRITE = "ReadWrite"


class BoolFormat(str, Enum):
    """Boolean output format."""

    TRUE_FALSE = "TrueFalse"
    ONE_ZERO = "OneZero"
    YES_NO = "YesNo"


class SourceConnectionConfig(BaseModel):
    """Source database connection configuration."""

    type: str = Field(..., description="Source connection type")
    server: Optional[str] = Field(
        None, description="Server address (host:port or host\\instance)"
    )
    database: str = Field(..., description="Database name")
    schema: Optional[str] = Field(None, description="Schema name")
    table: Optional[str] = Field(
        None, description="Table name (optional if query provided)"
    )
    query: Optional[str] = Field(None, description="SQL query (alternative to table)")
    user: Optional[str] = Field(None, description="Username for authentication")
    password: Optional[str] = Field(None, description="Password for authentication")
    trusted_auth: bool = Field(
        False, description="Use trusted authentication (Windows)"
    )
    connect_string: Optional[str] = Field(
        None, description="Full connection string (alternative)"
    )
    dsn: Optional[str] = Field(None, description="ODBC DSN name")
    provider: Optional[str] = Field(None, description="OleDB provider name")
    application_intent: Optional[ApplicationIntent] = Field(
        None, description="SQL Server application intent"
    )

    @model_validator(mode="after")
    def validate_mutual_exclusivity(self):
        """Validate mutually exclusive connection parameters."""
        # connect_string excludes individual connection params
        if self.connect_string:
            conflicts = []
            if self.dsn:
                conflicts.append("dsn")
            if self.provider:
                conflicts.append("provider")
            if self.server:
                conflicts.append("server")
            if self.user:
                conflicts.append("user")
            if self.password:
                conflicts.append("password")
            if self.trusted_auth:
                conflicts.append("trusted_auth")
            if conflicts:
                raise ValueError(
                    f"connect_string cannot be used with: {', '.join(conflicts)}"
                )

        # dsn excludes provider and server
        if self.dsn:
            conflicts = []
            if self.provider:
                conflicts.append("provider")
            if self.server:
                conflicts.append("server")
            if conflicts:
                raise ValueError(f"dsn cannot be used with: {', '.join(conflicts)}")

        # trusted_auth excludes user and password
        if self.trusted_auth:
            conflicts = []
            if self.user:
                conflicts.append("user")
            if self.password:
                conflicts.append("password")
            if conflicts:
                raise ValueError(
                    f"trusted_auth cannot be used with: {', '.join(conflicts)}"
                )

        return self

    @model_validator(mode="after")
    def validate_authentication(self):
        """Ensure either credentials or trusted auth is provided."""
        if not self.trusted_auth and not self.connect_string and not self.dsn:
            if not self.user:
                raise ValueError(
                    "Either user/password, trusted_auth, connect_string, or dsn must be provided"
                )
        return self


class OutputConfig(BaseModel):
    """Output file configuration."""

    format: OutputFormat = Field(..., description="Output file format")
    file_output: Optional[str] = Field(None, description="Output file path")
    directory: Optional[str] = Field(None, description="Output directory path")
    storage_target: StorageTarget = Field(
        StorageTarget.LOCAL, description="Storage target for output"
    )
    delimiter: Optional[str] = Field(None, description="Field delimiter (CSV/TSV)")
    quotes: Optional[str] = Field(None, description="Quote character")
    encoding: Optional[str] = Field(None, description="Output file encoding")
    no_header: bool = Field(False, description="Omit header row (CSV/TSV)")
    decimal_separator: Optional[DecimalSeparator] = Field(
        None, description="Decimal separator for numeric values"
    )
    date_format: Optional[str] = Field(None, description="Date format string")
    bool_format: Optional[BoolFormat] = Field(None, description="Boolean output format")
    parquet_compression: Optional[ParquetCompression] = Field(
        None, description="Parquet compression algorithm"
    )
    timestamped: bool = Field(False, description="Add timestamp to output filename")
    merge: bool = Field(False, description="Merge parallel output files")

    @model_validator(mode="after")
    def validate_output_destination(self):
        """Ensure at least file_output or directory is provided."""
        if not self.file_output and not self.directory:
            raise ValueError(
                "At least one of 'file_output' or 'directory' must be provided"
            )
        return self

    @model_validator(mode="after")
    def validate_parquet_compression(self):
        """Validate parquet_compression is only used with parquet format."""
        if self.parquet_compression and self.format != OutputFormat.PARQUET:
            raise ValueError("parquet_compression can only be used with parquet format")
        return self

    @model_validator(mode="after")
    def validate_delimiter_format(self):
        """Validate delimiter is only meaningful for CSV/TSV."""
        if self.delimiter and self.format not in (OutputFormat.CSV, OutputFormat.TSV):
            raise ValueError("delimiter is only meaningful for csv or tsv formats")
        return self

    @model_validator(mode="after")
    def validate_no_header_format(self):
        """Validate no_header is only meaningful for CSV/TSV."""
        if self.no_header and self.format not in (OutputFormat.CSV, OutputFormat.TSV):
            raise ValueError("no_header is only meaningful for csv or tsv formats")
        return self


class ExportOptions(BaseModel):
    """Options for data export execution."""

    method: ParallelismMethod = Field(
        ParallelismMethod.NONE, description="Parallelism method"
    )
    distribute_key_column: Optional[str] = Field(
        None, description="Column for data distribution (required for some methods)"
    )
    degree: int = Field(
        1,
        description="Parallelism degree: number of parallel workers",
    )
    load_mode: LoadMode = Field(
        LoadMode.APPEND, description="Load mode: Append or Truncate"
    )
    batch_size: Optional[int] = Field(
        None, ge=1, description="Batch size for export operations"
    )
    map_method: MapMethod = Field(
        MapMethod.POSITION, description="Column mapping method: Position or Name"
    )
    run_id: Optional[str] = Field(
        None, description="Run ID for logging and tracking purposes"
    )
    data_driven_query: Optional[str] = Field(
        None, description="Custom SQL query for DataDriven parallelism method"
    )
    settings_file: Optional[str] = Field(
        None, description="Path to custom settings JSON file"
    )
    log_level: Optional[LogLevel] = Field(None, description="Override log level")
    no_banner: bool = Field(False, description="Suppress the FastBCP banner")
    license_path: Optional[str] = Field(None, description="Path or URL to license file")
    cloud_profile: Optional[str] = Field(None, description="Cloud storage profile name")

    @model_validator(mode="after")
    def validate_distribute_key_requirements(self):
        """Validate distribute key column requirements."""
        methods_requiring_key = {
            ParallelismMethod.DATA_DRIVEN,
            ParallelismMethod.RANDOM,
            ParallelismMethod.RANGE_ID,
            ParallelismMethod.NTILE,
        }

        if self.method in methods_requiring_key and not self.distribute_key_column:
            raise ValueError(
                f"Method '{self.method.value}' requires distribute_key_column"
            )

        return self

    @model_validator(mode="after")
    def validate_data_driven_query(self):
        """Validate data_driven_query is only used with DataDriven method."""
        if self.data_driven_query and self.method != ParallelismMethod.DATA_DRIVEN:
            raise ValueError(
                "data_driven_query can only be used with the DataDriven method"
            )
        return self


class ExportRequest(BaseModel):
    """Complete export request with source, output, and options."""

    source: SourceConnectionConfig = Field(
        ..., description="Source database configuration"
    )
    output: OutputConfig = Field(..., description="Output file configuration")
    options: ExportOptions = Field(
        default_factory=ExportOptions, description="Export execution options"
    )

    @model_validator(mode="after")
    def validate_source_table_or_query(self):
        """Ensure source has exactly one of table or query."""
        has_table = bool(self.source.table)
        has_query = bool(self.source.query)
        count = sum([has_table, has_query])

        if count == 0:
            raise ValueError("Source must specify either 'table' or 'query'")
        if count > 1:
            raise ValueError("Source must specify only one of 'table' or 'query'")
        return self

    @model_validator(mode="after")
    def validate_method_compatibility(self):
        """Validate parallelism method compatibility with source database."""
        method = self.options.method
        source_type = self.source.type.lower()

        # Ctid is PostgreSQL-specific
        if method == ParallelismMethod.CTID and source_type not in [
            "pgsql",
            "pgcopy",
        ]:
            raise ValueError(
                f"Method 'Ctid' only works with PostgreSQL sources, not '{source_type}'"
            )

        # Rowid is Oracle-specific
        if method == ParallelismMethod.ROWID and source_type not in [
            "oraodp",
        ]:
            raise ValueError(
                f"Method 'Rowid' only works with Oracle sources, not '{source_type}'"
            )

        # NZDataSlice is Netezza-specific
        if method == ParallelismMethod.NZ_DATA_SLICE and source_type not in [
            "nzcopy",
            "nzoledb",
            "nzsql",
        ]:
            raise ValueError(
                f"Method 'NZDataSlice' only works with Netezza sources, not '{source_type}'"
            )

        # Physloc is SQL Server-specific
        if method == ParallelismMethod.PHYSLOC and source_type not in [
            "mssql",
            "oledb",
            "odbc",
            "msoledbsql",
        ]:
            raise ValueError(
                f"Method 'Physloc' only works with SQL Server sources (mssql, oledb, odbc, msoledbsql), not '{source_type}'"
            )

        return self


class ConnectionValidationRequest(BaseModel):
    """Request to validate a database connection."""

    connection: SourceConnectionConfig = Field(
        ..., description="Connection to validate"
    )
    side: str = Field(..., description="Connection side: 'source' or 'target'")

    @field_validator("side")
    @classmethod
    def validate_side(cls, v):
        """Ensure side is either source or target."""
        if v not in ["source", "target"]:
            raise ValueError("Side must be 'source' or 'target'")
        return v


class ParallelismSuggestionRequest(BaseModel):
    """Request for parallelism method suggestion."""

    source_type: str = Field(..., description="Source database type")
    has_numeric_key: bool = Field(
        ..., description="Whether table has a numeric key column"
    )
    has_identity_column: bool = Field(
        False, description="Whether table has an identity/auto-increment column"
    )
    table_size_estimate: str = Field(
        ..., description="Table size: 'small', 'medium', or 'large'"
    )

    @field_validator("table_size_estimate")
    @classmethod
    def validate_table_size(cls, v):
        """Validate table size estimate."""
        if v not in ["small", "medium", "large"]:
            raise ValueError(
                "Table size estimate must be 'small', 'medium', or 'large'"
            )
        return v
