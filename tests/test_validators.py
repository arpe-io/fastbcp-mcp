"""Tests for validators module."""

import pytest
from pydantic import ValidationError

from src.validators import (
    SourceConnectionType,
    OutputFormat,
    ParallelismMethod,
    StorageTarget,
    ParquetCompression,
    LogLevel,
    LoadMode,
    MapMethod,
    DecimalSeparator,
    ApplicationIntent,
    BoolFormat,
    SourceConnectionConfig,
    OutputConfig,
    ExportOptions,
    ExportRequest,
    ConnectionValidationRequest,
    ParallelismSuggestionRequest,
)


class TestSourceConnectionType:
    """Tests for SourceConnectionType enum."""

    def test_all_14_source_types(self):
        """Test that there are exactly 14 source types."""
        assert len(SourceConnectionType) == 14

    def test_oraodp_exists(self):
        """Test that oraodp source type exists."""
        assert SourceConnectionType("oraodp") == SourceConnectionType.ORACLE_ODP

    def test_msoledbsql_exists(self):
        """Test that msoledbsql source type exists."""
        assert SourceConnectionType("msoledbsql") == SourceConnectionType.MSOLEDBSQL

    def test_nzcopy_exists(self):
        """Test that nzcopy source type exists."""
        assert SourceConnectionType("nzcopy") == SourceConnectionType.NETEZZA_COPY

    def test_nzoledb_exists(self):
        """Test that nzoledb source type exists."""
        assert SourceConnectionType("nzoledb") == SourceConnectionType.NETEZZA_OLEDB

    def test_nzsql_exists(self):
        """Test that nzsql source type exists."""
        assert SourceConnectionType("nzsql") == SourceConnectionType.NETEZZA_SQL

    def test_pgsql_exists(self):
        """Test that pgsql source type exists."""
        assert SourceConnectionType("pgsql") == SourceConnectionType.POSTGRES

    def test_invalid_source_type(self):
        """Test that invalid source type raises ValueError."""
        with pytest.raises(ValueError):
            SourceConnectionType("invalid")


class TestOutputFormat:
    """Tests for OutputFormat enum."""

    def test_all_7_output_formats(self):
        """Test that there are exactly 7 output formats."""
        assert len(OutputFormat) == 7

    def test_csv_format(self):
        """Test CSV format exists."""
        assert OutputFormat("csv") == OutputFormat.CSV

    def test_parquet_format(self):
        """Test Parquet format exists."""
        assert OutputFormat("parquet") == OutputFormat.PARQUET

    def test_binary_format(self):
        """Test Binary format exists."""
        assert OutputFormat("binary") == OutputFormat.BINARY


class TestOtherEnums:
    """Tests for other enum types."""

    def test_all_parallelism_methods(self):
        """Test all 10 parallelism method values exist."""
        assert len(ParallelismMethod) == 10
        assert ParallelismMethod("Ctid") == ParallelismMethod.CTID
        assert ParallelismMethod("None") == ParallelismMethod.NONE
        assert ParallelismMethod("Rowid") == ParallelismMethod.ROWID

    def test_all_storage_targets(self):
        """Test all 7 storage target values exist."""
        assert len(StorageTarget) == 7
        assert StorageTarget("local") == StorageTarget.LOCAL
        assert StorageTarget("s3") == StorageTarget.S3
        assert StorageTarget("azure_blob") == StorageTarget.AZURE_BLOB

    def test_all_parquet_compressions(self):
        """Test all 6 parquet compression values exist."""
        assert len(ParquetCompression) == 6
        assert ParquetCompression("Snappy") == ParquetCompression.SNAPPY
        assert ParquetCompression("Zstd") == ParquetCompression.ZSTD

    def test_log_levels(self):
        """Test all 2 log level values exist."""
        assert len(LogLevel) == 2
        assert LogLevel("Information") == LogLevel.INFORMATION
        assert LogLevel("Debug") == LogLevel.DEBUG

    def test_decimal_separator(self):
        """Test decimal separator values."""
        assert DecimalSeparator(".") == DecimalSeparator.DOT
        assert DecimalSeparator(",") == DecimalSeparator.COMMA

    def test_application_intent(self):
        """Test application intent values."""
        assert ApplicationIntent("ReadOnly") == ApplicationIntent.READ_ONLY
        assert ApplicationIntent("ReadWrite") == ApplicationIntent.READ_WRITE

    def test_bool_format(self):
        """Test bool format values."""
        assert BoolFormat("TrueFalse") == BoolFormat.TRUE_FALSE
        assert BoolFormat("OneZero") == BoolFormat.ONE_ZERO
        assert BoolFormat("YesNo") == BoolFormat.YES_NO


class TestSourceConnectionConfig:
    """Tests for SourceConnectionConfig model."""

    def test_valid_connection_with_credentials(self):
        """Test valid connection with username and password."""
        config = SourceConnectionConfig(
            type="pgsql",
            server="localhost:5432",
            database="testdb",
            user="testuser",
            password="testpass",
        )
        assert config.server == "localhost:5432"
        assert config.user == "testuser"

    def test_valid_connection_with_trusted_auth(self):
        """Test valid connection with trusted authentication."""
        config = SourceConnectionConfig(
            type="mssql", server="localhost", database="testdb", trusted_auth=True
        )
        assert config.trusted_auth is True

    def test_valid_connection_with_connect_string(self):
        """Test valid connection with connection string."""
        config = SourceConnectionConfig(
            type="odbc",
            database="testdb",
            connect_string="DSN=mydsn;UID=user;PWD=pass",
        )
        assert config.connect_string is not None

    def test_valid_connection_with_dsn(self):
        """Test valid connection with DSN."""
        config = SourceConnectionConfig(
            type="odbc",
            database="testdb",
            dsn="mydsn",
        )
        assert config.dsn == "mydsn"

    def test_invalid_connection_no_auth(self):
        """Test that connection without authentication fails."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="pgsql", server="localhost:5432", database="testdb"
            )
        errors = exc_info.value.errors()
        assert any(
            "user" in str(e) or "authentication" in str(e).lower() for e in errors
        )

    def test_connect_string_excludes_server(self):
        """Test that connect_string cannot be used with server."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="odbc",
                server="localhost",
                database="testdb",
                connect_string="DSN=mydsn;UID=user;PWD=pass",
            )
        errors = exc_info.value.errors()
        assert any("connect_string" in str(e) for e in errors)

    def test_connect_string_excludes_user(self):
        """Test that connect_string cannot be used with user."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="odbc",
                database="testdb",
                connect_string="DSN=mydsn;UID=user;PWD=pass",
                user="extra_user",
            )
        errors = exc_info.value.errors()
        assert any("connect_string" in str(e) for e in errors)

    def test_dsn_excludes_provider(self):
        """Test that dsn cannot be used with provider."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="oledb",
                database="testdb",
                dsn="mydsn",
                provider="SQLOLEDB",
            )
        errors = exc_info.value.errors()
        assert any("dsn" in str(e) for e in errors)

    def test_dsn_excludes_server(self):
        """Test that dsn cannot be used with server."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="odbc",
                server="localhost",
                database="testdb",
                dsn="mydsn",
            )
        errors = exc_info.value.errors()
        assert any("dsn" in str(e) for e in errors)

    def test_trusted_auth_excludes_user(self):
        """Test that trusted_auth cannot be used with user."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="mssql",
                server="localhost",
                database="testdb",
                trusted_auth=True,
                user="someuser",
            )
        errors = exc_info.value.errors()
        assert any("trusted_auth" in str(e) for e in errors)

    def test_trusted_auth_excludes_password(self):
        """Test that trusted_auth cannot be used with password."""
        with pytest.raises(ValidationError) as exc_info:
            SourceConnectionConfig(
                type="mssql",
                server="localhost",
                database="testdb",
                trusted_auth=True,
                password="somepass",
            )
        errors = exc_info.value.errors()
        assert any("trusted_auth" in str(e) for e in errors)

    def test_connection_with_schema_and_table(self):
        """Test connection with optional schema and table."""
        config = SourceConnectionConfig(
            type="pgsql",
            server="localhost:5432",
            database="testdb",
            schema="public",
            table="users",
            user="testuser",
            password="testpass",
        )
        assert config.schema == "public"
        assert config.table == "users"

    def test_connection_with_application_intent(self):
        """Test connection with application intent."""
        config = SourceConnectionConfig(
            type="mssql",
            server="localhost",
            database="testdb",
            user="user",
            password="pass",
            application_intent=ApplicationIntent.READ_ONLY,
        )
        assert config.application_intent == ApplicationIntent.READ_ONLY


class TestOutputConfig:
    """Tests for OutputConfig model."""

    def test_valid_output_with_file(self):
        """Test valid output config with file_output."""
        config = OutputConfig(
            format=OutputFormat.CSV,
            file_output="/tmp/output.csv",
        )
        assert config.file_output == "/tmp/output.csv"

    def test_valid_output_with_directory(self):
        """Test valid output config with directory."""
        config = OutputConfig(
            format=OutputFormat.PARQUET,
            directory="/tmp/output/",
        )
        assert config.directory == "/tmp/output/"

    def test_output_requires_destination(self):
        """Test that output must have file_output or directory."""
        with pytest.raises(ValidationError) as exc_info:
            OutputConfig(format=OutputFormat.CSV)
        errors = exc_info.value.errors()
        assert any("file_output" in str(e) or "directory" in str(e) for e in errors)

    def test_parquet_compression_only_with_parquet(self):
        """Test parquet_compression only valid with parquet format."""
        with pytest.raises(ValidationError) as exc_info:
            OutputConfig(
                format=OutputFormat.CSV,
                file_output="/tmp/output.csv",
                parquet_compression=ParquetCompression.SNAPPY,
            )
        errors = exc_info.value.errors()
        assert any("parquet_compression" in str(e) for e in errors)

    def test_parquet_compression_valid_with_parquet(self):
        """Test parquet_compression works with parquet format."""
        config = OutputConfig(
            format=OutputFormat.PARQUET,
            file_output="/tmp/output.parquet",
            parquet_compression=ParquetCompression.SNAPPY,
        )
        assert config.parquet_compression == ParquetCompression.SNAPPY

    def test_delimiter_only_with_csv_tsv(self):
        """Test delimiter only valid with CSV/TSV formats."""
        with pytest.raises(ValidationError) as exc_info:
            OutputConfig(
                format=OutputFormat.JSON,
                file_output="/tmp/output.json",
                delimiter="|",
            )
        errors = exc_info.value.errors()
        assert any("delimiter" in str(e) for e in errors)

    def test_no_header_only_with_csv_tsv(self):
        """Test no_header only valid with CSV/TSV formats."""
        with pytest.raises(ValidationError) as exc_info:
            OutputConfig(
                format=OutputFormat.PARQUET,
                file_output="/tmp/output.parquet",
                no_header=True,
            )
        errors = exc_info.value.errors()
        assert any("no_header" in str(e) for e in errors)

    def test_no_header_valid_with_csv(self):
        """Test no_header works with CSV format."""
        config = OutputConfig(
            format=OutputFormat.CSV,
            file_output="/tmp/output.csv",
            no_header=True,
        )
        assert config.no_header is True

    def test_storage_target_default_local(self):
        """Test storage target defaults to local."""
        config = OutputConfig(
            format=OutputFormat.CSV,
            file_output="/tmp/output.csv",
        )
        assert config.storage_target == StorageTarget.LOCAL


class TestExportOptions:
    """Tests for ExportOptions model."""

    def test_default_options(self):
        """Test default export options."""
        options = ExportOptions()
        assert options.method == ParallelismMethod.NONE
        assert options.degree == 1
        assert options.load_mode == LoadMode.APPEND
        assert options.map_method == MapMethod.POSITION

    def test_data_driven_requires_distribute_key(self):
        """Test that DataDriven method requires distribute_key_column."""
        with pytest.raises(ValidationError) as exc_info:
            ExportOptions(method=ParallelismMethod.DATA_DRIVEN)
        errors = exc_info.value.errors()
        assert any("distribute_key_column" in str(e) for e in errors)

    def test_data_driven_with_distribute_key_valid(self):
        """Test that DataDriven with distribute_key_column is valid."""
        options = ExportOptions(
            method=ParallelismMethod.DATA_DRIVEN, distribute_key_column="id"
        )
        assert options.distribute_key_column == "id"

    def test_range_id_requires_distribute_key(self):
        """Test that RangeId method requires distribute_key_column."""
        with pytest.raises(ValidationError):
            ExportOptions(method=ParallelismMethod.RANGE_ID)

    def test_random_requires_distribute_key(self):
        """Test that Random method requires distribute_key_column."""
        with pytest.raises(ValidationError):
            ExportOptions(method=ParallelismMethod.RANDOM)

    def test_ntile_requires_distribute_key(self):
        """Test that Ntile method requires distribute_key_column."""
        with pytest.raises(ValidationError):
            ExportOptions(method=ParallelismMethod.NTILE)

    def test_ctid_no_distribute_key_needed(self):
        """Test that Ctid doesn't require distribute_key_column."""
        options = ExportOptions(method=ParallelismMethod.CTID)
        assert options.method == ParallelismMethod.CTID
        assert options.distribute_key_column is None

    def test_batch_size_validation(self):
        """Test batch size must be positive."""
        with pytest.raises(ValidationError):
            ExportOptions(batch_size=0)

        options = ExportOptions(batch_size=10000)
        assert options.batch_size == 10000

    def test_data_driven_query_only_with_datadriven(self):
        """Test data_driven_query requires DataDriven method."""
        with pytest.raises(ValidationError) as exc_info:
            ExportOptions(
                method=ParallelismMethod.NONE,
                data_driven_query="SELECT DISTINCT region FROM t",
            )
        errors = exc_info.value.errors()
        assert any("data_driven_query" in str(e) for e in errors)

    def test_data_driven_query_valid_with_datadriven(self):
        """Test data_driven_query accepted with DataDriven method."""
        options = ExportOptions(
            method=ParallelismMethod.DATA_DRIVEN,
            distribute_key_column="region",
            data_driven_query="SELECT DISTINCT region FROM t",
        )
        assert options.data_driven_query == "SELECT DISTINCT region FROM t"

    def test_default_degree_is_1(self):
        """Test that default degree is 1."""
        options = ExportOptions()
        assert options.degree == 1

    def test_settings_file_accepted(self):
        """Test settings_file field is accepted."""
        options = ExportOptions(settings_file="/path/to/settings.json")
        assert options.settings_file == "/path/to/settings.json"

    def test_no_banner_default_false(self):
        """Test no_banner defaults to False."""
        options = ExportOptions()
        assert options.no_banner is False

    def test_license_path_accepted(self):
        """Test license_path field is accepted."""
        options = ExportOptions(license_path="/path/to/license.lic")
        assert options.license_path == "/path/to/license.lic"

    def test_cloud_profile_accepted(self):
        """Test cloud_profile field is accepted."""
        options = ExportOptions(cloud_profile="my-aws-profile")
        assert options.cloud_profile == "my-aws-profile"

    def test_log_level_accepted(self):
        """Test log_level field is accepted."""
        options = ExportOptions(log_level=LogLevel.DEBUG)
        assert options.log_level == LogLevel.DEBUG


class TestExportRequest:
    """Tests for ExportRequest model."""

    def test_valid_export_request(self):
        """Test a valid complete export request."""
        request = ExportRequest(
            source={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "sourcedb",
                "schema": "public",
                "table": "users",
                "user": "sourceuser",
                "password": "sourcepass",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/users.csv",
            },
            options={"method": "Ctid", "degree": 4},
        )
        assert request.source.type == "pgsql"
        assert request.output.format == OutputFormat.CSV
        assert request.options.method == ParallelismMethod.CTID

    def test_source_requires_table_or_query(self):
        """Test that source must have either table or query."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(
                source={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "sourcedb",
                    "user": "user",
                    "password": "pass",
                },
                output={
                    "format": "csv",
                    "file_output": "/tmp/output.csv",
                },
            )
        errors = exc_info.value.errors()
        assert any("table" in str(e) or "query" in str(e) for e in errors)

    def test_source_cannot_have_both_table_and_query(self):
        """Test that source cannot have both table and query."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(
                source={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "sourcedb",
                    "table": "users",
                    "query": "SELECT * FROM users",
                    "user": "user",
                    "password": "pass",
                },
                output={
                    "format": "csv",
                    "file_output": "/tmp/output.csv",
                },
            )
        errors = exc_info.value.errors()
        assert any("only one" in str(e).lower() for e in errors)

    def test_ctid_only_with_postgresql(self):
        """Test that Ctid method only works with PostgreSQL sources."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(
                source={
                    "type": "mssql",
                    "server": "localhost",
                    "database": "sourcedb",
                    "table": "users",
                    "user": "user",
                    "password": "pass",
                },
                output={
                    "format": "csv",
                    "file_output": "/tmp/output.csv",
                },
                options={"method": "Ctid"},
            )
        errors = exc_info.value.errors()
        assert any("Ctid" in str(e) and "PostgreSQL" in str(e) for e in errors)

    def test_ctid_valid_with_postgresql(self):
        """Test that Ctid works with PostgreSQL source."""
        request = ExportRequest(
            source={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "sourcedb",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            output={
                "format": "parquet",
                "file_output": "/tmp/users.parquet",
            },
            options={"method": "Ctid"},
        )
        assert request.options.method == ParallelismMethod.CTID

    def test_rowid_only_with_oraodp(self):
        """Test that Rowid method only works with oraodp source."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(
                source={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "sourcedb",
                    "table": "users",
                    "user": "user",
                    "password": "pass",
                },
                output={
                    "format": "csv",
                    "file_output": "/tmp/output.csv",
                },
                options={"method": "Rowid"},
            )
        errors = exc_info.value.errors()
        assert any("Rowid" in str(e) and "Oracle" in str(e) for e in errors)

    def test_rowid_valid_with_oraodp(self):
        """Test that Rowid works with oraodp source."""
        request = ExportRequest(
            source={
                "type": "oraodp",
                "server": "localhost:1521",
                "database": "ORCL",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/users.csv",
            },
            options={"method": "Rowid"},
        )
        assert request.options.method == ParallelismMethod.ROWID

    def test_nzdataslice_with_netezza_types(self):
        """Test NZDataSlice works with all Netezza source types."""
        for nz_type in ["nzcopy", "nzoledb", "nzsql"]:
            request = ExportRequest(
                source={
                    "type": nz_type,
                    "server": "localhost",
                    "database": "nzdb",
                    "table": "data",
                    "user": "user",
                    "password": "pass",
                },
                output={
                    "format": "csv",
                    "file_output": "/tmp/output.csv",
                },
                options={"method": "NZDataSlice"},
            )
            assert request.options.method == ParallelismMethod.NZ_DATA_SLICE

    def test_physloc_only_with_sqlserver_types(self):
        """Test Physloc only works with SQL Server source types."""
        with pytest.raises(ValidationError) as exc_info:
            ExportRequest(
                source={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "sourcedb",
                    "table": "users",
                    "user": "user",
                    "password": "pass",
                },
                output={
                    "format": "csv",
                    "file_output": "/tmp/output.csv",
                },
                options={"method": "Physloc"},
            )
        errors = exc_info.value.errors()
        assert any("Physloc" in str(e) for e in errors)

    def test_physloc_valid_with_mssql(self):
        """Test Physloc works with mssql source."""
        request = ExportRequest(
            source={
                "type": "mssql",
                "server": "localhost",
                "database": "sourcedb",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
            options={"method": "Physloc"},
        )
        assert request.options.method == ParallelismMethod.PHYSLOC

    def test_physloc_valid_with_msoledbsql(self):
        """Test Physloc works with msoledbsql source."""
        request = ExportRequest(
            source={
                "type": "msoledbsql",
                "server": "localhost",
                "database": "sourcedb",
                "table": "users",
                "user": "user",
                "password": "pass",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
            options={"method": "Physloc"},
        )
        assert request.options.method == ParallelismMethod.PHYSLOC


class TestConnectionValidationRequest:
    """Tests for ConnectionValidationRequest model."""

    def test_valid_source_validation_request(self):
        """Test valid source validation request."""
        request = ConnectionValidationRequest(
            connection={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
            side="source",
        )
        assert request.side == "source"

    def test_invalid_side(self):
        """Test that invalid side is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ConnectionValidationRequest(
                connection={
                    "type": "pgsql",
                    "server": "localhost:5432",
                    "database": "testdb",
                    "user": "user",
                    "password": "pass",
                },
                side="invalid",
            )
        errors = exc_info.value.errors()
        assert any("side" in str(e) for e in errors)


class TestParallelismSuggestionRequest:
    """Tests for ParallelismSuggestionRequest model."""

    def test_valid_suggestion_request(self):
        """Test valid parallelism suggestion request."""
        request = ParallelismSuggestionRequest(
            source_type="pgsql",
            has_numeric_key=True,
            has_identity_column=True,
            table_size_estimate="large",
        )
        assert request.source_type == "pgsql"
        assert request.has_numeric_key is True
        assert request.table_size_estimate == "large"

    def test_invalid_table_size(self):
        """Test that invalid table size is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            ParallelismSuggestionRequest(
                source_type="pgsql", has_numeric_key=True, table_size_estimate="huge"
            )
        errors = exc_info.value.errors()
        assert any("table_size" in str(e) for e in errors)

    def test_default_has_identity_column(self):
        """Test default value for has_identity_column."""
        request = ParallelismSuggestionRequest(
            source_type="pgsql", has_numeric_key=True, table_size_estimate="medium"
        )
        assert request.has_identity_column is False
