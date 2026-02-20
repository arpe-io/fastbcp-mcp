"""Tests for FastBCP command builder."""

from pathlib import Path
from unittest.mock import Mock, patch
import subprocess

import pytest

from src.fastbcp import (
    CommandBuilder,
    FastBCPError,
    get_supported_formats,
    suggest_parallelism_method,
)
from src.validators import ExportRequest
from src.version import FastBCPVersion


@pytest.fixture
def mock_binary(tmp_path):
    """Create a mock FastBCP binary."""
    binary = tmp_path / "FastBCP"
    binary.write_text("#!/bin/bash\necho 'mock binary'")
    binary.chmod(0o755)
    return str(binary)


@pytest.fixture
def command_builder(mock_binary):
    """Create a CommandBuilder with mock binary."""
    with patch("src.fastbcp.VersionDetector") as MockDetector:
        mock_detector = MockDetector.return_value
        mock_detector.detect.return_value = FastBCPVersion(0, 29, 1, 0)
        mock_detector.capabilities = Mock()
        mock_detector.capabilities.source_types = frozenset(["pgsql", "mssql"])
        mock_detector.capabilities.output_formats = frozenset(["csv", "parquet"])
        mock_detector.capabilities.parallelism_methods = frozenset(["None", "Ctid"])
        mock_detector.capabilities.storage_targets = frozenset(["local", "s3"])
        mock_detector.capabilities.supports_nobanner = True
        mock_detector.capabilities.supports_version_flag = True
        mock_detector.capabilities.supports_cloud_profile = True
        mock_detector.capabilities.supports_merge = True
        builder = CommandBuilder(mock_binary)
    return builder


@pytest.fixture
def sample_request():
    """Create a sample export request."""
    return ExportRequest(
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
        options={
            "method": "Ctid",
            "degree": 4,
            "load_mode": "Truncate",
            "map_method": "Position",
        },
    )


class TestCommandBuilder:
    """Tests for CommandBuilder class."""

    def test_init_with_valid_binary(self, mock_binary):
        """Test initialization with valid binary."""
        with patch("src.fastbcp.VersionDetector"):
            builder = CommandBuilder(mock_binary)
        assert builder.binary_path == Path(mock_binary)

    def test_init_with_nonexistent_binary(self):
        """Test initialization with nonexistent binary fails."""
        with pytest.raises(FastBCPError) as exc_info:
            CommandBuilder("/nonexistent/path/FastBCP")
        assert "not found" in str(exc_info.value)

    def test_init_with_non_executable_binary(self, tmp_path):
        """Test initialization with non-executable binary fails."""
        binary = tmp_path / "FastBCP"
        binary.write_text("not executable")
        binary.chmod(0o644)

        with pytest.raises(FastBCPError) as exc_info:
            CommandBuilder(str(binary))
        assert "not executable" in str(exc_info.value)

    def test_build_command_basic(self, command_builder, sample_request):
        """Test building a basic export command."""
        command = command_builder.build_command(sample_request)

        assert isinstance(command, list)
        assert command[0] == str(command_builder.binary_path)

        # Check source parameters
        assert "--sourceconnectiontype" in command
        assert "pgsql" in command
        assert "--sourceserver" in command
        assert "localhost:5432" in command
        assert "--sourceuser" in command
        assert "sourceuser" in command
        assert "--sourcepassword" in command
        assert "sourcepass" in command
        assert "--sourcedatabase" in command
        assert "sourcedb" in command
        assert "--sourceschema" in command
        assert "public" in command
        assert "--sourcetable" in command
        assert "users" in command

        # Check output parameters
        assert "--format" in command
        assert "csv" in command
        assert "--fileoutput" in command
        assert "/tmp/users.csv" in command

        # Check options
        assert "--method" in command
        assert "Ctid" in command
        assert "--degree" in command
        assert "4" in command
        assert "--loadmode" in command
        assert "Truncate" in command

    def test_build_command_with_query(self, command_builder):
        """Test building command with query instead of table."""
        request = ExportRequest(
            source={
                "type": "pgsql",
                "server": "localhost:5432",
                "database": "sourcedb",
                "query": "SELECT * FROM users WHERE active = true",
                "user": "sourceuser",
                "password": "sourcepass",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
        )

        command = command_builder.build_command(request)

        assert "--query" in command
        query_idx = command.index("--query")
        assert "SELECT * FROM users WHERE active = true" in command[query_idx + 1]
        assert "--sourcetable" not in command

    def test_build_command_with_directory(self, command_builder):
        """Test building command with directory output."""
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
                "directory": "/tmp/output/",
            },
        )

        command = command_builder.build_command(request)
        assert "--directory" in command
        idx = command.index("--directory")
        assert command[idx + 1] == "/tmp/output/"

    def test_build_command_with_storage_target(self, command_builder):
        """Test building command with S3 storage target and cloud profile."""
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
                "directory": "s3://my-bucket/exports/",
                "storage_target": "s3",
            },
            options={
                "cloud_profile": "my-aws-profile",
            },
        )

        command = command_builder.build_command(request)
        assert "--storagetarget" in command
        assert "s3" in command
        assert "--cloudprofile" in command
        assert "my-aws-profile" in command

    def test_build_command_parquet_compression(self, command_builder):
        """Test building command with parquet compression."""
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
                "file_output": "/tmp/output.parquet",
                "parquet_compression": "Snappy",
            },
        )

        command = command_builder.build_command(request)
        assert "--parquetcompression" in command
        idx = command.index("--parquetcompression")
        assert command[idx + 1] == "Snappy"

    def test_build_command_with_delimiter_and_quotes(self, command_builder):
        """Test building command with delimiter and quotes."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
                "delimiter": "|",
                "quotes": '"',
                "encoding": "utf-8",
            },
        )

        command = command_builder.build_command(request)
        assert "--delimiter" in command
        assert "|" in command
        assert "--quotes" in command
        assert '"' in command
        assert "--encoding" in command
        assert "utf-8" in command

    def test_build_command_no_header(self, command_builder):
        """Test building command with no_header flag."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
                "no_header": True,
            },
        )

        command = command_builder.build_command(request)
        assert "--noheader" in command

    def test_build_command_timestamped_and_merge(self, command_builder):
        """Test building command with timestamped and merge flags."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
                "timestamped": True,
                "merge": True,
            },
        )

        command = command_builder.build_command(request)
        assert "--timestamped" in command
        assert "--merge" in command

    def test_build_command_decimal_separator(self, command_builder):
        """Test building command with decimal separator."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
                "decimal_separator": ",",
            },
        )

        command = command_builder.build_command(request)
        assert "--decimalseparator" in command
        assert "," in command

    def test_build_command_bool_format(self, command_builder):
        """Test building command with bool format."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
                "bool_format": "OneZero",
            },
        )

        command = command_builder.build_command(request)
        assert "--boolformat" in command
        assert "OneZero" in command

    def test_build_command_date_format(self, command_builder):
        """Test building command with date format."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
                "date_format": "yyyy-MM-dd",
            },
        )

        command = command_builder.build_command(request)
        assert "--dateformat" in command
        assert "yyyy-MM-dd" in command

    def test_build_command_with_connect_string(self, command_builder):
        """Test building command with source connect_string."""
        request = ExportRequest(
            source={
                "type": "odbc",
                "database": "sourcedb",
                "table": "users",
                "connect_string": "Driver={ODBC Driver 17};Server=myhost;Database=sourcedb;UID=u;PWD=p",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
        )

        command = command_builder.build_command(request)
        assert "--sourceconnectstring" in command
        assert "--sourceserver" not in command
        assert "--sourceuser" not in command

    def test_build_command_with_dsn(self, command_builder):
        """Test building command with source DSN."""
        request = ExportRequest(
            source={
                "type": "odbc",
                "database": "sourcedb",
                "table": "users",
                "dsn": "MyDSN",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
        )

        command = command_builder.build_command(request)
        assert "--sourcedsn" in command
        idx = command.index("--sourcedsn")
        assert command[idx + 1] == "MyDSN"

    def test_build_command_with_trusted_auth(self, command_builder):
        """Test building command with trusted authentication."""
        request = ExportRequest(
            source={
                "type": "mssql",
                "server": "localhost",
                "database": "sourcedb",
                "table": "users",
                "trusted_auth": True,
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
        )

        command = command_builder.build_command(request)
        assert "--sourcetrusted" in command
        assert "--sourcepassword" not in command

    def test_build_command_with_provider(self, command_builder):
        """Test building command with source provider."""
        request = ExportRequest(
            source={
                "type": "oledb",
                "server": "localhost",
                "database": "sourcedb",
                "table": "users",
                "user": "user",
                "password": "pass",
                "provider": "SQLOLEDB",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
        )

        command = command_builder.build_command(request)
        assert "--sourceprovider" in command
        idx = command.index("--sourceprovider")
        assert command[idx + 1] == "SQLOLEDB"

    def test_build_command_with_application_intent(self, command_builder):
        """Test building command with application intent."""
        request = ExportRequest(
            source={
                "type": "mssql",
                "server": "localhost",
                "database": "sourcedb",
                "table": "users",
                "user": "user",
                "password": "pass",
                "application_intent": "ReadOnly",
            },
            output={
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
        )

        command = command_builder.build_command(request)
        assert "--applicationintent" in command
        assert "ReadOnly" in command

    def test_build_command_with_all_options(self, command_builder):
        """Test building command with all option parameters."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
            options={
                "method": "RangeId",
                "distribute_key_column": "id",
                "degree": 8,
                "load_mode": "Append",
                "batch_size": 50000,
                "map_method": "Name",
                "run_id": "test-run-001",
                "settings_file": "/path/to/settings.json",
                "log_level": "Debug",
                "no_banner": True,
                "license_path": "/path/to/license.lic",
            },
        )

        command = command_builder.build_command(request)

        assert "--distributeKeyColumn" in command
        assert "id" in command
        assert "--batchsize" in command
        assert "50000" in command
        assert "--mapmethod" in command
        assert "Name" in command
        assert "--runid" in command
        assert "test-run-001" in command
        assert "--settingsfile" in command
        assert "/path/to/settings.json" in command
        assert "--loglevel" in command
        assert "Debug" in command
        assert "--nobanner" in command
        assert "--license" in command
        assert "/path/to/license.lic" in command

    def test_build_command_with_data_driven_query(self, command_builder):
        """Test building command with data_driven_query."""
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
                "format": "csv",
                "file_output": "/tmp/output.csv",
            },
            options={
                "method": "DataDriven",
                "distribute_key_column": "region",
                "data_driven_query": "SELECT DISTINCT region FROM users",
            },
        )

        command = command_builder.build_command(request)
        assert "--datadrivenquery" in command
        idx = command.index("--datadrivenquery")
        assert command[idx + 1] == "SELECT DISTINCT region FROM users"

    def test_mask_password(self, command_builder):
        """Test password masking in commands."""
        command = [
            "/path/to/FastBCP",
            "--sourceuser",
            "user1",
            "--sourcepassword",
            "secret123",
        ]

        masked = command_builder.mask_password(command)

        assert "secret123" not in masked
        assert masked.count("******") == 1
        assert "user1" in masked

    def test_mask_connection_string(self, command_builder):
        """Test connection string masking."""
        command = [
            "/path/to/FastBCP",
            "--sourceconnectstring",
            "Server=host;UID=user;PWD=secret",
        ]

        masked = command_builder.mask_password(command)
        assert "Server=host;UID=user;PWD=secret" not in masked
        assert masked.count("******") == 1

    def test_mask_short_flags(self, command_builder):
        """Test masking with short flags."""
        command = [
            "/path/to/FastBCP",
            "-x",
            "source_pass",
            "-g",
            "source_connstr",
        ]

        masked = command_builder.mask_password(command)
        assert "source_pass" not in masked
        assert "source_connstr" not in masked
        assert masked.count("******") == 2

    def test_format_command_display_with_mask(self, command_builder, sample_request):
        """Test formatting command for display with masked passwords."""
        command = command_builder.build_command(sample_request)
        display = command_builder.format_command_display(command, mask=True)

        assert "--sourcepassword ******" in display
        assert " sourcepass " not in display and not display.endswith(" sourcepass")
        assert "sourceuser" in display

    def test_format_command_display_without_mask(self, command_builder, sample_request):
        """Test formatting command for display without masking."""
        command = command_builder.build_command(sample_request)
        display = command_builder.format_command_display(command, mask=False)

        assert "--sourcepassword sourcepass" in display

    def test_get_version_method(self, command_builder):
        """Test get_version returns structured info."""
        info = command_builder.get_version()

        assert "version" in info
        assert "detected" in info
        assert "binary_path" in info
        assert "capabilities" in info
        assert "source_types" in info["capabilities"]
        assert "output_formats" in info["capabilities"]
        assert "parallelism_methods" in info["capabilities"]
        assert "storage_targets" in info["capabilities"]
        assert "supports_nobanner" in info["capabilities"]
        assert "supports_cloud_profile" in info["capabilities"]
        assert "supports_merge" in info["capabilities"]

    def test_version_detector_property(self, command_builder):
        """Test version_detector property is accessible."""
        assert command_builder.version_detector is not None

    @patch("subprocess.run")
    def test_execute_command_success(self, mock_run, command_builder):
        """Test successful command execution."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Export completed successfully"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        command = [str(command_builder.binary_path), "--help"]
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=10
        )

        assert return_code == 0
        assert "success" in stdout.lower()
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_execute_command_failure(self, mock_run, command_builder):
        """Test failed command execution."""
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        mock_result.stderr = "Connection failed"
        mock_run.return_value = mock_result

        command = [str(command_builder.binary_path), "--help"]
        return_code, stdout, stderr = command_builder.execute_command(
            command, timeout=10
        )

        assert return_code == 1
        assert "failed" in stderr.lower()

    @patch("subprocess.run")
    def test_execute_command_timeout(self, mock_run, command_builder):
        """Test command execution timeout."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=1)

        command = [str(command_builder.binary_path), "--help"]
        with pytest.raises(FastBCPError) as exc_info:
            command_builder.execute_command(command, timeout=1)

        assert "timed out" in str(exc_info.value).lower()

    @patch("subprocess.run")
    def test_execute_command_with_logging(self, mock_run, command_builder, tmp_path):
        """Test command execution with log saving."""
        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "Success"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        log_dir = tmp_path / "logs"
        command = [str(command_builder.binary_path), "--help"]
        command_builder.execute_command(command, timeout=10, log_dir=log_dir)

        assert log_dir.exists()
        log_files = list(log_dir.glob("fastbcp_*.log"))
        assert len(log_files) == 1

        log_content = log_files[0].read_text()
        assert "FastBCP Execution Log" in log_content
        assert "Return Code: 0" in log_content


class TestHelperFunctions:
    """Tests for helper functions."""

    def test_get_supported_formats(self):
        """Test getting supported formats."""
        formats = get_supported_formats()

        assert isinstance(formats, dict)
        assert "Database Sources" in formats
        assert "Output Formats" in formats
        assert "Storage Targets" in formats

        # Check database sources
        sources = formats["Database Sources"]
        assert "PostgreSQL (pgsql, pgcopy)" in sources
        assert "SQL Server (mssql, msoledbsql, odbc, oledb)" in sources
        assert "Oracle (oraodp)" in sources
        assert "SAP HANA (hana)" in sources
        assert "Teradata (teradata)" in sources

        # Check output formats
        assert "csv" in formats["Output Formats"]
        assert "parquet" in formats["Output Formats"]
        assert "binary" in formats["Output Formats"]
        assert len(formats["Output Formats"]) == 7

        # Check storage targets
        assert "local" in formats["Storage Targets"]
        assert "s3" in formats["Storage Targets"]
        assert "azure_blob" in formats["Storage Targets"]
        assert len(formats["Storage Targets"]) == 6

    def test_suggest_parallelism_small_table(self):
        """Test parallelism suggestion for small table."""
        suggestion = suggest_parallelism_method(
            source_type="pgsql",
            has_numeric_key=True,
            has_identity_column=False,
            table_size_estimate="small",
        )

        assert suggestion["method"] == "None"
        assert "small" in suggestion["explanation"].lower()

    def test_suggest_parallelism_postgresql(self):
        """Test parallelism suggestion for PostgreSQL source."""
        suggestion = suggest_parallelism_method(
            source_type="pgsql",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="large",
        )

        assert suggestion["method"] == "Ctid"

    def test_suggest_parallelism_oraodp(self):
        """Test parallelism suggestion for oraodp source."""
        suggestion = suggest_parallelism_method(
            source_type="oraodp",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="medium",
        )

        assert suggestion["method"] == "Rowid"

    def test_suggest_parallelism_netezza(self):
        """Test parallelism suggestion for Netezza source types."""
        for nz_type in ["nzcopy", "nzoledb", "nzsql"]:
            suggestion = suggest_parallelism_method(
                source_type=nz_type,
                has_numeric_key=False,
                has_identity_column=False,
                table_size_estimate="large",
            )
            assert suggestion["method"] == "NZDataSlice"

    def test_suggest_parallelism_mssql_no_key(self):
        """Test parallelism suggestion for SQL Server without numeric key."""
        suggestion = suggest_parallelism_method(
            source_type="mssql",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="large",
        )

        assert suggestion["method"] == "Physloc"

    def test_suggest_parallelism_msoledbsql_no_key(self):
        """Test parallelism suggestion for msoledbsql without numeric key."""
        suggestion = suggest_parallelism_method(
            source_type="msoledbsql",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="medium",
        )

        assert suggestion["method"] == "Physloc"

    def test_suggest_parallelism_with_numeric_key(self):
        """Test parallelism suggestion for table with numeric key."""
        suggestion = suggest_parallelism_method(
            source_type="mssql",
            has_numeric_key=True,
            has_identity_column=True,
            table_size_estimate="large",
        )

        assert suggestion["method"] in ["RangeId", "Random"]

    def test_suggest_parallelism_generic_large_table(self):
        """Test parallelism suggestion for generic large table."""
        suggestion = suggest_parallelism_method(
            source_type="mysql",
            has_numeric_key=False,
            has_identity_column=False,
            table_size_estimate="large",
        )

        assert suggestion["method"] in ["DataDriven", "Ntile"]
