"""Tests for version detection and capabilities registry."""

import subprocess
from unittest.mock import patch, Mock

import pytest

from src.version import (
    FastBCPVersion,
    VersionDetector,
    VERSION_REGISTRY,
    check_version_compatibility,
)


class TestFastBCPVersion:
    """Tests for FastBCPVersion dataclass."""

    def test_parse_full_version_string(self):
        """Test parsing a full 'FastBCP Version X.Y.Z.W' string."""
        v = FastBCPVersion.parse("FastBCP Version 0.29.1.0")
        assert v.major == 0
        assert v.minor == 29
        assert v.patch == 1
        assert v.build == 0

    def test_parse_numeric_only(self):
        """Test parsing a bare version number."""
        v = FastBCPVersion.parse("0.29.1.0")
        assert v == FastBCPVersion(0, 29, 1, 0)

    def test_parse_with_whitespace(self):
        """Test parsing a version string with leading/trailing whitespace."""
        v = FastBCPVersion.parse("  FastBCP Version 1.2.3.4  ")
        assert v == FastBCPVersion(1, 2, 3, 4)

    def test_parse_invalid_string(self):
        """Test that an unparseable string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            FastBCPVersion.parse("no version here")

    def test_parse_incomplete_version(self):
        """Test that an incomplete version string raises ValueError."""
        with pytest.raises(ValueError, match="Cannot parse version"):
            FastBCPVersion.parse("0.29.1")

    def test_str_representation(self):
        """Test string representation."""
        v = FastBCPVersion(0, 29, 1, 0)
        assert str(v) == "0.29.1.0"

    def test_equality(self):
        """Test equality comparison."""
        a = FastBCPVersion(0, 29, 1, 0)
        b = FastBCPVersion(0, 29, 1, 0)
        assert a == b

    def test_inequality(self):
        """Test inequality comparison."""
        a = FastBCPVersion(0, 29, 1, 0)
        b = FastBCPVersion(0, 30, 0, 0)
        assert a != b

    def test_less_than(self):
        """Test less-than comparison."""
        a = FastBCPVersion(0, 28, 0, 0)
        b = FastBCPVersion(0, 29, 1, 0)
        assert a < b

    def test_greater_than(self):
        """Test greater-than comparison (via total_ordering)."""
        a = FastBCPVersion(0, 29, 1, 0)
        b = FastBCPVersion(0, 28, 9, 9)
        assert a > b

    def test_comparison_across_fields(self):
        """Test comparison across major/minor/patch/build."""
        versions = [
            FastBCPVersion(0, 28, 0, 0),
            FastBCPVersion(0, 29, 0, 0),
            FastBCPVersion(0, 29, 0, 1),
            FastBCPVersion(0, 29, 1, 0),
            FastBCPVersion(1, 0, 0, 0),
        ]
        for i in range(len(versions) - 1):
            assert versions[i] < versions[i + 1]


class TestVersionDetector:
    """Tests for VersionDetector class."""

    @patch("src.version.subprocess.run")
    def test_detect_success(self, mock_run):
        """Test successful version detection."""
        mock_result = Mock()
        mock_result.stdout = "FastBCP Version 0.29.1.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version == FastBCPVersion(0, 29, 1, 0)
        mock_run.assert_called_once_with(
            ["/fake/binary", "--version", "--nobanner"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    @patch("src.version.subprocess.run")
    def test_detect_failure_no_match(self, mock_run):
        """Test detection when output doesn't match version pattern."""
        mock_result = Mock()
        mock_result.stdout = "Unknown output"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_timeout(self, mock_run):
        """Test detection handles timeout gracefully."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=10)

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_binary_not_found(self, mock_run):
        """Test detection handles missing binary gracefully."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector("/fake/binary")
        version = detector.detect()

        assert version is None

    @patch("src.version.subprocess.run")
    def test_detect_caching(self, mock_run):
        """Test that second call returns cached result without re-running subprocess."""
        mock_result = Mock()
        mock_result.stdout = "FastBCP Version 0.29.1.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        v1 = detector.detect()
        v2 = detector.detect()

        assert v1 == v2
        assert mock_run.call_count == 1

    @patch("src.version.subprocess.run")
    def test_capabilities_known_version(self, mock_run):
        """Test capabilities resolution for a known version."""
        mock_result = Mock()
        mock_result.stdout = "FastBCP Version 0.29.1.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        assert "oraodp" in caps.source_types
        assert "parquet" in caps.output_formats
        assert "s3" in caps.storage_targets
        assert caps.supports_nobanner is True
        assert caps.supports_version_flag is True
        assert caps.supports_cloud_profile is True
        assert caps.supports_merge is True

    @patch("src.version.subprocess.run")
    def test_capabilities_newer_unknown_version(self, mock_run):
        """Test capabilities falls back to latest known for newer unknown version."""
        mock_result = Mock()
        mock_result.stdout = "FastBCP Version 1.0.0.0\n"
        mock_result.stderr = ""
        mock_run.return_value = mock_result

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        # Should get the latest known capabilities (0.30.0.0)
        assert caps == VERSION_REGISTRY["0.30.0.0"]

    @patch("src.version.subprocess.run")
    def test_capabilities_undetected_version(self, mock_run):
        """Test capabilities falls back to latest known when detection fails."""
        mock_run.side_effect = FileNotFoundError("No such file")

        detector = VersionDetector("/fake/binary")
        detector.detect()
        caps = detector.capabilities

        # Should fall back to latest known
        assert caps == VERSION_REGISTRY["0.30.0.0"]

    def test_registry_0291_source_completeness(self):
        """Test that 0.29.1.0 registry has all expected source types."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        expected = {
            "clickhouse",
            "hana",
            "mssql",
            "msoledbsql",
            "mysql",
            "nzcopy",
            "nzoledb",
            "nzsql",
            "odbc",
            "oledb",
            "oraodp",
            "pgcopy",
            "pgsql",
            "teradata",
        }
        assert caps.source_types == expected

    def test_registry_0291_format_completeness(self):
        """Test that 0.29.1.0 registry has all expected output formats."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        expected = {
            "csv",
            "tsv",
            "json",
            "bson",
            "parquet",
            "xlsx",
            "binary",
        }
        assert caps.output_formats == expected

    def test_registry_0291_method_completeness(self):
        """Test that 0.29.1.0 registry has all expected parallelism methods."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        expected = {
            "Ctid",
            "DataDriven",
            "Ntile",
            "NZDataSlice",
            "None",
            "Physloc",
            "Random",
            "RangeId",
            "Rowid",
        }
        assert caps.parallelism_methods == expected

    def test_registry_0291_storage_completeness(self):
        """Test that 0.29.1.0 registry has all expected storage targets."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        expected = {
            "local",
            "s3",
            "s3compatible",
            "azure_blob",
            "azure_datalake",
            "fabric_onelake",
        }
        assert caps.storage_targets == expected


class TestCheckVersionCompatibility:
    """Tests for check_version_compatibility function."""

    def test_basic_params_no_warnings(self):
        """Basic params produce no warnings."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        version = FastBCPVersion(0, 29, 1, 0)
        warnings = check_version_compatibility(
            {"source": {"type": "pgsql"}}, caps, version
        )
        assert warnings == []

    def test_empty_params_no_warnings(self):
        """Empty params produce no warnings."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        version = FastBCPVersion(0, 29, 1, 0)
        warnings = check_version_compatibility({}, caps, version)
        assert warnings == []

    def test_none_version_no_warnings(self):
        """None detected version with basic params produces no warnings."""
        caps = VERSION_REGISTRY["0.29.1.0"]
        warnings = check_version_compatibility(
            {"source": {"type": "pgsql"}}, caps, None
        )
        assert warnings == []
