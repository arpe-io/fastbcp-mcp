"""
Version detection and capabilities registry for FastBCP.

This module detects the installed FastBCP binary version and maps it
to known capabilities (supported source types, output formats, parallelism methods,
storage targets, and feature flags).
"""

import logging
import re
import subprocess
from dataclasses import dataclass
from functools import total_ordering
from typing import Dict, FrozenSet, Optional


logger = logging.getLogger(__name__)


@total_ordering
@dataclass(frozen=True)
class FastBCPVersion:
    """Represents a FastBCP version number (X.Y.Z.W)."""

    major: int
    minor: int
    patch: int
    build: int

    @classmethod
    def parse(cls, version_string: str) -> "FastBCPVersion":
        """Parse a version string like 'FastBCP Version 0.29.1.0' or '0.29.1.0'.

        Args:
            version_string: Version string to parse

        Returns:
            FastBCPVersion instance

        Raises:
            ValueError: If the string cannot be parsed
        """
        match = re.search(r"(\d+)\.(\d+)\.(\d+)\.(\d+)", version_string.strip())
        if not match:
            raise ValueError(f"Cannot parse version from: {version_string!r}")
        return cls(
            major=int(match.group(1)),
            minor=int(match.group(2)),
            patch=int(match.group(3)),
            build=int(match.group(4)),
        )

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}.{self.build}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FastBCPVersion):
            return NotImplemented
        return self._tuple == other._tuple

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, FastBCPVersion):
            return NotImplemented
        return self._tuple < other._tuple

    @property
    def _tuple(self) -> tuple:
        return (self.major, self.minor, self.patch, self.build)


@dataclass(frozen=True)
class VersionCapabilities:
    """Capabilities available in a specific FastBCP version."""

    source_types: FrozenSet[str]
    output_formats: FrozenSet[str]
    parallelism_methods: FrozenSet[str]
    storage_targets: FrozenSet[str]
    supports_nobanner: bool = False
    supports_version_flag: bool = False
    supports_cloud_profile: bool = False
    supports_merge: bool = False


# Static version registry: version string -> capabilities
VERSION_REGISTRY: Dict[str, VersionCapabilities] = {
    "0.29.1.0": VersionCapabilities(
        source_types=frozenset(
            [
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
            ]
        ),
        output_formats=frozenset(
            [
                "csv",
                "tsv",
                "json",
                "bson",
                "parquet",
                "xlsx",
                "binary",
            ]
        ),
        parallelism_methods=frozenset(
            [
                "Ctid",
                "DataDriven",
                "Ntile",
                "NZDataSlice",
                "None",
                "Physloc",
                "Random",
                "RangeId",
                "Rowid",
            ]
        ),
        storage_targets=frozenset(
            [
                "local",
                "s3",
                "s3compatible",
                "azure_blob",
                "azure_datalake",
                "fabric_onelake",
            ]
        ),
        supports_nobanner=True,
        supports_version_flag=True,
        supports_cloud_profile=True,
        supports_merge=True,
    ),
}

# Pre-sorted list of known versions for lookup
_SORTED_VERSIONS = sorted(
    [(FastBCPVersion.parse(k), v) for k, v in VERSION_REGISTRY.items()],
    key=lambda x: x[0],
)


class VersionDetector:
    """Detects FastBCP binary version and resolves capabilities."""

    def __init__(self, binary_path: str):
        self._binary_path = binary_path
        self._detected_version: Optional[FastBCPVersion] = None
        self._detection_done = False

    def detect(self, timeout: int = 10) -> Optional[FastBCPVersion]:
        """Detect the FastBCP version by running the binary.

        Runs ``[binary_path, "--version", "--nobanner"]`` and parses the output.
        Results are cached after the first call.

        Args:
            timeout: Subprocess timeout in seconds

        Returns:
            FastBCPVersion if detected, None otherwise
        """
        if self._detection_done:
            return self._detected_version

        self._detection_done = True

        try:
            result = subprocess.run(
                [self._binary_path, "--version", "--nobanner"],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            output = (result.stdout + result.stderr).strip()
            match = re.search(r"FastBCP\s+Version\s+(\d+)\.(\d+)\.(\d+)\.(\d+)", output)
            if match:
                self._detected_version = FastBCPVersion(
                    major=int(match.group(1)),
                    minor=int(match.group(2)),
                    patch=int(match.group(3)),
                    build=int(match.group(4)),
                )
                logger.info(f"Detected FastBCP version: {self._detected_version}")
            else:
                logger.warning(f"Could not parse version from output: {output!r}")
        except subprocess.TimeoutExpired:
            logger.warning("Version detection timed out")
        except FileNotFoundError:
            logger.warning(f"Binary not found at: {self._binary_path}")
        except Exception as e:
            logger.warning(f"Version detection failed: {e}")

        return self._detected_version

    @property
    def capabilities(self) -> VersionCapabilities:
        """Resolve capabilities for the detected version.

        If the detected version matches a registry entry exactly, return that.
        If the version is newer than all known entries, return the latest known.
        If detection failed, return the latest known entry as a fallback.
        """
        if not self._detection_done:
            self.detect()

        if not _SORTED_VERSIONS:
            # No registry entries at all — return empty capabilities
            return VersionCapabilities(
                source_types=frozenset(),
                output_formats=frozenset(),
                parallelism_methods=frozenset(),
                storage_targets=frozenset(),
            )

        if self._detected_version is None:
            # Detection failed — fall back to latest known
            return _SORTED_VERSIONS[-1][1]

        # Find the highest registry entry <= detected version
        best: Optional[VersionCapabilities] = None
        for ver, caps in _SORTED_VERSIONS:
            if ver <= self._detected_version:
                best = caps
            else:
                break

        # If detected version is older than all known, fall back to latest
        return best if best is not None else _SORTED_VERSIONS[-1][1]
