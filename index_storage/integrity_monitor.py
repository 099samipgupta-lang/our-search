"""
OUR SEARCH Storage — Milestone 13
Production integrity diagnostics.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class IntegrityReport:
    checked: int
    healthy: int
    corrupted: int
    missing_integrity: int
    errors: tuple


class StorageIntegrityMonitor:
    """Scans stored objects and validates their persisted checksums."""

    def __init__(self, storage):
        self.storage = storage

    def scan(self, prefix=""):
        checked = 0
        healthy = 0
        corrupted = 0
        missing_integrity = 0
        errors = []

        keys = self.storage.list_keys(prefix)

        for key in keys:
            checked += 1

            try:
                data = self.storage.get(key)
                healthy += 1
            except FileNotFoundError as exc:
                missing_integrity += 1
                errors.append((key, str(exc)))
            except ValueError as exc:
                corrupted += 1
                errors.append((key, str(exc)))
            except Exception as exc:
                errors.append((key, str(exc)))

        return IntegrityReport(
            checked=checked,
            healthy=healthy,
            corrupted=corrupted,
            missing_integrity=missing_integrity,
            errors=tuple(errors),
        )

    def require_clean(self, prefix=""):
        report = self.scan(prefix)

        if (
            report.corrupted
            or report.missing_integrity
            or len(report.errors)
        ):
            raise RuntimeError(
                "Storage integrity check failed"
            )

        return report
