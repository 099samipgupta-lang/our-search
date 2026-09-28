from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


@dataclass(frozen=True)
class VerificationFinding:
    source_url: str
    checks: dict[str, bool]
    supported: bool


class VerificationEngine:
    """
    Structured verification machine for OUR SEARCH.

    It checks the structure and consistency of retrieved
    information before the explanation layer uses it.

    This is deliberately conservative: structural checks are
    not treated as proof that a source is factually correct.
    """

    def _valid_url(self, url):
        if not isinstance(url, str) or not url:
            return False

        try:
            parsed = urlparse(url)
        except ValueError:
            return False

        return bool(
            parsed.scheme
            and parsed.netloc
        )

    def inspect_source(
        self,
        source: dict[str, Any],
    ) -> VerificationFinding:
        title = source.get("title", "")
        snippet = source.get("snippet", "")
        url = source.get("url", "")

        checks = {
            "has_title": isinstance(title, str)
            and bool(title.strip()),

            "has_content": isinstance(snippet, str)
            and bool(snippet.strip()),

            "valid_url": self._valid_url(url),
        }

        supported = all(checks.values())

        return VerificationFinding(
            source_url=str(url),
            checks=checks,
            supported=supported,
        )

    def verify_sources(
        self,
        sources,
    ):
        findings = []

        for source in sources:
            if not isinstance(source, dict):
                continue

            finding = self.inspect_source(source)

            findings.append(
                {
                    "source_url": finding.source_url,
                    "checks": finding.checks,
                    "supported": finding.supported,
                }
            )

        supported_count = sum(
            1
            for finding in findings
            if finding["supported"]
        )

        return {
            "source_count": len(findings),
            "supported_source_count": supported_count,
            "findings": findings,
        }


__all__ = [
    "VerificationFinding",
    "VerificationEngine",
]
