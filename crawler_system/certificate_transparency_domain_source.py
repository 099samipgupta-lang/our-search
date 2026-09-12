from __future__ import annotations

import json
import re
from time import time
from typing import Any
from urllib.parse import quote, urlsplit

from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
)
from crawler_system.fetcher import Fetcher


class CertificateTransparencyDomainSource:
    """
    Independent domain-discovery source based on
    Certificate Transparency certificate records.

    This source is intentionally separate from the normal
    page-level discovery system and Stage 4.2 web-graph
    expansion.

    The CT endpoint is configurable so the source does not
    hard-code OUR SEARCH to a particular external provider.
    """

    name = "certificate_transparency"

    DEFAULT_ENDPOINT = (
        "https://crt.sh/?q={query}&output=json"
    )

    DEFAULT_QUERY = "%25"

    def __init__(
        self,
        endpoint: str | None = None,
        query: str = DEFAULT_QUERY,
        fetcher: Fetcher | None = None,
        max_candidates: int = 1000,
    ):
        if endpoint is None:
            endpoint = self.DEFAULT_ENDPOINT

        if not isinstance(endpoint, str):
            raise ValueError(
                "endpoint must be a string"
            )

        endpoint = endpoint.strip()

        if not endpoint:
            raise ValueError(
                "endpoint must not be empty"
            )

        if "{query}" not in endpoint:
            raise ValueError(
                "endpoint must contain {query}"
            )

        if not isinstance(query, str):
            raise ValueError(
                "query must be a string"
            )

        if max_candidates < 1:
            raise ValueError(
                "max_candidates must be at least 1"
            )

        self.endpoint = endpoint
        self.query = query
        self.fetcher = (
            fetcher
            if fetcher is not None
            else Fetcher(
                user_agent="OurSearchBot/1.0"
            )
        )
        self.max_candidates = int(
            max_candidates
        )

    # ============================================================
    # SOURCE AVAILABILITY
    # ============================================================

    def can_discover(
        self,
        context: DomainDiscoveryContext,
    ) -> bool:
        """
        CT discovery does not require a crawled page.

        It can operate with an empty discovery context.
        """

        return True

    # ============================================================
    # DOMAIN NORMALIZATION
    # ============================================================

    @staticmethod
    def _normalize_hostname(
        value: str,
    ) -> str | None:
        if not isinstance(value, str):
            return None

        value = value.strip().lower()

        if value.startswith("*."):
            value = value[2:]

        value = value.rstrip(".")

        if not value:
            return None

        # Reject URL-like values here. CT name fields should
        # contain hostnames, not complete URLs.
        if "://" in value or "/" in value:
            return None

        if len(value) > 253:
            return None

        labels = value.split(".")

        if len(labels) < 2:
            return None

        label_pattern = re.compile(
            r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
        )

        for label in labels:
            if not label_pattern.fullmatch(label):
                return None

        # A hostname ending in a numeric-only final label is
        # normally an IP-like value rather than a DNS domain.
        if labels[-1].isdigit():
            return None

        return value

    # ============================================================
    # CT RECORD EXTRACTION
    # ============================================================

    @classmethod
    def _extract_names(
        cls,
        record: Any,
    ) -> set[str]:
        names: set[str] = set()

        if not isinstance(record, dict):
            return names

        possible_fields = (
            "name_value",
            "common_name",
            "subject_cn",
            "dns_names",
            "domains",
        )

        for field in possible_fields:
            value = record.get(field)

            if isinstance(value, str):
                values = value.splitlines()

                for item in values:
                    hostname = cls._normalize_hostname(
                        item
                    )

                    if hostname is not None:
                        names.add(hostname)

            elif isinstance(value, list):
                for item in value:
                    hostname = cls._normalize_hostname(
                        item
                    )

                    if hostname is not None:
                        names.add(hostname)

        return names

    # ============================================================
    # DISCOVERY
    # ============================================================

    def discover(
        self,
        context: DomainDiscoveryContext,
    ) -> set[DomainCandidate]:
        query = quote(
            self.query,
            safe=""
        )

        request_url = self.endpoint.format(
            query=query
        )

        response = self.fetcher.fetch(
            request_url
        )

        status = response.get(
            "status",
            0
        )

        if status < 200 or status >= 300:
            return set()

        body = response.get(
            "body",
            b""
        )

        if not body:
            return set()

        try:
            if isinstance(body, bytes):
                body = body.decode(
                    "utf-8",
                    errors="replace"
                )

            payload = json.loads(body)

        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
            TypeError,
        ):
            return set()

        if not isinstance(payload, list):
            return set()

        discovered_at = time()

        candidates: set[DomainCandidate] = set()

        for record in payload:
            for hostname in self._extract_names(
                record
            ):
                candidate = DomainCandidate(
                    hostname=hostname,
                    url=f"https://{hostname}/",
                    source=self.name,
                    evidence=request_url,
                    discovered_at=discovered_at,
                    metadata={
                        "discovery_method":
                            "certificate_transparency",
                        "endpoint":
                            request_url,
                    },
                )

                candidates.add(candidate)

                if len(candidates) >= (
                    self.max_candidates
                ):
                    return candidates

        return candidates
