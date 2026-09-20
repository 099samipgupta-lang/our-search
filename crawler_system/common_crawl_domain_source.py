from __future__ import annotations

import os
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import (
    parse_qsl,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)

from crawler_system.common_crawl_partition_scheduler import (
    CommonCrawlPartitionScheduler,
)
from crawler_system.domain_discovery_sources import (
    DomainCandidate,
    DomainDiscoveryContext,
)


class CommonCrawlPartitionProcessor(Protocol):
    """
    Backend responsible for extracting URLs from one Common Crawl
    Parquet partition.

    The scheduler/source does not depend on a particular Parquet
    implementation.
    """

    def process(
        self,
        partition_url: str,
        partition_path: str,
        max_candidates: int,
    ) -> list[str]:
        ...


class DuckDBPartitionProcessor:
    """
    Initial local Parquet backend.

    DuckDB is invoked through its native CLI because the Termux
    environment does not provide the Python duckdb module.

    A partition is downloaded locally, queried, then removed.

    This backend is intentionally isolated from the discovery
    scheduler so a future range-aware Parquet backend can replace it
    without changing CommonCrawlURLIndexSource.
    """

    def __init__(
        self,
        download_root: str | Path = (
            "crawler_system_data/common_crawl/partitions"
        ),
        user_agent: str = "OurSearchBot/1.0",
        timeout_seconds: int = 3600,
    ):
        self.download_root = Path(download_root)
        self.download_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.user_agent = user_agent
        self.timeout_seconds = max(
            60,
            int(timeout_seconds),
        )

    @staticmethod
    def _escape_sql(value: str) -> str:
        return value.replace("'", "''")

    def process(
        self,
        partition_url: str,
        partition_path: str,
        max_candidates: int,
    ) -> list[str]:
        filename = Path(
            partition_path
        ).name

        local_path = (
            self.download_root
            / filename
        )

        max_candidates = max(
            1,
            int(max_candidates),
        )

        try:
            self._download(
                partition_url,
                local_path,
            )

            sql_path = self._escape_sql(
                str(local_path)
            )

            query = f"""
COPY (
    SELECT DISTINCT url
    FROM read_parquet('{sql_path}')
    WHERE
        url IS NOT NULL
        AND url != ''
        AND url_host_registered_domain IS NOT NULL
    LIMIT {max_candidates}
) TO STDOUT (FORMAT CSV, HEADER FALSE);
"""

            result = subprocess.run(
                [
                    "duckdb",
                    "-csv",
                    "-c",
                    query,
                ],
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
            )

            if result.returncode != 0:
                raise RuntimeError(
                    "DuckDB failed: "
                    + (
                        result.stderr.strip()
                        or "unknown error"
                    )
                )

            urls = []

            for line in result.stdout.splitlines():
                url = line.strip()

                if (
                    not url
                    or url == "NULL"
                ):
                    continue

                urls.append(url)

                if len(urls) >= max_candidates:
                    break

            return urls

        finally:
            try:
                local_path.unlink(
                    missing_ok=True
                )
            except OSError:
                pass

    def _download(
        self,
        url: str,
        destination: Path,
    ) -> None:
        destination.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        temporary = destination.with_suffix(
            destination.suffix + ".part"
        )

        try:
            result = subprocess.run(
                [
                    "curl",
                    "-L",
                    "--fail",
                    "--retry",
                    "3",
                    "--retry-delay",
                    "5",
                    "-A",
                    self.user_agent,
                    "-o",
                    str(temporary),
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
            )

            if result.returncode != 0:
                raise RuntimeError(
                    "Common Crawl download failed: "
                    + (
                        result.stderr.strip()
                        or "unknown error"
                    )
                )

            if (
                not temporary.exists()
                or temporary.stat().st_size == 0
            ):
                raise RuntimeError(
                    "Common Crawl download produced "
                    "an empty partition"
                )

            temporary.replace(destination)

        finally:
            try:
                temporary.unlink(
                    missing_ok=True
                )
            except OSError:
                pass


class CommonCrawlURLIndexSource:
    """
    Common Crawl URL Index discovery source.

    Common Crawl is used only to discover public URLs/domains.
    OUR SEARCH fetches discovered URLs itself.

    Persistent partition scheduling is handled by
    CommonCrawlPartitionScheduler.
    """

    name = "common_crawl_url_index"

    PATHS_URL_TEMPLATE = (
        "https://data.commoncrawl.org/"
        "crawl-data/{crawl}/"
        "cc-index-table.paths.gz"
    )

    DATA_ROOT = (
        "crawler_system_data/common_crawl"
    )

    def __init__(
        self,
        scheduler: CommonCrawlPartitionScheduler | None = None,
        processor: CommonCrawlPartitionProcessor | None = None,
        crawl: str = "CC-MAIN-2025-51",
        subset: str = "warc",
        max_candidates: int = 1000,
        max_per_domain: int = 25,
        paths_fetcher: Any | None = None,
        worker_id: str = "common-crawl-worker",
    ):
        self.crawl = crawl
        self.subset = subset

        self.max_candidates = max(
            1,
            int(max_candidates),
        )

        self.max_per_domain = max(
            1,
            int(max_per_domain),
        )

        self.worker_id = (
            worker_id
            or "common-crawl-worker"
        )

        self.scheduler = (
            scheduler
            if scheduler is not None
            else CommonCrawlPartitionScheduler(
                crawl=self.crawl,
                subset=self.subset,
                state_root=(
                    Path(self.DATA_ROOT)
                    / "scheduler"
                ),
            )
        )

        self.processor = (
            processor
            if processor is not None
            else DuckDBPartitionProcessor(
                download_root=(
                    Path(self.DATA_ROOT)
                    / "partitions"
                )
            )
        )

        self.paths_fetcher = paths_fetcher

        self._inventory_loaded = bool(
            self.scheduler.state.partitions
        )

    # ------------------------------------------------------------------
    # DomainDiscoverySource interface
    # ------------------------------------------------------------------

    def can_discover(
        self,
        context: DomainDiscoveryContext,
    ) -> bool:
        return True

    def discover(
        self,
        context: DomainDiscoveryContext,
    ) -> set[DomainCandidate]:
        self._ensure_inventory()

        partition = self.scheduler.lease_next(
            self.worker_id
        )

        if partition is None:
            return set()

        lease_id = partition.lease_id

        if not lease_id:
            return set()

        partition_url = (
            self.scheduler.partition_url(
                partition
            )
        )

        try:
            urls = self.processor.process(
                partition_url=partition_url,
                partition_path=partition.path,
                max_candidates=self.max_candidates,
            )

            candidates = set()
            seen_urls = set()
            domain_counts: dict[str, int] = {}

            for url in urls:
                normalized_url = self._normalize_url(url)

                if normalized_url is None:
                    continue

                if normalized_url in seen_urls:
                    continue

                hostname = self._hostname(
                    normalized_url
                )

                if hostname is None:
                    continue

                if (
                    domain_counts.get(hostname, 0)
                    >= self.max_per_domain
                ):
                    continue

                candidate = self._candidate_from_url(
                    url=normalized_url,
                    partition=partition.path,
                )

                if candidate is None:
                    continue

                seen_urls.add(normalized_url)
                domain_counts[hostname] = (
                    domain_counts.get(hostname, 0) + 1
                )
                candidates.add(candidate)

                if len(candidates) >= self.max_candidates:
                    break

            self.scheduler.complete(
                partition.path,
                lease_id,
            )

            return candidates

        except Exception as exc:
            self.scheduler.fail(
                partition.path,
                lease_id,
                f"{type(exc).__name__}: {exc}",
            )

            raise

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    def _ensure_inventory(self) -> None:
        if self._inventory_loaded:
            return

        paths_url = self.PATHS_URL_TEMPLATE.format(
            crawl=self.crawl
        )

        paths_bytes = self._fetch_paths(
            paths_url
        )

        count = (
            self.scheduler
            .inventory_from_paths_bytes(
                paths_bytes
            )
        )

        if count <= 0:
            raise RuntimeError(
                "Common Crawl URL Index returned "
                "no partitions for "
                f"{self.crawl}/{self.subset}"
            )

        self._inventory_loaded = True

    def _fetch_paths(
        self,
        url: str,
    ) -> bytes:
        if self.paths_fetcher is not None:
            response = self.paths_fetcher.fetch(
                url
            )

            status = response.get(
                "status",
                0,
            )

            if status < 200 or status >= 300:
                raise RuntimeError(
                    "Common Crawl paths request failed: "
                    f"HTTP {status}"
                )

            body = response.get(
                "body",
                b"",
            )

            if isinstance(body, str):
                return body.encode(
                    "utf-8"
                )

            if isinstance(body, bytes):
                return body

            raise RuntimeError(
                "Common Crawl paths response "
                "contained no usable body"
            )

        result = subprocess.run(
            [
                "curl",
                "-L",
                "--fail",
                "--retry",
                "3",
                "--retry-delay",
                "5",
                "-A",
                "OurSearchBot/1.0",
                url,
            ],
            capture_output=True,
            timeout=300,
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(
                "Common Crawl paths download failed: "
                + (
                    result.stderr.decode(
                        "utf-8",
                        errors="replace",
                    ).strip()
                    or "unknown error"
                )
            )

        if not result.stdout:
            raise RuntimeError(
                "Common Crawl paths response was empty"
            )

        return bytes(result.stdout)

    # ------------------------------------------------------------------
    # URL normalization
    # ------------------------------------------------------------------

    TRACKING_QUERY_PREFIXES = (
        "utm_",
        "mc_",
        "fbclid",
        "gclid",
        "dclid",
        "msclkid",
        "ref_",
        "ref",
    )

    @classmethod
    def _normalize_url(
        cls,
        url: str,
    ) -> str | None:
        if not isinstance(url, str):
            return None

        url = url.strip()

        if not url.startswith(
            (
                "http://",
                "https://",
            )
        ):
            return None

        try:
            parsed = urlsplit(url)
        except ValueError:
            return None

        if not parsed.hostname:
            return None

        scheme = parsed.scheme.lower()

        if scheme not in (
            "http",
            "https",
        ):
            return None

        hostname = parsed.hostname.lower().rstrip(".")

        if "." not in hostname:
            return None

        # Preserve an explicit non-default port.
        netloc = hostname

        if parsed.port is not None:
            default_port = (
                (scheme == "http" and parsed.port == 80)
                or
                (scheme == "https" and parsed.port == 443)
            )

            if not default_port:
                netloc = f"{netloc}:{parsed.port}"

        # Fragments are client-side and do not identify
        # a separately fetchable web resource.
        path = parsed.path or "/"

        # Normalize repeated trailing slashes while preserving
        # the root path.
        if path != "/":
            path = path.rstrip("/") or "/"

        query_items = parse_qsl(
            parsed.query,
            keep_blank_values=True,
        )

        filtered_query = []

        for key, value in query_items:
            key_lower = key.casefold()

            if any(
                key_lower == prefix
                or key_lower.startswith(prefix)
                for prefix in cls.TRACKING_QUERY_PREFIXES
            ):
                continue

            filtered_query.append(
                (key, value)
            )

        query = urlencode(
            filtered_query,
            doseq=True,
        )

        return urlunsplit(
            (
                scheme,
                netloc,
                path,
                query,
                "",
            )
        )

    # ------------------------------------------------------------------
    # Candidate conversion
    # ------------------------------------------------------------------

    @staticmethod
    def _hostname(
        url: str,
    ) -> str | None:
        from urllib.parse import urlsplit

        try:
            hostname = urlsplit(
                url
            ).hostname
        except ValueError:
            return None

        if not hostname:
            return None

        hostname = hostname.lower().rstrip(".")

        if "." not in hostname:
            return None

        return hostname

    def _candidate_from_url(
        self,
        url: str,
        partition: str,
    ) -> DomainCandidate | None:
        if not isinstance(url, str):
            return None

        url = url.strip()

        if not url.startswith(
            (
                "http://",
                "https://",
            )
        ):
            return None

        hostname = self._hostname(
            url
        )

        if hostname is None:
            return None

        return DomainCandidate(
            hostname=hostname,
            url=url,
            source=self.name,
            evidence=(
                f"{self.crawl}:{partition}"
            ),
            discovered_at=time.time(),
            metadata={
                "source_url": url,
                "collection": self.crawl,
                "discovery_type": (
                    "common_crawl_url_index"
                ),
                "seed_url": url,
                "partition": partition,
            },
        )
