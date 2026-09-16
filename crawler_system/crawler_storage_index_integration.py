from __future__ import annotations

"""
OUR SEARCH — Crawler → Storage / Index Integration

Phase 7
Brick 7.3

Version:
    crawler-storage-index-integration.v1

Purpose:
    Canonical production handoff from completed crawler results into the
    distributed storage/index integration layer.

Flow:

    WholeWebCrawler
          ↓
    Crawl Result
          ↓
    DiscoveryWorkContract
          ↓
    Storage / Index Integration
          ↓
    Durable Index Handoff
          ↓
    Search Index

This layer coordinates existing components. It does not replace the
crawler, crawler frontier, storage system, or index implementation.
"""

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

from crawler_system.global_discovery_work_contract import (
    CONTRACT_VERSION,
    DiscoveryWorkContract,
    validate_contract,
)


INTEGRATION_VERSION = "crawler-storage-index-integration.v1"


@dataclass(frozen=True)
class CrawlerStorageIndexResult:
    work_id: str
    hostname: str
    url: str
    stored: bool
    indexed: bool
    duplicate: bool = False
    error: Optional[str] = None


class CrawlerStorageIndexIntegration:
    """
    Canonical handoff coordinator between crawler output and the
    distributed storage/index layer.
    """

    def __init__(
        self,
        storage_index: Any,
        storage_root: str = "crawler_storage",
    ):
        self.storage_index = storage_index
        self.storage_root = storage_root

        self._stats = {
            "results_received": 0,
            "contracts_created": 0,
            "contracts_rejected": 0,
            "stored": 0,
            "indexed": 0,
            "duplicates": 0,
            "failures": 0,
            "batches": 0,
            "handoffs": 0,
            "acknowledged": 0,
            "failed_handoffs": 0,
        }

    # ------------------------------------------------------------------
    # VALUE EXTRACTION
    # ------------------------------------------------------------------

    @staticmethod
    def _get(
        value: Any,
        name: str,
        default: Any = None,
    ) -> Any:
        if isinstance(value, Mapping):
            return value.get(name, default)

        return getattr(
            value,
            name,
            default,
        )

    @classmethod
    def _hostname(
        cls,
        result: Any,
    ) -> Optional[str]:
        hostname = cls._get(
            result,
            "hostname",
        )

        if hostname:
            return str(hostname)

        url = cls._get(
            result,
            "url",
        )

        if not url:
            return None

        try:
            from urllib.parse import urlsplit

            return urlsplit(
                str(url)
            ).hostname
        except Exception:
            return None

    @classmethod
    def _url(
        cls,
        result: Any,
    ) -> Optional[str]:
        url = cls._get(
            result,
            "url",
        )

        if url:
            return str(url)

        return None

    @classmethod
    def _source(
        cls,
        result: Any,
    ) -> str:
        source = cls._get(
            result,
            "source",
            "crawler",
        )

        value = str(source).strip().lower()

        return value or "crawler"

    @classmethod
    def _priority(
        cls,
        result: Any,
    ) -> float:
        try:
            return float(
                cls._get(
                    result,
                    "priority",
                    50.0,
                )
            )
        except Exception:
            return 50.0

    @classmethod
    def _metadata(
        cls,
        result: Any,
    ) -> dict[str, Any]:
        metadata = cls._get(
            result,
            "metadata",
        )

        if isinstance(metadata, Mapping):
            return dict(metadata)

        return {}

    # ------------------------------------------------------------------
    # CONTRACT CREATION
    # ------------------------------------------------------------------

    def contract_from_crawl_result(
        self,
        result: Any,
    ) -> DiscoveryWorkContract:
        hostname = self._hostname(result)

        if not hostname:
            raise ValueError(
                "crawler result does not contain a hostname or URL"
            )

        url = self._url(result)

        contract = DiscoveryWorkContract.create(
            hostname=hostname,
            url=url,
            source=self._source(result),
            priority=self._priority(result),
            logical_partition=self._get(
                result,
                "logical_partition",
            ),
            physical_bucket=self._get(
                result,
                "physical_bucket",
            ),
            discovered_at=self._get(
                result,
                "discovered_at",
            ),
            metadata=self._metadata(result),
        )

        validate_contract(contract)

        self._stats[
            "contracts_created"
        ] += 1

        return contract

    # ------------------------------------------------------------------
    # CRAWLER PAYLOAD
    # ------------------------------------------------------------------

    @staticmethod
    def _crawler_payload(
        result: Any,
        contract: DiscoveryWorkContract,
    ) -> dict[str, Any]:
        """
        Preserve crawler-produced information while adding canonical
        contract identity.

        Unknown crawler fields are kept under `crawl_result`.
        """

        if isinstance(result, Mapping):
            crawl_result = dict(result)
        elif hasattr(result, "__dict__"):
            crawl_result = dict(
                vars(result)
            )
        else:
            crawl_result = {
                "value": result
            }

        return {
            "work_id": contract.work_id,
            "hostname": contract.hostname,
            "url": contract.url,
            "source": contract.source,
            "priority": contract.priority,
            "logical_partition": (
                contract.logical_partition
            ),
            "physical_bucket": (
                contract.physical_bucket
            ),
            "discovered_at": contract.discovered_at,
            "metadata": dict(
                contract.metadata
            ),
            "contract_version": CONTRACT_VERSION,
            "crawl_result": crawl_result,
        }

    # ------------------------------------------------------------------
    # STORAGE / INDEX HANDOFF
    # ------------------------------------------------------------------

    def _ingest(
        self,
        payload: dict[str, Any],
    ) -> Any:
        """
        Use the strongest compatible ingestion interface exposed by the
        distributed storage/index integration.

        The bridge prefers batch ingestion and falls back to single-item
        ingestion where supported.
        """

        ingest_candidates = getattr(
            self.storage_index,
            "ingest_candidates",
            None,
        )

        if ingest_candidates is not None:
            return ingest_candidates(
                [payload]
            )

        ingest = getattr(
            self.storage_index,
            "ingest",
            None,
        )

        if ingest is not None:
            return ingest(
                payload
            )

        raise AttributeError(
            "storage/index integration does not expose "
            "ingest_candidates or ingest"
        )

    # ------------------------------------------------------------------
    # SINGLE RESULT
    # ------------------------------------------------------------------

    def process_result(
        self,
        result: Any,
    ) -> CrawlerStorageIndexResult:
        self._stats[
            "results_received"
        ] += 1

        hostname = self._hostname(result) or ""
        url = self._url(result) or ""

        try:
            contract = (
                self.contract_from_crawl_result(
                    result
                )
            )

        except Exception as exc:
            self._stats[
                "contracts_rejected"
            ] += 1

            self._stats[
                "failures"
            ] += 1

            return CrawlerStorageIndexResult(
                work_id=str(
                    self._get(
                        result,
                        "work_id",
                        "",
                    )
                ),
                hostname=hostname,
                url=url,
                stored=False,
                indexed=False,
                error=str(exc),
            )

        payload = self._crawler_payload(
            result,
            contract,
        )

        try:
            ingestion_result = self._ingest(
                payload
            )

            stored = True
            indexed = False
            duplicate = False

            if isinstance(
                ingestion_result,
                Mapping,
            ):
                stored = bool(
                    ingestion_result.get(
                        "stored",
                        ingestion_result.get(
                            "accepted",
                            True,
                        ),
                    )
                )

                indexed = bool(
                    ingestion_result.get(
                        "indexed",
                        False,
                    )
                )

                duplicate = bool(
                    ingestion_result.get(
                        "duplicate",
                        False,
                    )
                )

            elif isinstance(
                ingestion_result,
                bool,
            ):
                stored = ingestion_result

            if duplicate:
                self._stats[
                    "duplicates"
                ] += 1

            if stored:
                self._stats[
                    "stored"
                ] += 1

            if indexed:
                self._stats[
                    "indexed"
                ] += 1

            return CrawlerStorageIndexResult(
                work_id=contract.work_id,
                hostname=contract.hostname,
                url=contract.url,
                stored=stored,
                indexed=indexed,
                duplicate=duplicate,
            )

        except Exception as exc:
            self._stats[
                "failures"
            ] += 1

            return CrawlerStorageIndexResult(
                work_id=contract.work_id,
                hostname=contract.hostname,
                url=contract.url,
                stored=False,
                indexed=False,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # BATCH
    # ------------------------------------------------------------------

    def process_batch(
        self,
        results: Iterable[Any],
    ) -> list[CrawlerStorageIndexResult]:
        materialized = list(results)

        if not materialized:
            return []

        self._stats[
            "batches"
        ] += 1

        contracts: list[
            tuple[
                Any,
                DiscoveryWorkContract,
            ]
        ] = []

        output: list[
            CrawlerStorageIndexResult
        ] = []

        for result in materialized:
            self._stats[
                "results_received"
            ] += 1

            try:
                contract = (
                    self.contract_from_crawl_result(
                        result
                    )
                )

                contracts.append(
                    (
                        result,
                        contract,
                    )
                )

            except Exception as exc:
                self._stats[
                    "contracts_rejected"
                ] += 1

                self._stats[
                    "failures"
                ] += 1

                output.append(
                    CrawlerStorageIndexResult(
                        work_id=str(
                            self._get(
                                result,
                                "work_id",
                                "",
                            )
                        ),
                        hostname=(
                            self._hostname(
                                result
                            )
                            or ""
                        ),
                        url=(
                            self._url(
                                result
                            )
                            or ""
                        ),
                        stored=False,
                        indexed=False,
                        error=str(exc),
                    )
                )

        if not contracts:
            return output

        payloads = [
            self._crawler_payload(
                result,
                contract,
            )
            for result, contract in contracts
        ]

        try:
            ingest_candidates = getattr(
                self.storage_index,
                "ingest_candidates",
                None,
            )

            if ingest_candidates is None:
                for result in materialized:
                    output.append(
                        self.process_result(
                            result
                        )
                    )

                return output

            ingestion_result = (
                ingest_candidates(
                    payloads
                )
            )

            self._stats[
                "handoffs"
            ] += len(payloads)

            accepted_ids: set[str] = set()
            duplicate_ids: set[str] = set()
            indexed_ids: set[str] = set()

            if isinstance(
                ingestion_result,
                Mapping,
            ):
                for key in (
                    "accepted",
                    "stored",
                    "work_ids",
                ):
                    values = ingestion_result.get(
                        key,
                        [],
                    )

                    if isinstance(
                        values,
                        (list, tuple, set),
                    ):
                        accepted_ids.update(
                            str(value)
                            for value in values
                        )

                values = ingestion_result.get(
                    "duplicates",
                    [],
                )

                if isinstance(
                    values,
                    (list, tuple, set),
                ):
                    duplicate_ids.update(
                        str(value)
                        for value in values
                    )

                values = ingestion_result.get(
                    "indexed",
                    [],
                )

                if isinstance(
                    values,
                    (list, tuple, set),
                ):
                    indexed_ids.update(
                        str(value)
                        for value in values
                    )

            for _, contract in contracts:
                duplicate = (
                    contract.work_id
                    in duplicate_ids
                )

                indexed = (
                    contract.work_id
                    in indexed_ids
                )

                stored = (
                    contract.work_id
                    in accepted_ids
                    or not isinstance(
                        ingestion_result,
                        Mapping,
                    )
                )

                if duplicate:
                    self._stats[
                        "duplicates"
                    ] += 1

                if stored:
                    self._stats[
                        "stored"
                    ] += 1

                if indexed:
                    self._stats[
                        "indexed"
                    ] += 1

                output.append(
                    CrawlerStorageIndexResult(
                        work_id=contract.work_id,
                        hostname=contract.hostname,
                        url=contract.url,
                        stored=stored,
                        indexed=indexed,
                        duplicate=duplicate,
                    )
                )

            return output

        except Exception as exc:
            self._stats[
                "failures"
            ] += len(contracts)

            for result, contract in contracts:
                output.append(
                    CrawlerStorageIndexResult(
                        work_id=contract.work_id,
                        hostname=contract.hostname,
                        url=contract.url,
                        stored=False,
                        indexed=False,
                        error=str(exc),
                    )
                )

            return output

    # ------------------------------------------------------------------
    # DURABLE HANDOFF
    # ------------------------------------------------------------------

    def create_handoff_batch(
        self,
        limit: Optional[int] = None,
    ) -> Any:
        method = getattr(
            self.storage_index,
            "create_handoff_batch",
            None,
        )

        if method is None:
            return None

        if limit is None:
            result = method()
        else:
            try:
                result = method(
                    limit=limit
                )
            except TypeError:
                result = method(
                    limit
                )

        if result is not None:
            self._stats[
                "handoffs"
            ] += 1

        return result

    def acknowledge_handoff(
        self,
        batch: Any,
    ) -> Any:
        method = getattr(
            self.storage_index,
            "acknowledge_handoff",
            None,
        )

        if method is None:
            return None

        result = method(batch)

        if result:
            self._stats[
                "acknowledged"
            ] += 1

        return result

    def fail_handoff(
        self,
        batch: Any,
        error: str,
    ) -> Any:
        method = getattr(
            self.storage_index,
            "fail_handoff",
            None,
        )

        if method is None:
            return None

        result = method(
            batch,
            error,
        )

        self._stats[
            "failed_handoffs"
        ] += 1

        return result

    def replay_pending(self) -> Any:
        method = getattr(
            self.storage_index,
            "replay_pending",
            None,
        )

        if method is None:
            return None

        return method()

    # ------------------------------------------------------------------
    # CRAWLER RESULT ENTRYPOINTS
    # ------------------------------------------------------------------

    def accept(
        self,
        result: Any,
    ) -> CrawlerStorageIndexResult:
        return self.process_result(
            result
        )

    def accept_many(
        self,
        results: Iterable[Any],
    ) -> list[CrawlerStorageIndexResult]:
        return self.process_batch(
            results
        )

    # ------------------------------------------------------------------
    # OBSERVABILITY
    # ------------------------------------------------------------------

    def stats(self) -> dict[str, Any]:
        return {
            **self._stats,
            "integration_version": (
                INTEGRATION_VERSION
            ),
            "contract_version": (
                CONTRACT_VERSION
            ),
        }


__all__ = [
    "INTEGRATION_VERSION",
    "CrawlerStorageIndexResult",
    "CrawlerStorageIndexIntegration",
]
