from __future__ import annotations

import gzip
import json
import os
import tempfile
import time

import fcntl
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin


@dataclass
class PartitionState:
    path: str
    partition_id: int
    status: str = "pending"
    attempts: int = 0
    lease_id: str | None = None
    lease_until: float | None = None
    completed_at: float | None = None
    last_error: str | None = None


@dataclass
class SchedulerState:
    crawl: str
    subset: str
    updated_at: float
    partitions: dict[str, PartitionState] = field(default_factory=dict)


class CommonCrawlPartitionScheduler:
    """
    Durable scheduler for Common Crawl URL-Index Parquet partitions.

    Responsibilities:
      - obtain the official .paths.gz inventory
      - persist every partition
      - lease work safely
      - recover expired leases after crashes
      - retry failed partitions
      - remember completed partitions
      - resume from disk after restart

    This class does NOT decode Parquet and does NOT modify
    the OUR SEARCH crawler.
    """

    PATHS_TEMPLATE = (
        "https://data.commoncrawl.org/"
        "crawl-data/{crawl}/cc-index-table.paths.gz"
    )

    DEFAULT_STATE_ROOT = "crawler_system_data/common_crawl"

    def __init__(
        self,
        crawl: str,
        subset: str = "warc",
        state_root: str | Path | None = None,
        lease_seconds: int = 1800,
        max_attempts: int = 5,
    ):
        self.crawl = crawl
        self.subset = subset

        self.state_root = Path(
            state_root
            if state_root is not None
            else self.DEFAULT_STATE_ROOT
        )

        self.state_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.state_file = (
            self.state_root
            / f"{self.crawl}_{self.subset}_state.json"
        )

        self.lock_file = (
            self.state_root
            / f"{self.crawl}_{self.subset}_state.lock"
        )

        self.lease_seconds = max(
            60,
            int(lease_seconds),
        )

        self.max_attempts = max(
            1,
            int(max_attempts),
        )

        self.state = self._load_state()

    # ------------------------------------------------------------------
    # Persistent state
    # ------------------------------------------------------------------

    @contextmanager
    def _state_lock(self):
        """
        Acquire an OS-level exclusive lock for state mutations.

        This protects the scheduler when multiple crawler processes
        share the same state directory.
        """
        handle = self.lock_file.open(
            "a+",
            encoding="utf-8",
        )

        try:
            fcntl.flock(
                handle.fileno(),
                fcntl.LOCK_EX,
            )
            yield
        finally:
            fcntl.flock(
                handle.fileno(),
                fcntl.LOCK_UN,
            )
            handle.close()

    def _load_state(self) -> SchedulerState:
        if not self.state_file.exists():
            return SchedulerState(
                crawl=self.crawl,
                subset=self.subset,
                updated_at=time.time(),
            )

        with self.state_file.open(
            "r",
            encoding="utf-8",
        ) as handle:
            raw = json.load(handle)

        partitions = {}

        for key, value in raw.get(
            "partitions",
            {},
        ).items():
            partitions[key] = PartitionState(
                **value
            )

        return SchedulerState(
            crawl=raw["crawl"],
            subset=raw["subset"],
            updated_at=float(
                raw.get(
                    "updated_at",
                    time.time(),
                )
            ),
            partitions=partitions,
        )

    def _save_state(self) -> None:
        self.state.updated_at = time.time()

        payload = {
            "crawl": self.state.crawl,
            "subset": self.state.subset,
            "updated_at": self.state.updated_at,
            "partitions": {
                key: asdict(value)
                for key, value
                in self.state.partitions.items()
            },
        }

        self.state_file.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        fd, temporary = tempfile.mkstemp(
            prefix=".common-crawl-state-",
            suffix=".tmp",
            dir=str(self.state_file.parent),
        )

        try:
            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as handle:
                json.dump(
                    payload,
                    handle,
                    indent=2,
                    sort_keys=True,
                )
                handle.flush()
                os.fsync(handle.fileno())

            os.replace(
                temporary,
                self.state_file,
            )

        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    # ------------------------------------------------------------------
    # Inventory
    # ------------------------------------------------------------------

    def inventory_from_paths_bytes(
        self,
        paths_gzip: bytes,
    ) -> int:
        """
        Load the official Common Crawl .paths.gz inventory.

        Only paths belonging to this crawl/subset are retained.
        Existing state for matching partitions is preserved.
        """

        text = gzip.decompress(
            paths_gzip
        ).decode(
            "utf-8",
            errors="replace",
        )

        prefix = (
            "cc-index/table/cc-main/warc/"
            f"crawl={self.crawl}/"
            f"subset={self.subset}/"
        )

        with self._state_lock():
            # Reload while holding the process-wide state lock so
            # another process cannot overwrite a newer state snapshot.
            self.state = self._load_state()

            discovered: dict[str, PartitionState] = {}

            for raw_line in text.splitlines():
                path = raw_line.strip()

                if not path.startswith(prefix):
                    continue

                filename = path.rsplit(
                    "/",
                    1,
                )[-1]

                if not filename.startswith("part-"):
                    continue

                try:
                    partition_text = filename.split(
                        "-",
                        2,
                    )[1]

                    partition_id = int(
                        partition_text
                    )

                except (
                    IndexError,
                    ValueError,
                ):
                    continue

                existing = self.state.partitions.get(
                    path
                )

                if existing is None:
                    discovered[path] = PartitionState(
                        path=path,
                        partition_id=partition_id,
                    )
                else:
                    discovered[path] = existing

            self.state.partitions = discovered

            self._save_state()

            return len(discovered)

    def partition_url(
        self,
        partition: PartitionState,
    ) -> str:
        return urljoin(
            "https://data.commoncrawl.org/",
            partition.path,
        )

    # ------------------------------------------------------------------
    # Lease management
    # ------------------------------------------------------------------

    def _recover_expired_leases(self) -> None:
        now = time.time()
        changed = False

        for partition in self.state.partitions.values():
            if (
                partition.status == "leased"
                and partition.lease_until is not None
                and partition.lease_until <= now
            ):
                partition.status = "pending"
                partition.lease_id = None
                partition.lease_until = None
                changed = True

        if changed:
            self._save_state()

    def lease_next(
        self,
        worker_id: str,
    ) -> PartitionState | None:
        """
        Atomically lease the lowest-numbered eligible partition.

        The complete read/modify/write sequence is protected by an
        OS-level lock, making this safe across processes.
        """

        with self._state_lock():
            self.state = self._load_state()

            self._recover_expired_leases()

            now = time.time()

            candidates = [
                partition
                for partition in self.state.partitions.values()
                if partition.status == "pending"
                and partition.attempts < self.max_attempts
            ]

            if not candidates:
                return None

            partition = min(
                candidates,
                key=lambda item: item.partition_id,
            )

            partition.status = "leased"
            partition.attempts += 1
            partition.lease_id = (
                f"{worker_id}:{partition.partition_id}:"
                f"{int(now * 1000000)}"
            )
            partition.lease_until = (
                now + self.lease_seconds
            )
            partition.last_error = None

            self._save_state()

            return partition

    def renew_lease(
        self,
        partition_path: str,
        lease_id: str,
    ) -> bool:
        with self._state_lock():
            self.state = self._load_state()

            partition = self.state.partitions.get(
                partition_path
            )

            if partition is None:
                return False

            if (
                partition.status != "leased"
                or partition.lease_id != lease_id
            ):
                return False

            partition.lease_until = (
                time.time()
                + self.lease_seconds
            )

            self._save_state()

            return True

    def complete(
        self,
        partition_path: str,
        lease_id: str,
    ) -> bool:
        with self._state_lock():
            self.state = self._load_state()

            partition = self.state.partitions.get(
                partition_path
            )

            if partition is None:
                return False

            if (
                partition.status != "leased"
                or partition.lease_id != lease_id
            ):
                return False

            partition.status = "completed"
            partition.completed_at = time.time()
            partition.lease_id = None
            partition.lease_until = None
            partition.last_error = None

            self._save_state()

            return True

    def fail(
        self,
        partition_path: str,
        lease_id: str,
        error: str,
    ) -> bool:
        with self._state_lock():
            self.state = self._load_state()

            partition = self.state.partitions.get(
                partition_path
            )

            if partition is None:
                return False

            if (
                partition.status != "leased"
                or partition.lease_id != lease_id
            ):
                return False

            partition.lease_id = None
            partition.lease_until = None
            partition.last_error = str(error)

            if partition.attempts >= self.max_attempts:
                partition.status = "failed"
            else:
                partition.status = "pending"

            self._save_state()

            return True

    def stats(self) -> dict[str, int]:
        with self._state_lock():
            self.state = self._load_state()

            counts = {
                "total": 0,
                "pending": 0,
                "leased": 0,
                "completed": 0,
                "failed": 0,
            }

            for partition in self.state.partitions.values():
                counts["total"] += 1

                if partition.status in counts:
                    counts[partition.status] += 1

            return counts
