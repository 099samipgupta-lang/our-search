from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, Optional


FABRIC_VERSION = "global-domain-discovery.v4"


@dataclass(frozen=True)
class DomainDiscoveryRecord:
    hostname: str
    url: str
    source: str
    evidence: Optional[str] = None
    discovered_at: Optional[float] = None
    metadata: Optional[dict[str, Any]] = None
    priority: float = 50.0


@dataclass(frozen=True)
class ClaimedDomainDiscovery:
    hostname: str
    url: str
    source: str
    evidence: Optional[str]
    discovered_at: float
    metadata: dict[str, Any]
    priority: float
    attempts: int
    shard_id: int


class DomainDiscoveryPartitioner:
    """
    Stable logical partitioning.

    A hostname is assigned to a logical partition using SHA-256.
    Logical partition identity never depends on the number of physical
    buckets or workers.

    Physical placement is handled separately by DomainDiscoveryPlacementMap.
    """

    DEFAULT_LOGICAL_PARTITIONS = 1_048_576

    def __init__(
        self,
        shard_count: int,
        logical_partition_count: int = DEFAULT_LOGICAL_PARTITIONS,
    ):
        shard_count = int(shard_count)
        logical_partition_count = int(
            logical_partition_count
        )

        if shard_count <= 0:
            raise ValueError(
                "shard_count must be positive"
            )

        if logical_partition_count <= 0:
            raise ValueError(
                "logical_partition_count must be positive"
            )

        self.shard_count = shard_count
        self.logical_partition_count = (
            logical_partition_count
        )

    @staticmethod
    def normalize_hostname(
        hostname: str,
    ) -> str:
        return (
            str(hostname)
            .strip()
            .lower()
            .rstrip(".")
        )

    @staticmethod
    def _digest(
        hostname: str,
    ) -> bytes:
        return hashlib.sha256(
            hostname.encode("utf-8")
        ).digest()

    def logical_partition(
        self,
        hostname: str,
    ) -> int:
        normalized = self.normalize_hostname(
            hostname
        )

        if not normalized:
            raise ValueError(
                "hostname must not be empty"
            )

        digest = self._digest(normalized)

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return (
            value
            % self.logical_partition_count
        )

    def partition(
        self,
        hostname: str,
    ) -> int:
        """
        Compatibility method.

        New callers should use a placement map for physical routing.
        This method provides deterministic fallback routing only.
        """

        logical = self.logical_partition(
            hostname
        )

        return logical % self.shard_count


class DomainDiscoveryPlacementMap:
    """
    Durable logical-partition -> physical-bucket placement map.

    The logical namespace and the physical storage namespace are
    deliberately separated.

    Logical partition:
        stable identity derived only from hostname.

    Physical bucket:
        mutable placement target.

    Therefore increasing physical capacity does not change the logical
    identity of already-discovered domains.

    The placement map itself is durable SQLite metadata. It is a routing
    control-plane artifact, not the domain workload database.
    """

    DEFAULT_PLACEMENT_BUCKETS = 4096

    def __init__(
        self,
        storage_root: str,
        logical_partition_count: int,
        physical_bucket_count: int,
    ):
        self.storage_root = str(
            storage_root
        ).rstrip("/")

        if not self.storage_root:
            raise ValueError(
                "storage_root must not be empty"
            )

        self.logical_partition_count = int(
            logical_partition_count
        )

        self.physical_bucket_count = int(
            physical_bucket_count
        )

        if self.logical_partition_count <= 0:
            raise ValueError(
                "logical_partition_count must be positive"
            )

        if self.physical_bucket_count <= 0:
            raise ValueError(
                "physical_bucket_count must be positive"
            )

        os.makedirs(
            self.storage_root,
            exist_ok=True,
        )

        self.database_path = os.path.join(
            self.storage_root,
            "placement_map.db",
        )

        self._lock = threading.RLock()

        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
        )

        connection.row_factory = sqlite3.Row

        connection.execute(
            "PRAGMA journal_mode=WAL"
        )

        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )

        connection.execute(
            "PRAGMA busy_timeout=30000"
        )

        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS placement_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS partition_placements (
                    logical_partition INTEGER PRIMARY KEY,
                    physical_bucket INTEGER NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 1,
                    updated_at REAL NOT NULL
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_partition_placements_bucket
                ON partition_placements(physical_bucket)
                """
            )

            connection.commit()

            self._ensure_metadata(
                connection
            )

    def _ensure_metadata(
        self,
        connection: sqlite3.Connection,
    ) -> None:
        existing = connection.execute(
            """
            SELECT value
            FROM placement_metadata
            WHERE key = ?
            """,
            (
                "logical_partition_count",
            ),
        ).fetchone()

        if existing is None:
            connection.execute(
                """
                INSERT INTO placement_metadata
                    (key, value)
                VALUES
                    (?, ?)
                """,
                (
                    "logical_partition_count",
                    str(
                        self.logical_partition_count
                    ),
                ),
            )

        existing = connection.execute(
            """
            SELECT value
            FROM placement_metadata
            WHERE key = ?
            """,
            (
                "physical_bucket_count",
            ),
        ).fetchone()

        if existing is None:
            connection.execute(
                """
                INSERT INTO placement_metadata
                    (key, value)
                VALUES
                    (?, ?)
                """,
                (
                    "physical_bucket_count",
                    str(
                        self.physical_bucket_count
                    ),
                ),
            )

        connection.commit()

    def _validate_logical_partition(
        self,
        logical_partition: int,
    ) -> int:
        logical_partition = int(
            logical_partition
        )

        if not (
            0
            <= logical_partition
            < self.logical_partition_count
        ):
            raise ValueError(
                "logical_partition is outside the "
                "configured logical namespace"
            )

        return logical_partition

    def _validate_physical_bucket(
        self,
        physical_bucket: int,
    ) -> int:
        physical_bucket = int(
            physical_bucket
        )

        if not (
            0
            <= physical_bucket
            < self.physical_bucket_count
        ):
            raise ValueError(
                "physical_bucket is outside the "
                "configured physical namespace"
            )

        return physical_bucket

    def _initial_bucket(
        self,
        logical_partition: int,
    ) -> int:
        """
        Deterministic initial placement.

        This is used only when a logical partition receives its first
        durable placement entry.

        Once persisted, the placement is authoritative and does not
        change merely because the process restarts.
        """

        digest = hashlib.sha256(
            str(logical_partition).encode(
                "ascii"
            )
        ).digest()

        value = int.from_bytes(
            digest[:8],
            byteorder="big",
            signed=False,
        )

        return (
            value
            % self.physical_bucket_count
        )

    def get(
        self,
        logical_partition: int,
    ) -> Optional[int]:
        logical_partition = (
            self._validate_logical_partition(
                logical_partition
            )
        )

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT physical_bucket
                    FROM partition_placements
                    WHERE logical_partition = ?
                    """,
                    (
                        logical_partition,
                    ),
                ).fetchone()

        if row is None:
            return None

        return int(
            row["physical_bucket"]
        )

    def get_or_create(
        self,
        logical_partition: int,
    ) -> int:
        logical_partition = (
            self._validate_logical_partition(
                logical_partition
            )
        )

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT physical_bucket
                    FROM partition_placements
                    WHERE logical_partition = ?
                    """,
                    (
                        logical_partition,
                    ),
                ).fetchone()

                if row is not None:
                    return int(
                        row["physical_bucket"]
                    )

                bucket = self._initial_bucket(
                    logical_partition
                )

                now = time.time()

                connection.execute(
                    """
                    INSERT INTO partition_placements (
                        logical_partition,
                        physical_bucket,
                        generation,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        logical_partition,
                        bucket,
                        1,
                        now,
                    ),
                )

                connection.commit()

                return bucket

    def assign(
        self,
        logical_partition: int,
        physical_bucket: int,
    ) -> bool:
        logical_partition = (
            self._validate_logical_partition(
                logical_partition
            )
        )

        physical_bucket = (
            self._validate_physical_bucket(
                physical_bucket
            )
        )

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT generation
                    FROM partition_placements
                    WHERE logical_partition = ?
                    """,
                    (
                        logical_partition,
                    ),
                ).fetchone()

                now = time.time()

                if row is None:
                    connection.execute(
                        """
                        INSERT INTO partition_placements (
                            logical_partition,
                            physical_bucket,
                            generation,
                            updated_at
                        )
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            logical_partition,
                            physical_bucket,
                            1,
                            now,
                        ),
                    )
                else:
                    generation = (
                        int(row["generation"])
                        + 1
                    )

                    connection.execute(
                        """
                        UPDATE partition_placements
                        SET
                            physical_bucket = ?,
                            generation = ?,
                            updated_at = ?
                        WHERE logical_partition = ?
                        """,
                        (
                            physical_bucket,
                            generation,
                            now,
                            logical_partition,
                        ),
                    )

                connection.commit()

        return True

    def assign_many(
        self,
        placements: Iterable[
            tuple[int, int]
        ],
    ) -> int:
        items = list(
            placements
        )

        if not items:
            return 0

        with self._lock:
            with self._connect() as connection:
                now = time.time()

                for logical_partition, physical_bucket in items:
                    logical_partition = (
                        self._validate_logical_partition(
                            logical_partition
                        )
                    )

                    physical_bucket = (
                        self._validate_physical_bucket(
                            physical_bucket
                        )
                    )

                    row = connection.execute(
                        """
                        SELECT generation
                        FROM partition_placements
                        WHERE logical_partition = ?
                        """,
                        (
                            logical_partition,
                        ),
                    ).fetchone()

                    if row is None:
                        connection.execute(
                            """
                            INSERT INTO partition_placements (
                                logical_partition,
                                physical_bucket,
                                generation,
                                updated_at
                            )
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                logical_partition,
                                physical_bucket,
                                1,
                                now,
                            ),
                        )
                    else:
                        generation = (
                            int(
                                row["generation"]
                            )
                            + 1
                        )

                        connection.execute(
                            """
                            UPDATE partition_placements
                            SET
                                physical_bucket = ?,
                                generation = ?,
                                updated_at = ?
                            WHERE logical_partition = ?
                            """,
                            (
                                physical_bucket,
                                generation,
                                now,
                                logical_partition,
                            ),
                        )

                connection.commit()

        return len(items)

    def physical_bucket_for_hostname(
        self,
        hostname: str,
        partitioner: DomainDiscoveryPartitioner,
    ) -> int:
        logical_partition = (
            partitioner.logical_partition(
                hostname
            )
        )

        return self.get_or_create(
            logical_partition
        )

    def list_for_bucket(
        self,
        physical_bucket: int,
    ) -> list[int]:
        physical_bucket = (
            self._validate_physical_bucket(
                physical_bucket
            )
        )

        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT logical_partition
                    FROM partition_placements
                    WHERE physical_bucket = ?
                    ORDER BY logical_partition
                    """,
                    (
                        physical_bucket,
                    ),
                ).fetchall()

        return [
            int(row["logical_partition"])
            for row in rows
        ]

    def active_buckets(
        self,
    ) -> list[int]:
        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT DISTINCT physical_bucket
                    FROM partition_placements
                    ORDER BY physical_bucket
                    """
                ).fetchall()

        return [
            int(row["physical_bucket"])
            for row in rows
        ]

    def stats(self) -> dict[str, int]:
        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT
                        COUNT(*) AS assignments,
                        COUNT(DISTINCT physical_bucket)
                            AS active_buckets
                    FROM partition_placements
                    """
                ).fetchone()

        return {
            "logical_partition_count": (
                self.logical_partition_count
            ),
            "physical_bucket_count": (
                self.physical_bucket_count
            ),
            "assignments": int(
                row["assignments"]
            ),
            "active_buckets": int(
                row["active_buckets"]
            ),
        }


class DomainDiscoveryShard:
    """
    Durable workload database for one physical bucket.

    The shard ID is now a physical placement identifier rather than the
    identity of a domain.
    """

    def __init__(
        self,
        database_path: str,
        shard_id: int,
        lease_timeout: float,
    ):
        self.database_path = str(
            database_path
        )

        self.shard_id = int(
            shard_id
        )

        self.lease_timeout = float(
            lease_timeout
        )

        if self.shard_id < 0:
            raise ValueError(
                "shard_id must not be negative"
            )

        if self.lease_timeout <= 0:
            raise ValueError(
                "lease_timeout must be positive"
            )

        parent = os.path.dirname(
            self.database_path
        )

        if parent:
            os.makedirs(
                parent,
                exist_ok=True,
            )

        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
        )

        connection.row_factory = sqlite3.Row

        connection.execute(
            "PRAGMA journal_mode=WAL"
        )

        connection.execute(
            "PRAGMA synchronous=NORMAL"
        )

        connection.execute(
            "PRAGMA busy_timeout=30000"
        )

        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS
                domain_discovery_work (
                    hostname TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    source TEXT NOT NULL,
                    evidence TEXT,
                    discovered_at REAL NOT NULL,
                    metadata_json TEXT,
                    priority REAL NOT NULL DEFAULT 50.0,
                    status TEXT NOT NULL DEFAULT 'queued',
                    lease_owner TEXT,
                    leased_at REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    completed_at REAL,
                    failed_at REAL,
                    last_error TEXT
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_domain_discovery_ready
                ON domain_discovery_work(
                    status,
                    priority DESC,
                    discovered_at
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_domain_discovery_lease
                ON domain_discovery_work(
                    status,
                    leased_at
                )
                """
            )

            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_domain_discovery_source
                ON domain_discovery_work(
                    source,
                    status
                )
                """
            )

            connection.commit()

    @staticmethod
    def _record_values(
        record: DomainDiscoveryRecord,
    ) -> tuple:
        hostname = (
            str(record.hostname)
            .strip()
            .lower()
            .rstrip(".")
        )

        url = str(
            record.url
        ).strip()

        source = str(
            record.source
        ).strip()

        if not hostname:
            raise ValueError(
                "record hostname must not be empty"
            )

        if not url:
            raise ValueError(
                "record url must not be empty"
            )

        if not source:
            raise ValueError(
                "record source must not be empty"
            )

        discovered_at = (
            float(record.discovered_at)
            if record.discovered_at is not None
            else time.time()
        )

        metadata_json = json.dumps(
            record.metadata
            if isinstance(
                record.metadata,
                dict,
            )
            else {},
            sort_keys=True,
            separators=(",", ":"),
        )

        return (
            hostname,
            url,
            source,
            record.evidence,
            discovered_at,
            metadata_json,
            float(record.priority),
        )

    def enqueue_many(
        self,
        records: Iterable[
            DomainDiscoveryRecord
        ],
    ) -> dict[str, int]:
        records = list(records)

        if not records:
            return {
                "inserted": 0,
                "duplicates": 0,
                "invalid": 0,
            }

        inserted = 0
        duplicates = 0
        invalid = 0

        with self._connect() as connection:
            for record in records:
                try:
                    values = self._record_values(
                        record
                    )
                except Exception:
                    invalid += 1
                    continue

                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO
                    domain_discovery_work (
                        hostname,
                        url,
                        source,
                        evidence,
                        discovered_at,
                        metadata_json,
                        priority,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')
                    """,
                    values,
                )

                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    duplicates += 1

            connection.commit()

        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "invalid": invalid,
        }

    def claim_many(
        self,
        limit: int,
        lease_owner: str,
    ) -> list[ClaimedDomainDiscovery]:
        limit = max(
            1,
            int(limit),
        )

        lease_owner = str(
            lease_owner
        )

        now = time.time()

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM domain_discovery_work
                WHERE status = 'queued'
                ORDER BY
                    priority DESC,
                    discovered_at ASC,
                    hostname ASC
                LIMIT ?
                """,
                (
                    limit,
                ),
            ).fetchall()

            if not rows:
                return []

            claimed = []

            for row in rows:
                hostname = row["hostname"]

                cursor = connection.execute(
                    """
                    UPDATE domain_discovery_work
                    SET
                        status = 'processing',
                        lease_owner = ?,
                        leased_at = ?,
                        attempts = attempts + 1
                    WHERE
                        hostname = ?
                        AND status = 'queued'
                    """,
                    (
                        lease_owner,
                        now,
                        hostname,
                    ),
                )

                if cursor.rowcount != 1:
                    continue

                metadata = {}

                try:
                    decoded = json.loads(
                        row["metadata_json"]
                        or "{}"
                    )

                    if isinstance(
                        decoded,
                        dict,
                    ):
                        metadata = decoded
                except Exception:
                    metadata = {}

                claimed.append(
                    ClaimedDomainDiscovery(
                        hostname=hostname,
                        url=row["url"],
                        source=row["source"],
                        evidence=row["evidence"],
                        discovered_at=float(
                            row["discovered_at"]
                        ),
                        metadata=metadata,
                        priority=float(
                            row["priority"]
                        ),
                        attempts=int(
                            row["attempts"]
                        )
                        + 1,
                        shard_id=self.shard_id,
                    )
                )

            connection.commit()

        return claimed

    def mark_complete(
        self,
        item: ClaimedDomainDiscovery,
        lease_owner: str,
    ) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE domain_discovery_work
                SET
                    status = 'complete',
                    lease_owner = NULL,
                    leased_at = NULL,
                    completed_at = ?,
                    last_error = NULL
                WHERE
                    hostname = ?
                    AND status = 'processing'
                    AND lease_owner = ?
                """,
                (
                    time.time(),
                    item.hostname,
                    lease_owner,
                ),
            )

            connection.commit()

        return cursor.rowcount == 1

    def mark_failed(
        self,
        item: ClaimedDomainDiscovery,
        error: str,
        retry: bool = True,
        lease_owner: Optional[str] = None,
    ) -> bool:
        status = (
            "queued"
            if retry
            else "failed"
        )

        with self._connect() as connection:
            if lease_owner is None:
                cursor = connection.execute(
                    """
                    UPDATE domain_discovery_work
                    SET
                        status = ?,
                        lease_owner = NULL,
                        leased_at = NULL,
                        failed_at = ?,
                        last_error = ?
                    WHERE
                        hostname = ?
                        AND status = 'processing'
                    """,
                    (
                        status,
                        time.time(),
                        str(error),
                        item.hostname,
                    ),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE domain_discovery_work
                    SET
                        status = ?,
                        lease_owner = NULL,
                        leased_at = NULL,
                        failed_at = ?,
                        last_error = ?
                    WHERE
                        hostname = ?
                        AND status = 'processing'
                        AND lease_owner = ?
                    """,
                    (
                        status,
                        time.time(),
                        str(error),
                        item.hostname,
                        lease_owner,
                    ),
                )

            connection.commit()

        return cursor.rowcount == 1

    def recover_expired_leases(self) -> int:
        cutoff = (
            time.time()
            - self.lease_timeout
        )

        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE domain_discovery_work
                SET
                    status = 'queued',
                    lease_owner = NULL,
                    leased_at = NULL,
                    last_error = COALESCE(
                        last_error,
                        'lease expired and recovered'
                    )
                WHERE
                    status = 'processing'
                    AND leased_at IS NOT NULL
                    AND leased_at < ?
                """,
                (
                    cutoff,
                ),
            )

            connection.commit()

        return int(
            cursor.rowcount
        )

    def count(
        self,
        status: Optional[str] = None,
    ) -> int:
        with self._connect() as connection:
            if status is None:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM domain_discovery_work
                    """
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM domain_discovery_work
                    WHERE status = ?
                    """,
                    (
                        status,
                    ),
                ).fetchone()

        return int(
            row[0]
        )

    def stats(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM domain_discovery_work
                GROUP BY status
                """
            ).fetchall()

        result = {
            "total": 0,
            "queued": 0,
            "processing": 0,
            "complete": 0,
            "failed": 0,
        }

        for row in rows:
            status = str(
                row["status"]
            )

            count = int(
                row["count"]
            )

            result[status] = count
            result["total"] += count

        return result


class _LazyShardCollection:
    """
    Compatibility collection.

    It exposes the configured physical namespace without eagerly
    creating every physical database.
    """

    def __init__(
        self,
        fabric: "DomainDiscoveryFabric",
    ):
        self.fabric = fabric

    def __len__(self) -> int:
        return self.fabric.shard_count

    def __getitem__(
        self,
        shard_id: int,
    ) -> DomainDiscoveryShard:
        return self.fabric._get_shard(
            int(shard_id)
        )

    def __iter__(
        self,
    ) -> Iterator[DomainDiscoveryShard]:
        for shard_id in (
            self.fabric.active_shard_ids()
        ):
            yield self.fabric._get_shard(
                shard_id
            )


class DomainDiscoveryFabric:
    """
    Durable enormous-scale domain discovery fabric.

    Core separation:

        HOSTNAME
           |
           v
        STABLE LOGICAL PARTITION
           |
           v
        DURABLE PLACEMENT MAP
           |
           v
        PHYSICAL BUCKET
           |
           v
        DURABLE WORK QUEUE

    The hostname's logical identity is independent of physical capacity.

    Physical buckets can therefore be added, retired, drained, or
    reassigned without changing the logical partition identity.

    The physical bucket count is an infrastructure parameter, not a
    representation of the number of domains the system can discover.
    """

    VERSION = FABRIC_VERSION

    DEFAULT_LOGICAL_PARTITIONS = (
        DomainDiscoveryPartitioner.DEFAULT_LOGICAL_PARTITIONS
    )

    DEFAULT_PHYSICAL_BUCKETS = (
        DomainDiscoveryPlacementMap.DEFAULT_PLACEMENT_BUCKETS
    )

    def __init__(
        self,
        storage_root: str,
        shard_count: int = DEFAULT_PHYSICAL_BUCKETS,
        lease_timeout: float = 300.0,
        logical_partition_count: int = (
            DEFAULT_LOGICAL_PARTITIONS
        ),
        physical_bucket_count: Optional[int] = None,
    ):
        self.storage_root = str(
            storage_root
        ).rstrip("/")

        if not self.storage_root:
            raise ValueError(
                "storage_root must not be empty"
            )

        if physical_bucket_count is not None:
            shard_count = int(
                physical_bucket_count
            )

        self.shard_count = int(
            shard_count
        )

        self.logical_partition_count = int(
            logical_partition_count
        )

        self.lease_timeout = float(
            lease_timeout
        )

        if self.shard_count <= 0:
            raise ValueError(
                "shard_count must be positive"
            )

        if self.logical_partition_count <= 0:
            raise ValueError(
                "logical_partition_count must be positive"
            )

        if self.lease_timeout <= 0:
            raise ValueError(
                "lease_timeout must be positive"
            )

        os.makedirs(
            self.storage_root,
            exist_ok=True,
        )

        self.partitioner = (
            DomainDiscoveryPartitioner(
                shard_count=self.shard_count,
                logical_partition_count=(
                    self.logical_partition_count
                ),
            )
        )

        self.placement_map = (
            DomainDiscoveryPlacementMap(
                storage_root=self.storage_root,
                logical_partition_count=(
                    self.logical_partition_count
                ),
                physical_bucket_count=(
                    self.shard_count
                ),
            )
        )

        self._shards: dict[
            int,
            DomainDiscoveryShard,
        ] = {}

        self._shards_lock = (
            threading.RLock()
        )

        self.shards = _LazyShardCollection(
            self
        )

    # ============================================================
    # Physical storage topology
    # ============================================================

    def _physical_database_path(
        self,
        shard_id: int,
    ) -> str:
        shard_id = int(
            shard_id
        )

        if not (
            0
            <= shard_id
            < self.shard_count
        ):
            raise ValueError(
                "shard_id outside physical namespace"
            )

        # Hierarchical filesystem layout prevents enormous numbers of
        # database files from being placed in one directory.
        level_one = (
            shard_id // 256
        )

        level_two = (
            shard_id % 256
        )

        return os.path.join(
            self.storage_root,
            f"p{level_one:05d}",
            f"bucket_{level_two:03d}.db",
        )

    def _get_shard(
        self,
        shard_id: int,
    ) -> DomainDiscoveryShard:
        shard_id = int(
            shard_id
        )

        if not (
            0
            <= shard_id
            < self.shard_count
        ):
            raise ValueError(
                "shard_id outside physical namespace"
            )

        with self._shards_lock:
            shard = self._shards.get(
                shard_id
            )

            if shard is None:
                shard = DomainDiscoveryShard(
                    database_path=(
                        self._physical_database_path(
                            shard_id
                        )
                    ),
                    shard_id=shard_id,
                    lease_timeout=(
                        self.lease_timeout
                    ),
                )

                self._shards[
                    shard_id
                ] = shard

            return shard

    # ============================================================
    # Stable routing
    # ============================================================

    def logical_partition_for_hostname(
        self,
        hostname: str,
    ) -> int:
        return self.partitioner.logical_partition(
            hostname
        )

    def shard_for_hostname(
        self,
        hostname: str,
    ) -> int:
        """
        Resolve hostname through the durable placement map.

        This is the authoritative physical routing path.
        """

        return self.placement_map.physical_bucket_for_hostname(
            hostname=hostname,
            partitioner=self.partitioner,
        )

    def physical_bucket_for_logical_partition(
        self,
        logical_partition: int,
    ) -> int:
        return self.placement_map.get_or_create(
            logical_partition
        )

    def place_logical_partition(
        self,
        logical_partition: int,
        physical_bucket: int,
    ) -> bool:
        return self.placement_map.assign(
            logical_partition,
            physical_bucket,
        )

    # ============================================================
    # Durable ingestion
    # ============================================================

    def enqueue(
        self,
        record: DomainDiscoveryRecord,
    ) -> dict[str, int]:
        shard_id = self.shard_for_hostname(
            record.hostname
        )

        shard = self._get_shard(
            shard_id
        )

        return shard.enqueue_many(
            [record]
        )

    def enqueue_many(
        self,
        records: Iterable[
            DomainDiscoveryRecord
        ],
    ) -> dict[str, int]:
        records = list(
            records
        )

        if not records:
            return {
                "inserted": 0,
                "duplicates": 0,
                "invalid": 0,
            }

        grouped: dict[
            int,
            list[DomainDiscoveryRecord],
        ] = {}

        invalid = 0

        for record in records:
            try:
                hostname = (
                    str(record.hostname)
                    .strip()
                    .lower()
                    .rstrip(".")
                )

                if not hostname:
                    raise ValueError(
                        "hostname is empty"
                    )

                shard_id = (
                    self.shard_for_hostname(
                        hostname
                    )
                )

                grouped.setdefault(
                    shard_id,
                    [],
                ).append(
                    record
                )

            except Exception:
                invalid += 1

        inserted = 0
        duplicates = 0

        for shard_id, shard_records in (
            grouped.items()
        ):
            result = self._get_shard(
                shard_id
            ).enqueue_many(
                shard_records
            )

            inserted += result[
                "inserted"
            ]

            duplicates += result[
                "duplicates"
            ]

            invalid += result[
                "invalid"
            ]

        return {
            "inserted": inserted,
            "duplicates": duplicates,
            "invalid": invalid,
        }

    # ============================================================
    # Durable claiming
    # ============================================================

    def claim_many(
        self,
        shard_id: int,
        limit: int,
        lease_owner: str,
    ) -> list[ClaimedDomainDiscovery]:
        return self._get_shard(
            shard_id
        ).claim_many(
            limit=limit,
            lease_owner=lease_owner,
        )

    def claim_from_many(
        self,
        shard_ids: Iterable[int],
        limit_per_shard: int,
        lease_owner_prefix: str = (
            "domain-fabric-worker"
        ),
    ) -> list[ClaimedDomainDiscovery]:
        claimed = []

        for shard_id in shard_ids:
            owner = (
                f"{lease_owner_prefix}-"
                f"{int(shard_id)}-"
                f"{time.time_ns()}"
            )

            claimed.extend(
                self.claim_many(
                    shard_id=int(shard_id),
                    limit=limit_per_shard,
                    lease_owner=owner,
                )
            )

        return claimed

    # ============================================================
    # Durable completion/failure
    # ============================================================

    def mark_complete(
        self,
        item: ClaimedDomainDiscovery,
        lease_owner: str,
    ) -> bool:
        return self._get_shard(
            item.shard_id
        ).mark_complete(
            item=item,
            lease_owner=lease_owner,
        )

    def mark_failed(
        self,
        item: ClaimedDomainDiscovery,
        error: str,
        retry: bool = True,
        lease_owner: Optional[str] = None,
    ) -> bool:
        return self._get_shard(
            item.shard_id
        ).mark_failed(
            item=item,
            error=error,
            retry=retry,
            lease_owner=lease_owner,
        )

    # ============================================================
    # Recovery
    # ============================================================

    def recover_expired_leases(
        self,
    ) -> int:
        recovered = 0

        # Only instantiated buckets are touched here. The placement
        # metadata identifies the durable physical namespace, while
        # future deployments can use the placement catalog to discover
        # and distribute recovery ownership without a global scan.
        for shard_id in self.active_shard_ids():
            try:
                recovered += (
                    self._get_shard(
                        shard_id
                    ).recover_expired_leases()
                )
            except Exception:
                continue

        return recovered

    def recover_expired_leases_for_shards(
        self,
        shard_ids: Iterable[int],
    ) -> int:
        recovered = 0

        for shard_id in shard_ids:
            recovered += (
                self._get_shard(
                    int(shard_id)
                ).recover_expired_leases()
            )

        return recovered

    # ============================================================
    # Topology/work discovery
    # ============================================================

    def active_shard_ids(
        self,
    ) -> list[int]:
        with self._shards_lock:
            return sorted(
                self._shards.keys()
            )

    def shards_with_work(
        self,
    ) -> list[int]:
        result = []

        for shard_id in self.active_shard_ids():
            try:
                shard = self._get_shard(
                    shard_id
                )

                if (
                    shard.count(
                        "queued"
                    )
                    > 0
                    or shard.count(
                        "processing"
                    )
                    > 0
                ):
                    result.append(
                        shard_id
                    )
            except Exception:
                continue

        return result

    def placement_stats(
        self,
    ) -> dict[str, int]:
        return self.placement_map.stats()

    def physical_capacity(
        self,
    ) -> int:
        return self.shard_count

    def logical_capacity(
        self,
    ) -> int:
        return self.logical_partition_count

    # ============================================================
    # Statistics
    # ============================================================

    def stats(
        self,
    ) -> dict[str, Any]:
        aggregate = {
            "version": self.VERSION,
            "shard_count": self.shard_count,
            "physical_bucket_count": (
                self.shard_count
            ),
            "logical_partition_count": (
                self.logical_partition_count
            ),
            "total": 0,
            "queued": 0,
            "processing": 0,
            "complete": 0,
            "failed": 0,
            "active_physical_shards": 0,
            "shards_with_work": 0,
            "placement": (
                self.placement_map.stats()
            ),
            "shards": [],
        }

        for shard_id in self.active_shard_ids():
            try:
                stats = self._get_shard(
                    shard_id
                ).stats()
            except Exception:
                continue

            aggregate[
                "active_physical_shards"
            ] += 1

            if (
                stats.get("queued", 0)
                > 0
                or stats.get("processing", 0)
                > 0
            ):
                aggregate[
                    "shards_with_work"
                ] += 1

            aggregate["total"] += (
                stats.get("total", 0)
            )

            aggregate["queued"] += (
                stats.get("queued", 0)
            )

            aggregate["processing"] += (
                stats.get("processing", 0)
            )

            aggregate["complete"] += (
                stats.get("complete", 0)
            )

            aggregate["failed"] += (
                stats.get("failed", 0)
            )

            aggregate["shards"].append(
                {
                    "shard_id": shard_id,
                    **stats,
                }
            )

        return aggregate
