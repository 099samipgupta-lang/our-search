import json
import sqlite3
import time
from pathlib import Path
from threading import Lock

from crawler_system.domain_discovery_sources import DomainCandidate
from crawler_system.domain_candidate_provenance import (
    DomainCandidateProvenance,
)


class DomainCandidateStore:
    """
    Durable SQLite store for independently discovered domains.

    This store is intentionally separate from the crawler's URL
    state and the Stage 4.2 expansion candidate store.
    """

    def __init__(self, database_path):
        self.database_path = str(database_path)
        self._lock = Lock()

        self._prepare_database_directory()
        self._initialize()

    def _prepare_database_directory(self):
        path = Path(self.database_path)

        if path.parent != Path("."):
            path.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

    def _connect(self):
        connection = sqlite3.connect(
            self.database_path,
            timeout=30,
        )

        connection.row_factory = sqlite3.Row

        return connection

    def _initialize(self):
        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS domain_candidates (
                        hostname TEXT PRIMARY KEY,
                        url TEXT NOT NULL,
                        source TEXT NOT NULL,
                        evidence TEXT,
                        discovered_at REAL NOT NULL,
                        metadata_json TEXT,
                        status TEXT NOT NULL DEFAULT 'discovered'
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_domain_candidates_status
                    ON domain_candidates(status)
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_domain_candidates_source
                    ON domain_candidates(source)
                    """
                )

                connection.commit()

            finally:
                connection.close()

    @staticmethod
    def _normalize_hostname(hostname):
        if not isinstance(hostname, str):
            return None

        hostname = hostname.strip().lower().rstrip(".")

        if not hostname:
            return None

        return hostname

    @staticmethod
    def _metadata_json(metadata):
        if metadata is None:
            return None

        try:
            return json.dumps(
                metadata,
                ensure_ascii=False,
                sort_keys=True,
            )
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _metadata_from_json(value):
        if value is None:
            return None

        try:
            metadata = json.loads(value)
        except (TypeError, ValueError):
            return None

        if not isinstance(metadata, dict):
            return None

        return metadata

    def add(
        self,
        candidate: DomainCandidate,
    ) -> bool:
        if not isinstance(candidate, DomainCandidate):
            return False

        hostname = self._normalize_hostname(
            candidate.hostname
        )

        if hostname is None:
            return False

        if not isinstance(candidate.url, str):
            return False

        if not isinstance(candidate.source, str):
            return False

        discovered_at = candidate.discovered_at

        if discovered_at is None:
            discovered_at = time.time()

        metadata_json = self._metadata_json(
            candidate.metadata
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO domain_candidates
                    (
                        hostname,
                        url,
                        source,
                        evidence,
                        discovered_at,
                        metadata_json,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'discovered')
                    """,
                    (
                        hostname,
                        candidate.url,
                        candidate.source,
                        candidate.evidence,
                        float(discovered_at),
                        metadata_json,
                    ),
                )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def add_provenance(
        self,
        hostname,
        url,
        provenance: DomainCandidateProvenance,
        status="discovered",
    ) -> bool:
        """
        Convenience method for storing a candidate from a
        separately constructed provenance record.
        """

        hostname = self._normalize_hostname(hostname)

        if hostname is None:
            return False

        if not isinstance(url, str) or not url:
            return False

        if not isinstance(
            provenance,
            DomainCandidateProvenance,
        ):
            return False

        metadata_json = self._metadata_json(
            provenance.metadata
        )

        discovered_at = provenance.discovered_at

        if discovered_at is None:
            discovered_at = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO domain_candidates
                    (
                        hostname,
                        url,
                        source,
                        evidence,
                        discovered_at,
                        metadata_json,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        hostname,
                        url,
                        provenance.source,
                        provenance.evidence,
                        float(discovered_at),
                        metadata_json,
                        status,
                    ),
                )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def get(self, hostname):
        hostname = self._normalize_hostname(hostname)

        if hostname is None:
            return None

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT
                        hostname,
                        url,
                        source,
                        evidence,
                        discovered_at,
                        metadata_json,
                        status
                    FROM domain_candidates
                    WHERE hostname = ?
                    """,
                    (hostname,),
                ).fetchone()

                if row is None:
                    return None

                result = dict(row)

                result["metadata"] = (
                    self._metadata_from_json(
                        result.pop("metadata_json")
                    )
                )

                return result

            finally:
                connection.close()

    def list_candidates(
        self,
        status="discovered",
        limit=100,
    ):
        limit = max(1, int(limit))

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        hostname,
                        url,
                        source,
                        evidence,
                        discovered_at,
                        metadata_json,
                        status
                    FROM domain_candidates
                    WHERE status = ?
                    ORDER BY discovered_at ASC
                    LIMIT ?
                    """,
                    (
                        status,
                        limit,
                    ),
                ).fetchall()

                results = []

                for row in rows:
                    result = dict(row)

                    result["metadata"] = (
                        self._metadata_from_json(
                            result.pop("metadata_json")
                        )
                    )

                    results.append(result)

                return results

            finally:
                connection.close()

    def mark_status(
        self,
        hostname,
        status,
    ):
        hostname = self._normalize_hostname(hostname)

        if hostname is None:
            return False

        if not isinstance(status, str) or not status:
            return False

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE domain_candidates
                    SET status = ?
                    WHERE hostname = ?
                    """,
                    (
                        status,
                        hostname,
                    ),
                )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def count(self, status=None):
        with self._lock:
            connection = self._connect()

            try:
                if status is None:
                    row = connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM domain_candidates
                        """
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM domain_candidates
                        WHERE status = ?
                        """,
                        (status,),
                    ).fetchone()

                return int(row[0])

            finally:
                connection.close()

    def clear(self):
        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    "DELETE FROM domain_candidates"
                )

                connection.commit()

            finally:
                connection.close()
