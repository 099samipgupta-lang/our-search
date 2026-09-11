import sqlite3
import time
from pathlib import Path
from threading import Lock
from urllib.parse import urlsplit


class ExpansionCandidateStore:

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
                exist_ok=True
            )

    def _connect(self):
        connection = sqlite3.connect(
            self.database_path,
            timeout=30
        )

        connection.row_factory = sqlite3.Row

        return connection

    def _initialize(self):
        with self._lock:
            connection = self._connect()

            try:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS expansion_candidates (
                        hostname TEXT PRIMARY KEY,
                        first_url TEXT NOT NULL,
                        source_hostname TEXT,
                        discovered_at REAL NOT NULL,
                        status TEXT NOT NULL DEFAULT 'discovered'
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_expansion_status
                    ON expansion_candidates(status)
                    """
                )

                connection.commit()

            finally:
                connection.close()

    @staticmethod
    def _hostname(url):
        if not isinstance(url, str):
            return None

        try:
            hostname = urlsplit(url).hostname
        except Exception:
            return None

        if not hostname:
            return None

        return hostname.rstrip(".").lower()

    def add(
        self,
        url,
        source_url=None,
        discovered_at=None
    ):
        hostname = self._hostname(url)

        if hostname is None:
            return False

        if discovered_at is None:
            discovered_at = time.time()

        source_hostname = self._hostname(
            source_url
        )

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO
                    expansion_candidates
                    (
                        hostname,
                        first_url,
                        source_hostname,
                        discovered_at,
                        status
                    )
                    VALUES (?, ?, ?, ?, 'discovered')
                    """,
                    (
                        hostname,
                        url,
                        source_hostname,
                        float(discovered_at)
                    )
                )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    def get(self, hostname):
        if not isinstance(hostname, str):
            return None

        hostname = hostname.rstrip(".").lower()

        if not hostname:
            return None

        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT
                        hostname,
                        first_url,
                        source_hostname,
                        discovered_at,
                        status
                    FROM expansion_candidates
                    WHERE hostname = ?
                    """,
                    (hostname,)
                ).fetchone()

                if row is None:
                    return None

                return dict(row)

            finally:
                connection.close()

    def list_candidates(
        self,
        status="discovered",
        limit=100
    ):
        limit = max(1, int(limit))

        with self._lock:
            connection = self._connect()

            try:
                rows = connection.execute(
                    """
                    SELECT
                        hostname,
                        first_url,
                        source_hostname,
                        discovered_at,
                        status
                    FROM expansion_candidates
                    WHERE status = ?
                    ORDER BY discovered_at ASC
                    LIMIT ?
                    """,
                    (
                        status,
                        limit
                    )
                ).fetchall()

                return [
                    dict(row)
                    for row in rows
                ]

            finally:
                connection.close()

    def mark_status(
        self,
        hostname,
        status
    ):
        if not isinstance(hostname, str):
            return False

        hostname = hostname.rstrip(".").lower()

        if not hostname:
            return False

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    UPDATE expansion_candidates
                    SET status = ?
                    WHERE hostname = ?
                    """,
                    (
                        status,
                        hostname
                    )
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
                        FROM expansion_candidates
                        """
                    ).fetchone()
                else:
                    row = connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM expansion_candidates
                        WHERE status = ?
                        """,
                        (status,)
                    ).fetchone()

                return int(row[0])

            finally:
                connection.close()
