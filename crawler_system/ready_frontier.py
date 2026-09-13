import sqlite3
import threading
from typing import Dict, List, Optional


class ReadyFrontier:
    """
    Durable ready-work scheduling index.

    URLStateStore remains the authoritative source of URL state.
    This table is a rebuildable scheduling index.

    IMPORTANT:
    The mutating methods support commit=False so URLStateStore can
    atomically update URL state and the scheduling index in one SQLite
    transaction.
    """

    SCHEMA_VERSION = 1

    def __init__(self, connection: sqlite3.Connection):
        self._connection = connection
        self._lock = threading.RLock()
        self._connection.row_factory = sqlite3.Row
        self._initialize_schema()

    # ============================================================
    # SCHEMA
    # ============================================================

    def _initialize_schema(self) -> None:
        with self._lock:
            self._connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ready_frontier (
                    url TEXT PRIMARY KEY,
                    host TEXT NOT NULL,
                    priority REAL NOT NULL,
                    ready_at REAL NOT NULL,
                    sequence INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                idx_ready_frontier_priority
                ON ready_frontier(
                    priority DESC,
                    ready_at ASC,
                    sequence ASC
                );

                CREATE INDEX IF NOT EXISTS
                idx_ready_frontier_host_ready
                ON ready_frontier(
                    host,
                    ready_at ASC,
                    sequence ASC
                );

                CREATE INDEX IF NOT EXISTS
                idx_ready_frontier_ready
                ON ready_frontier(
                    ready_at ASC,
                    priority DESC,
                    sequence ASC
                );
                """
            )
            self._connection.commit()

    # ============================================================
    # INSERT / UPSERT
    # ============================================================

    def add(
        self,
        url: str,
        host: str,
        priority: float,
        ready_at: float,
        sequence: int,
        commit: bool = True,
    ) -> None:
        if not url:
            raise ValueError("url must not be empty")

        if not host:
            raise ValueError("host must not be empty")

        with self._lock:
            self._connection.execute(
                """
                INSERT INTO ready_frontier (
                    url,
                    host,
                    priority,
                    ready_at,
                    sequence
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(url)
                DO UPDATE SET
                    host = excluded.host,
                    priority = excluded.priority,
                    ready_at = excluded.ready_at,
                    sequence = excluded.sequence
                """,
                (
                    url,
                    host,
                    float(priority),
                    float(ready_at),
                    int(sequence),
                ),
            )

            if commit:
                self._connection.commit()

    # ============================================================
    # REMOVE
    # ============================================================

    def remove(
        self,
        url: str,
        commit: bool = True,
    ) -> bool:
        with self._lock:
            cursor = self._connection.execute(
                """
                DELETE FROM ready_frontier
                WHERE url = ?
                """,
                (url,),
            )

            if commit:
                self._connection.commit()

            return cursor.rowcount == 1

    # ============================================================
    # LOOKUP
    # ============================================================

    def get(self, url: str) -> Optional[Dict]:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT
                    url,
                    host,
                    priority,
                    ready_at,
                    sequence
                FROM ready_frontier
                WHERE url = ?
                """,
                (url,),
            ).fetchone()

            return dict(row) if row else None

    # ============================================================
    # CANDIDATES
    # ============================================================

    def candidates(
        self,
        limit: int = 100,
    ) -> List[Dict]:
        if limit <= 0:
            return []

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT
                    url,
                    host,
                    priority,
                    ready_at,
                    sequence
                FROM ready_frontier
                ORDER BY
                    priority DESC,
                    ready_at ASC,
                    sequence ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()

            return [dict(row) for row in rows]

    # ============================================================
    # DUE CANDIDATES
    # ============================================================

    def due_candidates(
        self,
        now: float,
        limit: int = 100,
    ) -> List[Dict]:
        if limit <= 0:
            return []

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT
                    url,
                    host,
                    priority,
                    ready_at,
                    sequence
                FROM ready_frontier
                WHERE ready_at <= ?
                ORDER BY
                    priority DESC,
                    ready_at ASC,
                    sequence ASC
                LIMIT ?
                """,
                (
                    float(now),
                    int(limit),
                ),
            ).fetchall()

            return [dict(row) for row in rows]

    # ============================================================
    # COUNT
    # ============================================================

    def count(self) -> int:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(*)
                FROM ready_frontier
                """
            ).fetchone()

            return int(row[0])

    # ============================================================
    # CLEAR
    # ============================================================

    def clear(self, commit: bool = True) -> None:
        with self._lock:
            self._connection.execute(
                "DELETE FROM ready_frontier"
            )

            if commit:
                self._connection.commit()

    # ============================================================
    # REBUILD
    # ============================================================

    def rebuild(self, commit: bool = True) -> int:
        """
        Rebuild the scheduling index entirely from authoritative URL
        state.

        Sequence uses SQLite rowid so rebuild ordering remains stable
        for the current database contents.
        """
        with self._lock:
            self._connection.execute(
                "DELETE FROM ready_frontier"
            )

            cursor = self._connection.execute(
                """
                INSERT INTO ready_frontier (
                    url,
                    host,
                    priority,
                    ready_at,
                    sequence
                )
                SELECT
                    url,
                    host,
                    priority,
                    COALESCE(next_crawl_at, discovered_at),
                    rowid
                FROM urls
                WHERE state IN (
                    'discovered',
                    'queued',
                    'retry'
                )
                """
            )

            if commit:
                self._connection.commit()

            return cursor.rowcount
