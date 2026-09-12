import sqlite3
from pathlib import Path
from threading import Lock

from crawler_system.discovery_priority_feedback import (
    DiscoveryPriorityFeedback,
)


class DiscoveryPriorityFeedbackStore:
    """Durable SQLite storage for discovery-source feedback."""

    def __init__(self, database_path, feedback=None):
        self.database_path = str(database_path)
        self.feedback = (
            feedback
            if feedback is not None
            else DiscoveryPriorityFeedback()
        )
        self._lock = Lock()

        path = Path(self.database_path)
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)

        self._initialize()
        self._load()

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
                    CREATE TABLE IF NOT EXISTS discovery_priority_feedback (
                        source TEXT PRIMARY KEY,
                        score REAL NOT NULL DEFAULT 0.0
                    )
                    """
                )
                connection.commit()
            finally:
                connection.close()

    def _load(self):
        with self._lock:
            connection = self._connect()
            try:
                rows = connection.execute(
                    """
                    SELECT source, score
                    FROM discovery_priority_feedback
                    """
                ).fetchall()

                for row in rows:
                    self.feedback._feedback[row["source"]] = float(
                        row["score"]
                    )
            finally:
                connection.close()

    def record(self, source, *, success):
        if not self.feedback.record(source, success=success):
            return False

        score = self.feedback.score(source)

        with self._lock:
            connection = self._connect()
            try:
                connection.execute(
                    """
                    INSERT INTO discovery_priority_feedback
                    (source, score)
                    VALUES (?, ?)
                    ON CONFLICT(source)
                    DO UPDATE SET score = excluded.score
                    """,
                    (source.strip(), score),
                )
                connection.commit()
            finally:
                connection.close()

        return True

    def score(self, source):
        return self.feedback.score(source)

    def snapshot(self):
        return self.feedback.snapshot()
