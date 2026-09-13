import sqlite3
import time
from pathlib import Path
from threading import Lock


class GlobalExpansionFeedback:
    """
    Durable source-level feedback and adaptation for global expansion.

    This component does not replace source health/adaptive control.
    It measures downstream effectiveness and converts those outcomes
    into source effectiveness scores that can influence future work.
    """

    def __init__(
        self,
        database_path,
        minimum_score=0.1,
        maximum_score=2.0,
        success_weight=1.0,
        failure_weight=1.0,
    ):
        self.database_path = str(database_path)
        self.minimum_score = float(minimum_score)
        self.maximum_score = float(maximum_score)
        self.success_weight = float(success_weight)
        self.failure_weight = float(failure_weight)

        if self.minimum_score < 0:
            raise ValueError("minimum_score must be >= 0")

        if self.maximum_score <= self.minimum_score:
            raise ValueError(
                "maximum_score must be greater than minimum_score"
            )

        if self.success_weight <= 0:
            raise ValueError("success_weight must be > 0")

        if self.failure_weight <= 0:
            raise ValueError("failure_weight must be > 0")

        self._lock = Lock()
        self._prepare_database_directory()
        self._initialize()

    def _prepare_database_directory(self):
        path = Path(self.database_path)
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)

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
                    CREATE TABLE IF NOT EXISTS
                    global_expansion_feedback (
                        event_id TEXT PRIMARY KEY,
                        hostname TEXT NOT NULL,
                        source TEXT NOT NULL,
                        outcome TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        metadata_json TEXT
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_feedback_source
                    ON global_expansion_feedback(source)
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_feedback_hostname
                    ON global_expansion_feedback(hostname)
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_feedback_outcome
                    ON global_expansion_feedback(outcome)
                    """
                )

                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS
                    global_expansion_source_stats (
                        source TEXT PRIMARY KEY,
                        successes INTEGER NOT NULL DEFAULT 0,
                        failures INTEGER NOT NULL DEFAULT 0,
                        total_events INTEGER NOT NULL DEFAULT 0,
                        last_event_at REAL,
                        score REAL NOT NULL DEFAULT 1.0
                    )
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

        return hostname or None

    @staticmethod
    def _normalize_source(source):
        if not isinstance(source, str):
            return None

        source = source.strip()

        return source or None

    @staticmethod
    def _event_id(hostname, source, outcome):
        return "|".join(
            (
                hostname,
                source,
                outcome,
            )
        )

    def _recalculate_score(
        self,
        successes,
        failures,
    ):
        weighted_successes = successes * self.success_weight
        weighted_failures = failures * self.failure_weight

        total = weighted_successes + weighted_failures

        if total <= 0:
            return 1.0

        success_ratio = weighted_successes / total

        score = (
            self.minimum_score
            + (
                self.maximum_score
                - self.minimum_score
            )
            * success_ratio
        )

        return max(
            self.minimum_score,
            min(self.maximum_score, score),
        )

    def record(
        self,
        hostname,
        source,
        outcome,
        metadata=None,
    ):
        hostname = self._normalize_hostname(hostname)
        source = self._normalize_source(source)

        if hostname is None:
            return {
                "recorded": False,
                "duplicate": False,
                "reason": "invalid_hostname",
            }

        if source is None:
            return {
                "recorded": False,
                "duplicate": False,
                "reason": "invalid_source",
            }

        if not isinstance(outcome, str):
            return {
                "recorded": False,
                "duplicate": False,
                "reason": "invalid_outcome",
            }

        outcome = outcome.strip()

        if not outcome:
            return {
                "recorded": False,
                "duplicate": False,
                "reason": "invalid_outcome",
            }

        event_id = self._event_id(
            hostname,
            source,
            outcome,
        )

        now = time.time()

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO
                    global_expansion_feedback
                    (
                        event_id,
                        hostname,
                        source,
                        outcome,
                        created_at,
                        metadata_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        hostname,
                        source,
                        outcome,
                        now,
                        None if metadata is None else str(metadata),
                    ),
                )

                if cursor.rowcount != 1:
                    connection.commit()

                    return {
                        "recorded": False,
                        "duplicate": True,
                        "event_id": event_id,
                        "source": source,
                        "hostname": hostname,
                        "outcome": outcome,
                    }

                success = outcome in {
                    "activation_success",
                    "queued",
                    "expansion_success",
                    "crawl_success",
                    "useful_work",
                }

                failure = outcome in {
                    "activation_failed",
                    "queue_failed",
                    "expansion_failed",
                    "crawl_failed",
                }

                row = connection.execute(
                    """
                    SELECT
                        successes,
                        failures
                    FROM global_expansion_source_stats
                    WHERE source = ?
                    """,
                    (source,),
                ).fetchone()

                successes = int(row["successes"]) if row else 0
                failures = int(row["failures"]) if row else 0

                if success:
                    successes += 1

                if failure:
                    failures += 1

                total_events = successes + failures

                score = self._recalculate_score(
                    successes,
                    failures,
                )

                connection.execute(
                    """
                    INSERT INTO
                    global_expansion_source_stats
                    (
                        source,
                        successes,
                        failures,
                        total_events,
                        last_event_at,
                        score
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source)
                    DO UPDATE SET
                        successes = excluded.successes,
                        failures = excluded.failures,
                        total_events = excluded.total_events,
                        last_event_at = excluded.last_event_at,
                        score = excluded.score
                    """,
                    (
                        source,
                        successes,
                        failures,
                        total_events,
                        now,
                        score,
                    ),
                )

                connection.commit()

                return {
                    "recorded": True,
                    "duplicate": False,
                    "event_id": event_id,
                    "source": source,
                    "hostname": hostname,
                    "outcome": outcome,
                    "score": score,
                }

            finally:
                connection.close()

    def record_outcome(
        self,
        hostname,
        source,
        outcome,
        metadata=None,
    ):
        return self.record(
            hostname=hostname,
            source=source,
            outcome=outcome,
            metadata=metadata,
        )

    def source_stats(self, source=None):
        with self._lock:
            connection = self._connect()

            try:
                if source is None:
                    rows = connection.execute(
                        """
                        SELECT
                            source,
                            successes,
                            failures,
                            total_events,
                            last_event_at,
                            score
                        FROM global_expansion_source_stats
                        ORDER BY score DESC, source ASC
                        """
                    ).fetchall()

                    return [dict(row) for row in rows]

                source = self._normalize_source(source)

                if source is None:
                    return None

                row = connection.execute(
                    """
                    SELECT
                        source,
                        successes,
                        failures,
                        total_events,
                        last_event_at,
                        score
                    FROM global_expansion_source_stats
                    WHERE source = ?
                    """,
                    (source,),
                ).fetchone()

                return None if row is None else dict(row)

            finally:
                connection.close()

    def get_score(self, source):
        stats = self.source_stats(source)

        if stats is None:
            return 1.0

        return float(stats["score"])

    def adapt_priority(
        self,
        priority,
        source,
    ):
        try:
            priority = float(priority)
        except (TypeError, ValueError):
            priority = 50.0

        priority = max(0.0, min(100.0, priority))

        score = self.get_score(source)

        adapted = priority * score

        return max(0.0, min(100.0, adapted))

    def feedback_count(self):
        with self._lock:
            connection = self._connect()

            try:
                row = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM global_expansion_feedback
                    """
                ).fetchone()

                return int(row[0])

            finally:
                connection.close()

    def close(self):
        """
        Close the feedback store.

        GlobalExpansionFeedback opens SQLite connections per operation,
        so there is no persistent connection to close. This method exists
        as an explicit lifecycle hook for callers and restart tests.
        """
        return None

    def status(self):
        stats = self.source_stats()

        return {
            "feedback_events": self.feedback_count(),
            "sources": len(stats),
            "minimum_score": self.minimum_score,
            "maximum_score": self.maximum_score,
            "source_stats": stats,
        }
