import sqlite3
import time
from pathlib import Path
from threading import Lock


class ContinuousExpansionFailureRecovery:
    """
    Durable failure-recovery policy for continuous expansion.

    Responsibilities:
    - track durable failure attempts per hostname
    - enforce a bounded retry budget
    - recover expired processing leases
    - isolate permanently failing candidates
    - preserve recovery state across process restarts

    This component does not own expansion processing.
    The existing ExpansionQueue remains the source of truth for
    queue state.
    """

    def __init__(
        self,
        expansion_queue,
        database_path,
        max_attempts=3,
        retry_delay=0.0,
    ):
        if expansion_queue is None:
            raise TypeError("expansion_queue is required")

        try:
            max_attempts = int(max_attempts)
        except Exception:
            max_attempts = 3

        try:
            retry_delay = float(retry_delay)
        except Exception:
            retry_delay = 0.0

        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        self.expansion_queue = expansion_queue
        self.database_path = str(database_path)
        self.max_attempts = max_attempts
        self.retry_delay = max(0.0, retry_delay)

        self._lock = Lock()

        Path(self.database_path).parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize()

        self.stats = {
            "cycles": 0,
            "lease_recoveries": 0,
            "retry_requests": 0,
            "retries_scheduled": 0,
            "retry_exhausted": 0,
            "invalid_candidates": 0,
            "errors": 0,
            "last_cycle_at": None,
            "last_recovered": 0,
            "last_retried": 0,
            "last_exhausted": 0,
        }

    # ---------------------------------------------------------
    # DATABASE
    # ---------------------------------------------------------

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
                    expansion_failure_recovery (
                        hostname TEXT PRIMARY KEY,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        last_failure_at REAL,
                        next_retry_at REAL,
                        status TEXT NOT NULL DEFAULT 'active'
                    )
                    """
                )

                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS
                    idx_failure_recovery_status
                    ON expansion_failure_recovery(status)
                    """
                )

                connection.commit()

            finally:
                connection.close()

    # ---------------------------------------------------------
    # NORMALIZATION
    # ---------------------------------------------------------

    @staticmethod
    def _normalize_hostname(hostname):
        if not isinstance(hostname, str):
            return None

        hostname = hostname.strip().lower().rstrip(".")

        return hostname or None

    # ---------------------------------------------------------
    # FAILURE STATE
    # ---------------------------------------------------------

    def record_failure(self, hostname, now=None):
        """
        Record one durable processing failure.

        Returns the resulting retry decision.
        """

        hostname = self._normalize_hostname(hostname)

        if hostname is None:
            with self._lock:
                self.stats["invalid_candidates"] += 1

            return {
                "hostname": None,
                "recorded": False,
                "retry": False,
                "exhausted": False,
                "attempts": 0,
                "reason": "invalid_hostname",
            }

        if now is None:
            now = time.time()

        now = float(now)

        with self._lock:
            connection = self._connect()

            try:
                connection.execute("BEGIN IMMEDIATE")

                row = connection.execute(
                    """
                    SELECT attempts, status
                    FROM expansion_failure_recovery
                    WHERE hostname = ?
                    """,
                    (hostname,),
                ).fetchone()

                attempts = int(row["attempts"]) if row else 0
                attempts += 1

                exhausted = attempts >= self.max_attempts

                status = (
                    "exhausted"
                    if exhausted
                    else "active"
                )

                next_retry_at = (
                    None
                    if exhausted
                    else now + self.retry_delay
                )

                connection.execute(
                    """
                    INSERT INTO expansion_failure_recovery (
                        hostname,
                        attempts,
                        last_failure_at,
                        next_retry_at,
                        status
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(hostname)
                    DO UPDATE SET
                        attempts = excluded.attempts,
                        last_failure_at = excluded.last_failure_at,
                        next_retry_at = excluded.next_retry_at,
                        status = excluded.status
                    """,
                    (
                        hostname,
                        attempts,
                        now,
                        next_retry_at,
                        status,
                    ),
                )

                connection.commit()

                self.stats["retry_requests"] += 1

                if exhausted:
                    self.stats["retry_exhausted"] += 1
                else:
                    self.stats["retries_scheduled"] += 1

                return {
                    "hostname": hostname,
                    "recorded": True,
                    "retry": not exhausted,
                    "exhausted": exhausted,
                    "attempts": attempts,
                    "max_attempts": self.max_attempts,
                    "next_retry_at": next_retry_at,
                }

            except Exception:
                connection.rollback()
                self.stats["errors"] += 1
                raise

            finally:
                connection.close()

    def handle_failure(self, candidate, now=None):
        """
        Record a processing failure and requeue the candidate when
        the durable retry budget still permits another attempt.

        The queue remains the source of truth for actual work state.
        """
        if not isinstance(candidate, dict):
            with self._lock:
                self.stats["invalid_candidates"] += 1
            return {
                "recorded": False,
                "retry": False,
                "exhausted": False,
                "requeued": False,
                "reason": "invalid_candidate",
            }

        hostname = self._normalize_hostname(
            candidate.get("hostname")
        )

        if hostname is None:
            with self._lock:
                self.stats["invalid_candidates"] += 1
            return {
                "recorded": False,
                "retry": False,
                "exhausted": False,
                "requeued": False,
                "reason": "invalid_hostname",
            }

        result = self.record_failure(
            hostname,
            now=now,
        )

        requeued = False

        if result["retry"]:
            try:
                priority = float(
                    candidate.get("priority", 50.0)
                )
            except (TypeError, ValueError):
                priority = 50.0

            priority = max(
                0.0,
                min(100.0, priority),
            )

            # A failed queue row can be safely re-enqueued by the
            # existing durable queue. Retry delay handling remains
            # controlled by the recovery policy.
            if self.retry_delay <= 0.0:
                requeued = bool(
                    self.expansion_queue.enqueue(
                        candidate,
                        priority=priority,
                    )
                )

        result["requeued"] = requeued
        return result

    def handle_success(self, hostname):
        """
        Clear durable failure history after successful processing.

        A candidate that eventually succeeds starts its next lifecycle
        with a clean retry budget.
        """
        return self.clear(hostname)

    def get_state(self, hostname):
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
                        attempts,
                        last_failure_at,
                        next_retry_at,
                        status
                    FROM expansion_failure_recovery
                    WHERE hostname = ?
                    """,
                    (hostname,),
                ).fetchone()

                return None if row is None else dict(row)

            finally:
                connection.close()

    def attempts(self, hostname):
        state = self.get_state(hostname)

        if state is None:
            return 0

        return int(state["attempts"])

    def can_retry(self, hostname, now=None):
        state = self.get_state(hostname)

        if state is None:
            return True

        if state["status"] == "exhausted":
            return False

        if now is None:
            now = time.time()

        next_retry_at = state["next_retry_at"]

        if next_retry_at is None:
            return True

        return float(now) >= float(next_retry_at)

    # ---------------------------------------------------------
    # QUEUE RECOVERY
    # ---------------------------------------------------------

    def recover_leases(self, now=None):
        """
        Recover expired processing leases using the durable queue.

        The queue remains responsible for the actual lease transition.
        """

        try:
            recovered = self.expansion_queue.recover_expired_leases(
                now=now,
            )

            recovered = max(0, int(recovered))

            with self._lock:
                self.stats["lease_recoveries"] += recovered
                self.stats["last_recovered"] = recovered

            return recovered

        except Exception:
            with self._lock:
                self.stats["errors"] += 1

            raise

    # ---------------------------------------------------------
    # FAILURE STATE RESET
    # ---------------------------------------------------------

    def clear(self, hostname):
        hostname = self._normalize_hostname(hostname)

        if hostname is None:
            return False

        with self._lock:
            connection = self._connect()

            try:
                cursor = connection.execute(
                    """
                    DELETE FROM expansion_failure_recovery
                    WHERE hostname = ?
                    """,
                    (hostname,),
                )

                connection.commit()

                return cursor.rowcount == 1

            finally:
                connection.close()

    # ---------------------------------------------------------
    # CYCLE
    # ---------------------------------------------------------

    def process_cycle(self, now=None):
        """
        Execute one recovery cycle.

        Lease recovery is performed first. Durable failure state is
        maintained independently so process restarts cannot reset
        retry budgets.
        """

        started_at = time.time()

        try:
            recovered = self.recover_leases(now=now)

            with self._lock:
                self.stats["cycles"] += 1
                self.stats["last_cycle_at"] = time.time()

            return {
                "recovered": recovered,
                "duration": time.time() - started_at,
                "error": None,
            }

        except Exception as exc:
            with self._lock:
                self.stats["cycles"] += 1
                self.stats["errors"] += 1
                self.stats["last_cycle_at"] = time.time()

            return {
                "recovered": 0,
                "duration": time.time() - started_at,
                "error": str(exc),
            }

    # ---------------------------------------------------------
    # STATUS
    # ---------------------------------------------------------

    def status(self):
        with self._lock:
            stats = dict(self.stats)

        with self._lock:
            connection = self._connect()

            try:
                active = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM expansion_failure_recovery
                    WHERE status = 'active'
                    """
                ).fetchone()[0]

                exhausted = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM expansion_failure_recovery
                    WHERE status = 'exhausted'
                    """
                ).fetchone()[0]

                total = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM expansion_failure_recovery
                    """
                ).fetchone()[0]

            finally:
                connection.close()

        return {
            "max_attempts": self.max_attempts,
            "retry_delay": self.retry_delay,
            "tracked_candidates": int(total),
            "active_failures": int(active),
            "exhausted_failures": int(exhausted),
            "stats": stats,
        }

    def close(self):
        """
        Lifecycle hook.

        SQLite connections are opened per operation, so there is no
        persistent connection to close.
        """
        return None
