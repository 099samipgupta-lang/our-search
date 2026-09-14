from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


class DomainSchedulingError(Exception):
    """Raised when domain scheduling invariants are violated."""


@dataclass(frozen=True)
class DomainSchedule:
    host: str
    crawl_delay: float
    last_crawl_time: float
    next_allowed_time: float
    failures: int
    active_concurrency: int
    max_concurrency: int
    backoff_until: float

    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> "DomainSchedule":
        return cls(
            host=str(record["host"]),
            crawl_delay=float(record["crawl_delay"]),
            last_crawl_time=float(record["last_crawl_time"]),
            next_allowed_time=float(record["next_allowed_time"]),
            failures=int(record["failures"]),
            active_concurrency=int(record["active_concurrency"]),
            max_concurrency=int(record["max_concurrency"]),
            backoff_until=float(record["backoff_until"]),
        )

    def is_time_ready(self, now: float) -> bool:
        next_time = max(
            self.next_allowed_time,
            self.last_crawl_time + self.crawl_delay,
            self.backoff_until,
        )
        return next_time <= float(now)

    def has_capacity(self) -> bool:
        return self.active_concurrency < self.max_concurrency

    def is_ready(self, now: float) -> bool:
        return self.is_time_ready(now) and self.has_capacity()


class DomainScheduler:
    """
    Durable domain-aware scheduling facade.

    URLStateStore remains the authoritative persistence layer.
    This class contains scheduling policy and delegates state changes
    to the store.
    """

    def __init__(
        self,
        state_store,
        default_delay: float = 2.0,
        default_max_concurrency: int = 1,
        failure_backoff_base: float = 30.0,
        failure_backoff_max: float = 3600.0,
    ):
        if default_delay < 0:
            raise ValueError("default_delay must not be negative")

        if default_max_concurrency <= 0:
            raise ValueError(
                "default_max_concurrency must be greater than zero"
            )

        if failure_backoff_base < 0:
            raise ValueError(
                "failure_backoff_base must not be negative"
            )

        if failure_backoff_max < failure_backoff_base:
            raise ValueError(
                "failure_backoff_max must be greater than or equal "
                "to failure_backoff_base"
            )

        self.state_store = state_store
        self.default_delay = float(default_delay)
        self.default_max_concurrency = int(default_max_concurrency)
        self.failure_backoff_base = float(failure_backoff_base)
        self.failure_backoff_max = float(failure_backoff_max)

    def ensure_domain(self, host: str) -> DomainSchedule:
        self.state_store.ensure_host(
            host,
            crawl_delay=self.default_delay,
        )

        self.state_store.ensure_domain_scheduler_state(
            host,
            max_concurrency=self.default_max_concurrency,
        )

        record = self.state_store.get_host(host)

        if record is None:
            raise DomainSchedulingError(
                f"host disappeared after initialization: {host}"
            )

        return DomainSchedule.from_record(record)

    def get(self, host: str) -> Optional[DomainSchedule]:
        record = self.state_store.get_host(host)

        if record is None:
            return None

        return DomainSchedule.from_record(record)

    def configure(
        self,
        host: str,
        *,
        crawl_delay: Optional[float] = None,
        max_concurrency: Optional[int] = None,
    ) -> DomainSchedule:
        self.ensure_domain(host)

        if crawl_delay is not None:
            self.state_store.set_host_delay(
                host,
                float(crawl_delay),
            )

        if max_concurrency is not None:
            self.state_store.set_host_max_concurrency(
                host,
                int(max_concurrency),
            )

        record = self.state_store.get_host(host)

        if record is None:
            raise DomainSchedulingError(
                f"host disappeared after configuration: {host}"
            )

        return DomainSchedule.from_record(record)

    def can_schedule(
        self,
        host: str,
        now: Optional[float] = None,
    ) -> bool:
        schedule = self.ensure_domain(host)

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        return schedule.is_ready(timestamp)

    def record_success(
        self,
        host: str,
        crawled_at: Optional[float] = None,
    ) -> DomainSchedule:
        self.ensure_domain(host)

        self.state_store.record_domain_success(
            host,
            crawled_at=crawled_at,
        )

        record = self.state_store.get_host(host)

        if record is None:
            raise DomainSchedulingError(
                f"host disappeared after success: {host}"
            )

        return DomainSchedule.from_record(record)

    def record_failure(
        self,
        host: str,
        *,
        failed_at: Optional[float] = None,
        retryable: bool = True,
    ) -> DomainSchedule:
        self.ensure_domain(host)

        timestamp = (
            time.time()
            if failed_at is None
            else float(failed_at)
        )

        self.state_store.record_domain_failure(
            host,
            failed_at=timestamp,
            backoff_base=self.failure_backoff_base,
            backoff_max=self.failure_backoff_max,
            retryable=retryable,
        )

        record = self.state_store.get_host(host)

        if record is None:
            raise DomainSchedulingError(
                f"host disappeared after failure: {host}"
            )

        return DomainSchedule.from_record(record)

    def acquire(
        self,
        host: str,
        now: Optional[float] = None,
    ) -> bool:
        self.ensure_domain(host)

        timestamp = (
            time.time()
            if now is None
            else float(now)
        )

        return self.state_store.acquire_host_slot(
            host,
            now=timestamp,
        )

    def release(
        self,
        host: str,
    ) -> bool:
        self.ensure_domain(host)

        return self.state_store.release_host_slot(host)

    def snapshot(self, host: str) -> Optional[Dict[str, Any]]:
        record = self.state_store.get_host(host)

        if record is None:
            return None

        return dict(record)
