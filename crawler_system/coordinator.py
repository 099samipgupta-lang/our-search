import time

from crawler_system.task import CrawlTask
from crawler_system.worker_pool import WorkerPool


class WorkerCoordinator:

    def __init__(
        self,
        frontier,
        worker_count=4,
        document_id_start=1,
        task_timeout=30,
        max_attempts=3
    ):

        self.frontier = frontier

        self.pool = WorkerPool(
            worker_count=worker_count
        )

        self.next_document_id = (
            document_id_start
        )

        self.task_timeout = float(
            task_timeout
        )

        self.max_attempts = int(
            max_attempts
        )

        self.in_flight = {}

        self.completed = 0
        self.failed = 0
        self.retried = 0
        self.timeouts = 0
        self.worker_restarts = 0
        self.stale_results = 0

        self.running = False

    def _new_document_id(self):

        document_id = (
            f"DOC-{self.next_document_id:06d}"
        )

        self.next_document_id += 1

        return document_id

    def start(self):

        if self.running:
            return

        self.pool.start()

        self.running = True

    def _dispatch_task(
        self,
        task,
        worker_id
    ):

        self.pool.submit(
            task,
            worker_id
        )

        self.in_flight[
            task.document_id
        ] = {
            "task": task,
            "worker_id": worker_id,
            "started_at": time.monotonic()
        }

    def dispatch(self):

        if not self.running:
            return 0

        dispatched = 0

        available_workers = (
            self.pool.available_worker_ids()
        )

        for worker_id in available_workers:

            url = self.frontier.get_next()

            if url is None:
                break

            task = CrawlTask(
                url=url,
                document_id=
                    self._new_document_id()
            )

            self._dispatch_task(
                task,
                worker_id
            )

            dispatched += 1

        return dispatched

    def _retry_task(
        self,
        task
    ):

        if task.attempt + 1 >= self.max_attempts:

            self.failed += 1

            return False

        retry_task = task.retry()

        self.retried += 1

        self.frontier.add(
            retry_task.url,
            priority=retry_task.priority,
            available_at=time.time()
        )

        return True

    def _handle_dead_workers(self):

        dead_workers = (
            self.pool.monitor_workers()
        )

        for worker_id in dead_workers:

            affected = []

            for document_id, lease in list(
                self.in_flight.items()
            ):

                if lease["worker_id"] == worker_id:

                    affected.append(
                        (
                            document_id,
                            lease["task"]
                        )
                    )

            for document_id, task in affected:

                self.in_flight.pop(
                    document_id,
                    None
                )

                self._retry_task(
                    task
                )

            self.pool.restart_worker(
                worker_id
            )

            self.worker_restarts += 1

    def _handle_timeouts(self):

        now = time.monotonic()

        expired = []

        for document_id, lease in list(
            self.in_flight.items()
        ):

            elapsed = (
                now - lease["started_at"]
            )

            if elapsed >= self.task_timeout:

                expired.append(
                    (
                        document_id,
                        lease
                    )
                )

        for document_id, lease in expired:

            task = lease["task"]
            worker_id = lease["worker_id"]

            self.in_flight.pop(
                document_id,
                None
            )

            self.timeouts += 1

            self.pool.release_worker(
                worker_id
            )

            self._retry_task(
                task
            )

            self.pool.restart_worker(
                worker_id
            )

            self.worker_restarts += 1

    def monitor(self):

        if not self.running:
            return

        self._handle_dead_workers()

        self._handle_timeouts()

    def collect(
        self,
        timeout=0.2
    ):

        if not self.running:
            return []

        results = []

        result = self.pool.get_result(
            timeout=timeout
        )

        if result is not None:

            results.append(
                self._process_result(
                    result
                )
            )

        while True:

            result = self.pool.get_result(
                timeout=0
            )

            if result is None:
                break

            results.append(
                self._process_result(
                    result
                )
            )

        return [
            result
            for result in results
            if result is not None
        ]

    def _process_result(
        self,
        result
    ):

        document_id = (
            result.task.document_id
        )

        lease = self.in_flight.get(
            document_id
        )

        if lease is None:

            self.stale_results += 1

            return None

        current_task = lease["task"]

        if (
            result.task.attempt
            != current_task.attempt
        ):

            self.stale_results += 1

            return None

        worker_id = lease[
            "worker_id"
        ]

        self.in_flight.pop(
            document_id,
            None
        )

        self.pool.release_worker(
            worker_id
        )

        status = result.response.get(
            "status",
            0
        )

        if 200 <= status < 300:

            self.completed += 1

        else:

            self._retry_task(
                result.task
            )

        return result

    def run_once(self):

        if not self.running:
            self.start()

        self.monitor()

        dispatched = self.dispatch()

        results = self.collect(
            timeout=0.5
        )

        self.monitor()

        return {
            "dispatched":
                dispatched,

            "completed":
                len(results),

            "in_flight":
                len(self.in_flight),

            "frontier":
                self.frontier.size(),

            "workers":
                self.pool.active_workers(),

            "retried":
                self.retried,

            "timeouts":
                self.timeouts,

            "worker_restarts":
                self.worker_restarts,

            "failed":
                self.failed
        }

    def run_until_empty(
        self,
        max_cycles=10000
    ):

        if not self.running:
            self.start()

        cycles = 0

        while cycles < max_cycles:

            cycles += 1

            self.monitor()

            self.dispatch()

            if self.in_flight:

                self.collect(
                    timeout=0.5
                )

            else:

                if self.frontier.size() == 0:
                    break

                time.sleep(
                    0.05
                )

        self.monitor()

        return self.status()

    def stop(self):

        if not self.running:
            return

        self.pool.stop()

        self.running = False

        self.in_flight.clear()

    def status(self):

        return {
            "running":
                self.running,

            "workers":
                self.pool.active_workers(),

            "worker_status":
                self.pool.worker_status(),

            "in_flight":
                len(self.in_flight),

            "completed":
                self.completed,

            "failed":
                self.failed,

            "retried":
                self.retried,

            "timeouts":
                self.timeouts,

            "worker_restarts":
                self.worker_restarts,

            "stale_results":
                self.stale_results,

            "frontier":
                self.frontier.size()
        }
