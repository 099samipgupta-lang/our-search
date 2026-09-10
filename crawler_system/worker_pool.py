import multiprocessing
import queue
import time

from crawler_system.fetcher import Fetcher
from crawler_system.policy import CrawlPolicy
from crawler_system.worker import CrawlWorker
from crawler_system.task import CrawlTask
from crawler_system.result import CrawlResult


def worker_process(
    worker_id,
    task_queue,
    result_queue
):

    fetcher = Fetcher(
        user_agent="OurSearchBot/1.0"
    )

    policy = CrawlPolicy(
        user_agent="OurSearchBot/1.0"
    )

    worker = CrawlWorker(
        fetcher,
        policy,
        worker_id=worker_id
    )

    while True:

        task = task_queue.get()

        if task is None:
            break

        try:

            result = worker.process(task)

            result_queue.put(result)

        except Exception as error:

            response = {
                "url": task.url,
                "requested_url": task.url,
                "status": 0,
                "status_type": "worker_exception",
                "content_type": "",
                "headers": {},
                "body": b"",
                "redirect_chain": [],
                "final_url": task.url,
                "retries": task.attempt,
                "storage_path": None,
                "worker_id": worker_id,
                "error": str(error)
            }

            result_queue.put(
                CrawlResult(
                    task,
                    response
                )
            )


class WorkerPool:

    def __init__(
        self,
        worker_count=4
    ):

        self.worker_count = max(
            1,
            int(worker_count)
        )

        self.result_queue = (
            multiprocessing.Queue()
        )

        self.task_queues = {}
        self.workers = {}

        self.busy_workers = set()

        self.running = False

    def _create_worker(self, worker_id):

        task_queue = multiprocessing.Queue()

        process = multiprocessing.Process(
            target=worker_process,
            args=(
                worker_id,
                task_queue,
                self.result_queue
            )
        )

        process.start()

        self.task_queues[worker_id] = task_queue
        self.workers[worker_id] = process

    def start(self):

        if self.running:
            return

        self.running = True

        # A stopped multiprocessing queue cannot be reused.
        # Create a fresh result queue for every new pool lifecycle.
        self.result_queue = multiprocessing.Queue()

        self.task_queues = {}
        self.workers = {}
        self.busy_workers = set()

        for worker_id in range(
            self.worker_count
        ):

            self._create_worker(
                worker_id
            )

    def available_worker_ids(self):

        if not self.running:
            return []

        available = []

        for worker_id, process in self.workers.items():

            if process.is_alive() and \
               worker_id not in self.busy_workers:

                available.append(worker_id)

        return available

    def submit(
        self,
        task,
        worker_id
    ):

        if not self.running:
            raise RuntimeError(
                "Worker pool is not running"
            )

        if worker_id not in self.workers:
            raise ValueError(
                "Unknown worker"
            )

        process = self.workers[worker_id]

        if not process.is_alive():
            raise RuntimeError(
                "Worker is not alive"
            )

        if worker_id in self.busy_workers:
            raise RuntimeError(
                "Worker is already busy"
            )

        if not isinstance(
            task,
            CrawlTask
        ):
            raise TypeError(
                "task must be CrawlTask"
            )

        self.busy_workers.add(
            worker_id
        )

        self.task_queues[worker_id].put(
            task
        )

    def get_result(
        self,
        timeout=None
    ):

        try:

            return self.result_queue.get(
                timeout=timeout
            )

        except queue.Empty:

            return None

    def release_worker(
        self,
        worker_id
    ):

        self.busy_workers.discard(
            worker_id
        )

    def restart_worker(
        self,
        worker_id
    ):

        old_process = self.workers.get(
            worker_id
        )

        if old_process is not None:

            if old_process.is_alive():

                old_process.terminate()

            old_process.join(
                timeout=2
            )

        old_queue = self.task_queues.get(
            worker_id
        )

        if old_queue is not None:

            try:
                old_queue.close()
            except Exception:
                pass

        self.busy_workers.discard(
            worker_id
        )

        self._create_worker(
            worker_id
        )

    def monitor_workers(self):

        if not self.running:
            return []

        dead_workers = []

        for worker_id, process in self.workers.items():

            if not process.is_alive():

                dead_workers.append(
                    worker_id
                )

        return dead_workers

    def ensure_workers(self):

        restarted = []

        for worker_id in self.monitor_workers():

            self.restart_worker(
                worker_id
            )

            restarted.append(
                worker_id
            )

        return restarted

    def worker_status(self):

        status = {}

        for worker_id, process in self.workers.items():

            status[worker_id] = {
                "alive":
                    process.is_alive(),

                "pid":
                    process.pid,

                "busy":
                    worker_id in self.busy_workers
            }

        return status

    def active_workers(self):

        return sum(
            process.is_alive()
            for process in self.workers.values()
        )

    def stop(self):

        if not self.running:
            return

        for worker_id in list(
            self.task_queues.keys()
        ):

            try:

                self.task_queues[
                    worker_id
                ].put(None)

            except Exception:
                pass

        for process in self.workers.values():

            process.join(
                timeout=5
            )

            if process.is_alive():

                process.terminate()

                process.join(
                    timeout=2
                )

        for task_queue in self.task_queues.values():

            try:
                task_queue.close()
            except Exception:
                pass

        try:
            self.result_queue.close()
        except Exception:
            pass

        self.workers = {}
        self.task_queues = {}
        self.busy_workers = set()

        self.running = False
