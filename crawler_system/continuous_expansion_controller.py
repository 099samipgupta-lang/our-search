import time
from threading import Lock


class ContinuousExpansionController:
    """
    Production lifecycle controller for durable Web-domain expansion.

    Responsibilities:
    - recover expired expansion leases
    - process bounded expansion batches
    - isolate processing failures
    - maintain operational metrics
    - provide explicit start/stop lifecycle state

    The controller does NOT own:
    - domain discovery
    - candidate storage
    - expansion priority
    - expansion queue implementation
    - crawler workers

    The existing WholeWebCrawler remains responsible for actually
    converting expansion candidates into normal crawl work.
    """

    def __init__(
        self,
        expansion_queue,
        processor,
        batch_size=10,
        cycle_interval=5.0,
    ):
        if expansion_queue is None:
            raise TypeError("expansion_queue is required")

        if not callable(processor):
            raise TypeError("processor must be callable")

        try:
            batch_size = int(batch_size)
        except Exception:
            batch_size = 10

        try:
            cycle_interval = float(cycle_interval)
        except Exception:
            cycle_interval = 5.0

        self.expansion_queue = expansion_queue
        self.processor = processor

        self.batch_size = max(1, batch_size)
        self.cycle_interval = max(0.1, cycle_interval)

        self.running = False

        self._lock = Lock()

        self.stats = {
            "cycles": 0,
            "recovered": 0,
            "processed": 0,
            "failed_cycles": 0,
            "processor_errors": 0,
            "last_processed": 0,
            "last_recovered": 0,
            "last_cycle_at": None,
        }

    # ---------------------------------------------------------
    # LIFECYCLE
    # ---------------------------------------------------------

    def start(self):
        with self._lock:
            if self.running:
                return False

            self.running = True
            return True

    def stop(self):
        with self._lock:
            if not self.running:
                return False

            self.running = False
            return True

    # ---------------------------------------------------------
    # RECOVERY
    # ---------------------------------------------------------

    def recover(self):
        """
        Recover expired processing leases.

        Recovery is deliberately isolated from candidate processing
        so a database recovery failure does not silently look like
        successful expansion work.
        """
        recovered = self.expansion_queue.recover_expired_leases()

        recovered = max(0, int(recovered))

        with self._lock:
            self.stats["recovered"] += recovered
            self.stats["last_recovered"] = recovered

        return recovered

    # ---------------------------------------------------------
    # SINGLE CYCLE
    # ---------------------------------------------------------

    def run_cycle(self):
        """
        Execute exactly one expansion-control cycle.

        Returns a compact cycle result.
        """

        started_at = time.time()

        recovered = 0
        processed = 0
        error = None

        try:
            recovered = self.recover()
        except Exception as exc:
            error = exc

            with self._lock:
                self.stats["failed_cycles"] += 1
                self.stats["processor_errors"] += 1
                self.stats["last_processed"] = 0
                self.stats["last_cycle_at"] = time.time()

            return {
                "recovered": 0,
                "processed": 0,
                "duration": time.time() - started_at,
                "error": str(error),
            }

        try:
            processed = self.processor(
                limit=self.batch_size
            )

            processed = max(0, int(processed))

        except Exception as exc:
            error = exc

            with self._lock:
                self.stats["failed_cycles"] += 1
                self.stats["processor_errors"] += 1
                self.stats["last_processed"] = 0
                self.stats["last_cycle_at"] = time.time()

            return {
                "recovered": recovered,
                "processed": 0,
                "duration": time.time() - started_at,
                "error": str(error),
            }

        duration = time.time() - started_at

        with self._lock:
            self.stats["cycles"] += 1
            self.stats["processed"] += processed
            self.stats["last_processed"] = processed
            self.stats["last_recovered"] = recovered
            self.stats["last_cycle_at"] = time.time()

        return {
            "recovered": recovered,
            "processed": processed,
            "duration": duration,
            "error": None,
        }

    # ---------------------------------------------------------
    # BOUNDED BATCH
    # ---------------------------------------------------------

    def process_once(self):
        """
        Compatibility alias for executing one controller cycle.
        """
        return self.run_cycle()

    # ---------------------------------------------------------
    # CONTINUOUS LOOP
    # ---------------------------------------------------------

    def run(self, max_cycles=None):
        """
        Run the expansion controller continuously.

        max_cycles is optional and exists for deterministic testing.
        """

        if not self.start():
            return self.status()

        cycles = 0

        try:
            while self.running:

                cycles += 1

                if (
                    max_cycles is not None
                    and cycles > int(max_cycles)
                ):
                    break

                self.run_cycle()

                if not self.running:
                    break

                time.sleep(self.cycle_interval)

        finally:
            self.stop()

        return self.status()

    # ---------------------------------------------------------
    # STATUS
    # ---------------------------------------------------------

    def status(self):
        with self._lock:
            stats = dict(self.stats)

        return {
            "running": self.running,
            "batch_size": self.batch_size,
            "cycle_interval": self.cycle_interval,
            "queued": self.expansion_queue.count("queued"),
            "processing": self.expansion_queue.count("processing"),
            "complete": self.expansion_queue.count("complete"),
            "failed": self.expansion_queue.count("failed"),
            "stats": stats,
        }
