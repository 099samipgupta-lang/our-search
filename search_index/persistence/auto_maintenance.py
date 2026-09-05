import threading


class AutomaticIndexMaintenance:
    def __init__(
        self,
        maintenance,
        max_segments=10,
    ):
        self.maintenance = maintenance
        self.max_segments = max(2, int(max_segments))

        self._lock = threading.Lock()
        self._running = False

        self.stats = {
            "checks": 0,
            "merge_requests": 0,
            "merges_completed": 0,
            "merge_failures": 0,
        }

    def check(self):
        with self._lock:
            self.stats["checks"] += 1

            if self._running:
                return False

            if not self.maintenance.should_merge(
                self.max_segments
            ):
                return False

            self._running = True
            self.stats["merge_requests"] += 1

        try:
            result = self.maintenance.merge_all()

            if result is not None:
                self.stats["merges_completed"] += 1

            return result

        except Exception:
            with self._lock:
                self.stats["merge_failures"] += 1

            return None

        finally:
            with self._lock:
                self._running = False

    def is_running(self):
        with self._lock:
            return self._running

    def status(self):
        with self._lock:
            running = self._running

        return {
            "max_segments": self.max_segments,
            "running": running,
            "maintenance": self.maintenance.status(),
            "stats": dict(self.stats),
        }
