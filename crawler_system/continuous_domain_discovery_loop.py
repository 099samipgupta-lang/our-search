import time
from threading import Lock


class ContinuousDomainDiscoveryLoop:
    """
    Production continuous controller for independent domain discovery.

    Responsibilities:
    - repeatedly execute the existing DomainDiscoveryPipeline
    - rotate through configured discovery contexts
    - drain durable discovered candidates
    - activate candidates through DomainCandidateActivator
    - isolate individual activation failures
    - maintain operational metrics
    - provide explicit start/stop lifecycle control

    This component does NOT:
    - implement discovery sources
    - implement source health/adaptive control
    - replace DomainDiscoveryPipeline
    - replace DomainCandidateStore
    - replace DomainCandidateActivator
    - crawl domains
    """

    def __init__(
        self,
        discovery_pipeline,
        activator,
        contexts=None,
        context_provider=None,
        cycle_interval=5.0,
        max_contexts_per_cycle=10,
        activation_batch_size=100,
        feedback_controller=None,
    ):
        if discovery_pipeline is None:
            raise TypeError("discovery_pipeline is required")

        if activator is None:
            raise TypeError("activator is required")

        if contexts is not None and context_provider is not None:
            raise ValueError(
                "contexts and context_provider are mutually exclusive"
            )

        try:
            cycle_interval = float(cycle_interval)
        except Exception:
            cycle_interval = 5.0

        try:
            max_contexts_per_cycle = int(
                max_contexts_per_cycle
            )
        except Exception:
            max_contexts_per_cycle = 10

        try:
            activation_batch_size = int(
                activation_batch_size
            )
        except Exception:
            activation_batch_size = 100

        self.discovery_pipeline = discovery_pipeline
        self.activator = activator
        self.feedback_controller = feedback_controller

        self.contexts = (
            list(contexts)
            if contexts is not None
            else []
        )

        self.context_provider = context_provider

        self.cycle_interval = max(
            0.1,
            cycle_interval,
        )

        self.max_contexts_per_cycle = max(
            1,
            max_contexts_per_cycle,
        )

        self.activation_batch_size = max(
            1,
            activation_batch_size,
        )

        self.running = False

        self._lock = Lock()
        self._context_index = 0

        self.stats = {
            "cycles": 0,
            "discovery_runs": 0,
            "discovery_failures": 0,
            "discovered": 0,
            "accepted": 0,
            "stored": 0,
            "duplicates": 0,
            "activation_batches": 0,
            "activation_requested": 0,
            "activated": 0,
            "activation_failed": 0,
            "already_active": 0,
            "last_discovered": 0,
            "last_stored": 0,
            "last_duplicates": 0,
            "last_activation_requested": 0,
            "last_activated": 0,
            "last_activation_failed": 0,
            "last_cycle_at": None,
        }

    # ============================================================
    # CONTEXT MANAGEMENT
    # ============================================================

    def _next_contexts(self):
        if self.context_provider is not None:
            provided = self.context_provider()

            if provided is None:
                return []

            if isinstance(provided, (list, tuple)):
                return list(provided)[
                    : self.max_contexts_per_cycle
                ]

            return [
                provided
            ]

        if not self.contexts:
            return [None]

        selected = []

        total = len(self.contexts)

        for _ in range(
            min(
                self.max_contexts_per_cycle,
                total,
            )
        ):
            selected.append(
                self.contexts[
                    self._context_index % total
                ]
            )

            self._context_index = (
                self._context_index + 1
            )

        return selected

    # ============================================================
    # LIFECYCLE
    # ============================================================

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

    # ============================================================
    # DISCOVERY
    # ============================================================

    def discover_once(self):
        contexts = self._next_contexts()

        total_discovered = 0
        total_accepted = 0
        total_stored = 0
        total_duplicates = 0
        failures = 0

        for context in contexts:
            try:
                result = (
                    self.discovery_pipeline
                    .discover_and_store(context)
                )

                total_discovered += max(
                    0,
                    int(result.get("discovered", 0)),
                )

                total_accepted += max(
                    0,
                    int(result.get("accepted", 0)),
                )

                total_stored += max(
                    0,
                    int(result.get("stored", 0)),
                )

                total_duplicates += max(
                    0,
                    int(result.get("duplicates", 0)),
                )

                with self._lock:
                    self.stats["discovery_runs"] += 1

            except Exception:
                failures += 1

                with self._lock:
                    self.stats["discovery_failures"] += 1

        return {
            "contexts": len(contexts),
            "discovered": total_discovered,
            "accepted": total_accepted,
            "stored": total_stored,
            "duplicates": total_duplicates,
            "failures": failures,
        }

    # ============================================================
    # DURABLE ACTIVATION
    # ============================================================

    def _get_discovered_candidates(self):
        return self.discovery_pipeline.stored_candidates(
            status="discovered",
            limit=self.activation_batch_size,
        )

    def activate_once(self):
        candidates = self._get_discovered_candidates()

        if not candidates:
            return {
                "requested": 0,
                "activated": 0,
                "already_active": 0,
                "failed": 0,
                "results": [],
            }

        results = []

        for candidate in candidates:
            if not isinstance(candidate, dict):
                results.append(
                    {
                        "success": False,
                        "status": "invalid_record",
                        "hostname": None,
                    }
                )
                continue

            hostname = candidate.get("hostname")

            try:
                if self.feedback_controller is None:
                    result = self.activator.activate(
                        hostname
                    )
                else:
                    try:
                        adapted_priority = (
                            self.feedback_controller
                            .adapt_candidate_priority(candidate)
                        )
                    except Exception:
                        adapted_priority = candidate.get(
                            "priority",
                            50.0,
                        )

                    result = self.activator.activate(
                        hostname,
                        priority=adapted_priority,
                    )

                if not isinstance(result, dict):
                    result = {
                        "success": False,
                        "status": "invalid_activation_result",
                        "hostname": hostname,
                    }

            except Exception as exc:
                result = {
                    "success": False,
                    "status": "activation_exception",
                    "hostname": hostname,
                    "error": str(exc),
                }

            results.append(result)

        activated = sum(
            1
            for result in results
            if result.get("success")
            and result.get("status")
            in {
                "activated",
                "already_activated",
                "already_queued",
                "already_in_pipeline",
            }
        )

        already_active = sum(
            1
            for result in results
            if result.get("status")
            == "already_activated"
        )

        failed = len(results) - activated

        with self._lock:
            self.stats["activation_batches"] += 1
            self.stats["activation_requested"] += len(
                results
            )
            self.stats["activated"] += activated
            self.stats["activation_failed"] += failed
            self.stats["already_active"] += (
                already_active
            )

        return {
            "requested": len(results),
            "activated": activated,
            "already_active": already_active,
            "failed": failed,
            "results": results,
        }

    # ============================================================
    # CYCLE
    # ============================================================

    def run_cycle(self):
        started_at = time.time()

        discovery = self.discover_once()

        activation = self.activate_once()

        feedback = None

        if self.feedback_controller is not None:
            try:
                feedback = self.feedback_controller.process(
                    limit=self.activation_batch_size,
                )
            except Exception:
                feedback = {
                    "processed": 0,
                    "results": [],
                    "error": True,
                }

        with self._lock:
            self.stats["cycles"] += 1

            self.stats["discovered"] += (
                discovery["discovered"]
            )

            self.stats["accepted"] += (
                discovery["accepted"]
            )

            self.stats["stored"] += (
                discovery["stored"]
            )

            self.stats["duplicates"] += (
                discovery["duplicates"]
            )

            self.stats["last_discovered"] = (
                discovery["discovered"]
            )

            self.stats["last_stored"] = (
                discovery["stored"]
            )

            self.stats["last_duplicates"] = (
                discovery["duplicates"]
            )

            self.stats["last_activation_requested"] = (
                activation["requested"]
            )

            self.stats["last_activated"] = (
                activation["activated"]
            )

            self.stats["last_activation_failed"] = (
                activation["failed"]
            )

            self.stats["last_cycle_at"] = time.time()

        return {
            "discovery": discovery,
            "activation": activation,
            "feedback": feedback,
            "duration": time.time() - started_at,
        }

    def process_once(self):
        return self.run_cycle()

    # ============================================================
    # CONTINUOUS EXECUTION
    # ============================================================

    def run(self, max_cycles=None):
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

    # ============================================================
    # STATUS
    # ============================================================

    def status(self):
        with self._lock:
            stats = dict(self.stats)
            context_index = self._context_index

        return {
            "running": self.running,
            "cycle_interval": self.cycle_interval,
            "max_contexts_per_cycle": (
                self.max_contexts_per_cycle
            ),
            "activation_batch_size": (
                self.activation_batch_size
            ),
            "contexts": len(self.contexts),
            "context_index": context_index,
            "stats": stats,
        }
