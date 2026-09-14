import py_compile
import tempfile
from pathlib import Path

from crawler_system.worker_registry import WorkerRegistry


FILES = [
    "crawler_system/worker_registry.py",
    "stage5_4_2_registration_test.py",
    "stage5_4_2_multi_node_registration_test.py",
    "stage5_4_2_persistence_test.py",
    "stage5_4_2_final_gate.py",
]


def compile_gate():
    for file in FILES:
        py_compile.compile(file, doraise=True)
    print("TEST 1 — Python compilation: PASS")


def registration_gate():
    registry = WorkerRegistry()

    registry.register("node-a", 4, now=10.0)
    registry.register("node-b", 8, now=20.0)

    assert registry.count() == 2
    assert registry.total_worker_capacity() == 12

    try:
        registry.register("node-a", 16, now=30.0)
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate active registration accepted")

    registry.heartbeat("node-a", now=40.0)
    assert registry.get("node-a").last_heartbeat == 40.0

    registry.unregister("node-b")
    assert registry.count() == 1
    assert registry.count(active_only=False) == 2

    print("TEST 2 — Registration lifecycle: PASS")


def multi_node_gate():
    registry = WorkerRegistry()

    for index in range(16):
        registry.register(f"node-{index:02d}", index + 1, now=float(index))

    assert registry.count() == 16
    assert len(registry.node_ids()) == 16
    assert registry.total_worker_capacity() == sum(range(1, 17))
    registry.validate()

    print("TEST 3 — Multi-node coordination registry: PASS")


def persistence_gate():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "registry.json"

        registry = WorkerRegistry(cluster_id="our-search")
        registry.register("node-a", 4, now=100.0)
        registry.register("node-b", 8, now=200.0)
        registry.heartbeat("node-a", now=150.0)
        registry.unregister("node-b")

        registry.save(path)
        restored = WorkerRegistry.load(path)

        assert restored.snapshot() == registry.snapshot()
        assert restored.validate()

    print("TEST 4 — Persistence and restoration: PASS")


def concurrency_gate():
    import threading

    registry = WorkerRegistry()
    registry.register("node-a", 4, now=1.0)

    errors = []

    def heartbeat():
        try:
            for i in range(100):
                registry.heartbeat("node-a", now=10.0 + i)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=heartbeat) for _ in range(8)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert not errors
    assert registry.get("node-a").active
    assert registry.get("node-a").last_heartbeat >= 109.0

    print("TEST 5 — Concurrent registry updates: PASS")


def main():
    compile_gate()
    registration_gate()
    multi_node_gate()
    persistence_gate()
    concurrency_gate()

    print()
    print("RESULT: PASS")
    print("STAGE 5.4.2 WORKER REGISTRATION & COORDINATION: 100% COMPLETE")


if __name__ == "__main__":
    main()
