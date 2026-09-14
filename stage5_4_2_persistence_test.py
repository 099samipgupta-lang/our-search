import tempfile
from pathlib import Path

from crawler_system.worker_registry import WorkerRegistry


def main():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "worker_registry.json"

        registry = WorkerRegistry(cluster_id="persistence-test")
        registry.register("node-a", 4, now=100.0)
        registry.register("node-b", 8, now=200.0)
        registry.heartbeat("node-a", now=150.0)
        registry.unregister("node-b")

        registry.save(path)

        restored = WorkerRegistry.load(path)

        assert restored.cluster_id == registry.cluster_id
        assert restored.snapshot() == registry.snapshot()
        assert restored.node_ids() == registry.node_ids()
        assert restored.node_ids(active_only=False) == registry.node_ids(
            active_only=False
        )
        assert restored.total_worker_capacity() == registry.total_worker_capacity()
        assert restored.total_worker_capacity(
            active_only=False
        ) == registry.total_worker_capacity(active_only=False)

        restored.validate()

        try:
            WorkerRegistry.load(path.with_name("missing.json"))
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("missing registry file did not fail")

    print("STAGE 5.4.2 PERSISTENCE TEST: PASS")


if __name__ == "__main__":
    main()
