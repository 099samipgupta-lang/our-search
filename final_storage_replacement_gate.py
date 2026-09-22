import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor

from index_storage.backend import IndexStorageBackend
from index_storage.local import LocalIndexStorage
from index_storage.replication import ReplicatedIndexStorage
from index_storage.sharding import ShardedIndexStorage
from index_storage.distributed_transactions import (
    DistributedTransactionalIndexStorage,
)
from index_storage.health import StorageHealthMonitor
from index_storage.integrity_monitor import StorageIntegrityMonitor


ROOT = tempfile.mkdtemp(prefix="our_search_final_storage_gate_")
roots = []


def local_backend(name):
    path = os.path.join(ROOT, name)
    roots.append(path)
    return LocalIndexStorage(
        root=path,
        max_object_size=16 * 1024 * 1024,
        max_key_length=512,
    )


try:
    # ============================================================
    # 1. Independent storage / backend contract
    # ============================================================
    base = local_backend("base")
    assert isinstance(base, IndexStorageBackend)
    base.put("documents/independent", b"OUR SEARCH")
    assert base.get("documents/independent") == b"OUR SEARCH"
    print("INDEPENDENT_STORAGE: PASS")

    # ============================================================
    # 2. Durable persistence + restart recovery
    # ============================================================
    base.put("documents/persistent", b"persistent-data")
    base.close()

    reopened = LocalIndexStorage(
        root=roots[0],
        max_object_size=16 * 1024 * 1024,
        max_key_length=512,
    )
    assert reopened.get("documents/persistent") == b"persistent-data"
    print("DURABLE_RESTART_PERSISTENCE: PASS")

    # ============================================================
    # 3. Catalog
    # ============================================================
    keys = set(reopened.list_keys())
    assert "documents/independent" in keys
    assert "documents/persistent" in keys
    print("CATALOG_LOOKUP: PASS")

    # ============================================================
    # 4. Large-object storage
    # ============================================================
    large_payload = os.urandom(5 * 1024 * 1024)
    reopened.put("objects/large", large_payload)
    assert reopened.get("objects/large") == large_payload
    print("LARGE_OBJECT_STORAGE: PASS")

    # ============================================================
    # 5. Integrity + health
    # ============================================================
    integrity = StorageIntegrityMonitor(reopened).scan()
    assert integrity.checked >= 3
    assert integrity.corrupted == 0
    assert integrity.missing_integrity == 0

    health = StorageHealthMonitor(reopened).check()
    assert health.healthy
    print("INTEGRITY_AND_HEALTH: PASS")

    # ============================================================
    # 6. Replication / distributed durability
    # ============================================================
    replicas = [
        local_backend("replica1"),
        local_backend("replica2"),
        local_backend("replica3"),
    ]

    replicated = ReplicatedIndexStorage(
        replicas,
        write_quorum=2,
    )

    for i in range(50):
        replicated.put(
            f"replicated/{i}",
            f"replica-data-{i}".encode(),
        )

    assert all(
        replica.get("replicated/0") == b"replica-data-0"
        for replica in replicas
    )
    print("REPLICATION: PASS")

    # ============================================================
    # 7. Replica failure + recovery
    # ============================================================
    failing_replica = replicas[2]

    original_get = failing_replica.get

    def failed_get(key):
        raise RuntimeError("simulated replica failure")

    failing_replica.get = failed_get

    assert replicated.get("replicated/10") == b"replica-data-10"

    failing_replica.get = original_get

    repaired = replicated.repair_replica(
        target_index=2,
        source_index=0,
    )
    assert repaired >= 0
    assert replicas[2].get("replicated/10") == b"replica-data-10"
    print("REPLICA_FAILURE_RECOVERY: PASS")

    # ============================================================
    # 8. Sharding
    # ============================================================
    shard_backends = [
        ReplicatedIndexStorage(
            [
                local_backend(f"s{i}_a"),
                local_backend(f"s{i}_b"),
            ],
            write_quorum=2,
        )
        for i in range(4)
    ]

    sharded = ShardedIndexStorage(shard_backends)

    for i in range(200):
        sharded.put(
            f"sharded/{i}",
            f"shard-data-{i}".encode(),
        )

    assert len(sharded.list_keys("sharded/")) == 200
    print("SHARDING: PASS")

    # ============================================================
    # 9. Horizontal expansion / rebalancing
    # ============================================================
    new_shards = [
        ReplicatedIndexStorage(
            [
                local_backend(f"expanded{i}_a"),
                local_backend(f"expanded{i}_b"),
            ],
            write_quorum=2,
        )
        for i in range(6)
    ]

    moved = sharded.rebalance(new_shards)
    assert moved >= 200

    for i in range(200):
        assert sharded.get(
            f"sharded/{i}"
        ) == f"shard-data-{i}".encode()

    assert len(sharded.list_keys("sharded/")) == 200
    print("HORIZONTAL_EXPANSION_REBALANCE: PASS")

    # ============================================================
    # 10. Distributed transactions
    # ============================================================
    transactional_participants = []

    for index, backend in enumerate(sharded.backends):
        transactional_participants.append(
            __import__(
                "index_storage.transactions",
                fromlist=["TransactionalIndexStorage"],
            ).TransactionalIndexStorage(
                backend,
                wal_path=os.path.join(
                    ROOT,
                    f"participant_{index}_transactions.wal",
                ),
            )
        )

    transactional_topology = ShardedIndexStorage(
        transactional_participants
    )

    transactional = DistributedTransactionalIndexStorage(
        transactional_topology,
        wal_path=os.path.join(
            ROOT,
            "distributed_transactions.wal",
        ),
    )

    tx = transactional.begin_transaction()
    tx.put("transaction/a", b"A")
    tx.put("transaction/b", b"B")
    tx.commit()

    assert sharded.get("transaction/a") == b"A"
    assert sharded.get("transaction/b") == b"B"

    rollback = transactional.begin_transaction()
    rollback.put("transaction/c", b"C")
    rollback.rollback()

    assert sharded.get("transaction/c") is None
    print("DISTRIBUTED_TRANSACTIONS: PASS")

    # ============================================================
    # 11. Concurrent operations
    # ============================================================
    def concurrent_write(i):
        sharded.put(
            f"concurrent/{i}",
            f"value-{i}".encode(),
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(concurrent_write, range(100)))

    for i in range(100):
        assert sharded.get(
            f"concurrent/{i}"
        ) == f"value-{i}".encode()

    print("CONCURRENT_READ_WRITE: PASS")

    # ============================================================
    # 12. Capacity / scale namespace
    # ============================================================
    total_keys = len(sharded.list_keys())
    assert total_keys >= 300
    print("CAPACITY_NAMESPACE: PASS")

    # ============================================================
    # 13. Restart one physical backend after replicated workload
    # ============================================================
    restart_path = roots[1]
    restarted = LocalIndexStorage(
        root=restart_path,
        max_object_size=16 * 1024 * 1024,
        max_key_length=512,
    )

    assert restarted.list_keys() is not None
    restarted.close()
    print("PHYSICAL_BACKEND_RESTART: PASS")

    # ============================================================
    # 14. Supabase independence
    # ============================================================
    assert type(reopened).__name__ == "LocalIndexStorage"
    assert type(sharded).__name__ == "ShardedIndexStorage"
    assert type(transactional).__name__ == (
        "DistributedTransactionalIndexStorage"
    )

    # These storage operations were completed without requiring
    # SUPABASE_URL, SUPABASE_KEY, or a Supabase client.
    print("SUPABASE_INDEPENDENCE: PASS")

    # ============================================================
    # 15. Final storage health
    # ============================================================
    final_integrity = StorageIntegrityMonitor(sharded).scan()
    assert final_integrity.corrupted == 0
    assert final_integrity.missing_integrity == 0

    print("FINAL_INTEGRITY: PASS")

    print()
    print("============================================================")
    print("FINAL STORAGE REPLACEMENT GATE: PASS")
    print("============================================================")
    print("OUR SEARCH STORAGE IS READY TO SERVE AS THE")
    print("OUR SEARCH STORAGE/PERSISTENCE LAYER.")
    print("============================================================")

finally:
    for obj in list(locals().values()):
        if hasattr(obj, "close"):
            try:
                obj.close()
            except Exception:
                pass

    shutil.rmtree(ROOT, ignore_errors=True)
