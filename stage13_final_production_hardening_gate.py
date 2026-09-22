import os
import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.health import StorageHealthMonitor
from index_storage.config import validate_storage_config
from index_storage.integrity_monitor import StorageIntegrityMonitor


root = tempfile.mkdtemp(prefix="our_search_m13_gate_")

try:
    # 1. Configuration validation
    validate_storage_config(root, max_object_size=1024 * 1024, max_key_length=128)
    try:
        validate_storage_config("", max_object_size=1)
        raise AssertionError("invalid configuration was accepted")
    except ValueError:
        pass
    print("CONFIG_VALIDATION: PASS")

    # 2. Core storage + resource limits
    storage = LocalIndexStorage(
        root=root,
        max_object_size=1024 * 1024,
        max_key_length=128,
    )

    storage.put("documents/a", b"hello")
    storage.put("documents/b", b"world")

    assert storage.get("documents/a") == b"hello"
    assert storage.exists("documents/b")
    print("CORE_STORAGE: PASS")

    try:
        storage.put("x" * 129, b"bad")
        raise AssertionError("oversized key accepted")
    except ValueError:
        pass

    try:
        storage.put("oversized", b"x" * (1024 * 1024 + 1))
        raise AssertionError("oversized object accepted")
    except ValueError:
        pass

    print("RESOURCE_LIMITS: PASS")

    # 3. Health monitoring
    health = StorageHealthMonitor(storage).check()
    assert health.healthy
    assert health.objects == 2
    print("HEALTH_MONITORING: PASS")

    # 4. Integrity monitoring
    integrity = StorageIntegrityMonitor(storage).scan()
    assert integrity.checked == 2
    assert integrity.healthy == 2
    assert integrity.corrupted == 0
    print("INTEGRITY_MONITORING: PASS")

    # 5. Metrics
    metrics = storage.metrics.snapshot()
    assert metrics.puts == 2
    assert metrics.gets >= 1
    assert metrics.exists >= 1
    assert metrics.errors == 2
    print("METRICS: PASS")

    # 6. Safe close + persistence
    storage.close()
    storage.close()

    try:
        storage.put("after-close", b"rejected")
        raise AssertionError("operation accepted after close")
    except RuntimeError:
        pass

    reopened = LocalIndexStorage(
        root=root,
        max_object_size=1024 * 1024,
        max_key_length=128,
    )

    assert reopened.get("documents/a") == b"hello"
    assert reopened.get("documents/b") == b"world"
    print("SAFE_CLOSE_AND_RESTART: PASS")

    # 7. Integrity after restart
    integrity_after_restart = StorageIntegrityMonitor(reopened).scan()
    assert integrity_after_restart.checked == 2
    assert integrity_after_restart.healthy == 2
    print("RESTART_INTEGRITY: PASS")

    # 8. Recovery/WAL presence check
    wal_path = os.path.join(root, "storage.wal")
    assert os.path.exists(wal_path)
    assert os.path.getsize(wal_path) == 0
    print("WAL_RECOVERY_STATE: PASS")

    # 9. Final health
    final_health = StorageHealthMonitor(reopened).check()
    assert final_health.healthy
    print("FINAL_HEALTH: PASS")

    reopened.close()

    print()
    print("========================================")
    print("MILESTONE 13 PRODUCTION HARDENING GATE: PASS")
    print("========================================")

finally:
    shutil.rmtree(root, ignore_errors=True)
