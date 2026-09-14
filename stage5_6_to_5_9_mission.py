from __future__ import annotations

import os
import sys
import time
import tempfile
import shutil
import subprocess
from pathlib import Path

from crawler_system.url_state import URLStateStore
from crawler_system.frontier import CrawlFrontier
from crawler_system.partition_router import PartitionRouter
from crawler_system.partition_registry import PartitionRegistry
from crawler_system.partitioned_frontier import PartitionedFrontier
from crawler_system.fault_tolerance import FaultToleranceManager
from crawler_system.distributed_persistence import DistributedPersistenceManager


ROOT = Path(__file__).resolve().parent


def check(name, condition):
    if not condition:
        raise AssertionError(name)
    print(f"PASS — {name}")


def run_existing_gate(filename):
    path = ROOT / filename
    print("=" * 72)
    print(f"REGRESSION: {filename}")
    print("=" * 72)

    result = subprocess.run(
        [sys.executable, str(path)],
        cwd=str(ROOT),
    )

    check(f"{filename} exit=0", result.returncode == 0)


def stage56():
    print("\n" + "=" * 72)
    print("STAGE 5.6 — FAULT TOLERANCE")
    print("=" * 72)

    workspace = Path(
        tempfile.mkdtemp(prefix="our_search_stage5_6_")
    )

    try:
        db = workspace / "state.db"

        store = URLStateStore(
            database_path=str(db)
        )

        frontier = CrawlFrontier(
            state_store=store,
            default_delay=0,
            max_retries=3,
        )

        url = "https://fault-tolerance.example/recovery"

        check(
            "fault-tolerance URL insertion",
            frontier.add(url, priority=80),
        )

        claimed = frontier.get_next()

        check(
            "lease acquired",
            claimed == url,
        )

        manager = FaultToleranceManager(
            frontier,
            lease_timeout=0,
        )

        report = manager.recover_expired_leases(
            now=time.time() + 1
        )

        check(
            "expired lease recovered",
            report.recovered_leases == 1,
        )

        state = store.get(url)

        check(
            "recovered URL returned to retry state",
            state is not None and state["state"] == "retry",
        )

        check(
            "lease owner cleared",
            state is not None and state["lease_owner"] is None,
        )

        check(
            "leased timestamp cleared",
            state is not None and state["leased_at"] is None,
        )

        check(
            "attempt incremented during recovery",
            state is not None and int(state["attempts"]) == 1,
        )

        check(
            "fault-tolerance invariants",
            manager.validate(),
        )

        second = manager.recover_expired_leases(
            now=time.time() + 2
        )

        check(
            "recovery is idempotent",
            second.recovered_leases == 0,
        )

        store.close()

        reopened = URLStateStore(
            database_path=str(db)
        )

        persisted = reopened.get(url)

        check(
            "recovered state survives restart",
            persisted is not None
            and persisted["state"] == "retry",
        )

        reopened.close()

    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    print("STAGE 5.6 RESULT: PASS")


def stage57():
    print("\n" + "=" * 72)
    print("STAGE 5.7 — DISTRIBUTED PERSISTENCE")
    print("=" * 72)

    workspace = Path(
        tempfile.mkdtemp(prefix="our_search_stage5_7_")
    )

    try:
        router = PartitionRouter(
            partition_count=8,
            database_root=str(workspace / "partitions"),
        )

        registry = PartitionRegistry(router)

        # PartitionRegistry is intentionally lazy. Open every partition
        # through its authoritative public API before validating coverage.
        for partition_id in registry.partitions():
            registry.store_for_partition(partition_id)

        stores = registry.stores()

        check(
            "all partition stores initialized",
            len(stores) == 8,
        )

        for partition_id, store in stores.items():
            check(
                f"partition {partition_id} readable",
                store.count() >= 0,
            )

        manager = DistributedPersistenceManager(
            registry
        )

        manifest = manager.manifest()

        check(
            "manifest covers all partitions",
            len(manifest["partitions"]) == 8,
        )

        check(
            "manifest partition count",
            manifest["partition_count"] == 8,
        )

        manifest_path = (
            workspace / "persistence_manifest.json"
        )

        saved = manager.save_manifest(
            manifest_path
        )

        check(
            "manifest persisted",
            manifest_path.exists()
            and saved["partition_count"] == 8,
        )

        loaded = manager.load_manifest(
            manifest_path
        )

        check(
            "manifest restored",
            loaded == saved,
        )

        check(
            "live partition validation",
            manager.validate_live_partitions(),
        )

        check(
            "persistence validation",
            manager.validate(),
        )

        # The checksum is a snapshot checksum. Validate immediately,
        # before intentionally performing any database writes.
        snapshot_manager = DistributedPersistenceManager(
            registry
        )

        snapshot = snapshot_manager.manifest()

        snapshot_path = (
            workspace / "snapshot_manifest.json"
        )

        snapshot_manager.save_manifest(
            snapshot_path
        )

        loaded_snapshot = (
            snapshot_manager.load_manifest(
                snapshot_path
            )
        )

        check(
            "snapshot manifest validates unchanged state",
            snapshot_manager.validate_manifest(
                loaded_snapshot
            ),
        )

        registry.close()

        # Reopen the complete partition set.
        reopened_registry = PartitionRegistry(
            router
        )

        reopened_manager = (
            DistributedPersistenceManager(
                reopened_registry
            )
        )

        check(
            "all partitions reopen after restart",
            reopened_manager.validate_live_partitions(),
        )

        reopened_registry.close()

    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    print("STAGE 5.7 RESULT: PASS")


def stage58():
    print("\n" + "=" * 72)
    print("STAGE 5.8 — WEB-SCALE STRESS / SUSTAINED PIPELINE")
    print("=" * 72)

    workspace = Path(
        tempfile.mkdtemp(prefix="our_search_stage5_8_")
    )

    try:
        router = PartitionRouter(
            partition_count=8,
            database_root=str(workspace / "partitions"),
        )

        frontier = PartitionedFrontier(
            partition_count=8,
            storage_root=str(workspace / "partitions"),
            default_delay=0,
        )

        total_urls = 20000

        started = time.monotonic()

        for index in range(total_urls):
            host = f"host-{index % 400}.example"
            url = f"https://{host}/page/{index}"

            added = frontier.add(
                url,
                priority=(index % 100),
            )

            check(
                f"stress insertion {index}",
                added,
            )

        inserted = frontier.size()

        check(
            "20,000 URLs inserted",
            inserted == total_urls,
        )

        claimed = 0
        completed = 0

        while True:
            url = frontier.get_next()

            if url is None:
                break

            claimed += 1

            check(
                "claimed URL exists",
                isinstance(url, str) and url.startswith("https://"),
            )

            completed_ok = frontier.complete(url)

            check(
                "claimed URL completes",
                completed_ok,
            )

            completed += 1

        elapsed = time.monotonic() - started

        check(
            "all stress URLs claimed",
            claimed == total_urls,
        )

        check(
            "all stress URLs completed",
            completed == total_urls,
        )

        check(
            "frontier empty after stress",
            frontier.size() == 0,
        )

        counts = frontier.counts()

        check(
            "no queued URLs remain",
            counts.get("queued", 0) == 0,
        )

        check(
            "no leased URLs remain",
            counts.get("leased", 0) == 0,
        )

        check(
            "completed state retained",
            counts.get("crawled", 0) == total_urls,
        )

        throughput = (
            total_urls / elapsed
            if elapsed > 0
            else 0
        )

        print(
            f"STRESS_URLS={total_urls}"
        )
        print(
            f"STRESS_ELAPSED={elapsed:.3f}s"
        )
        print(
            f"STRESS_THROUGHPUT={throughput:.2f}/s"
        )

        frontier.close()

        # Restart verification.
        restarted = PartitionedFrontier(
            partition_count=8,
            storage_root=str(workspace / "partitions"),
            default_delay=0,
        )

        restart_counts = restarted.counts()

        check(
            "stress state survives restart",
            restart_counts.get("crawled", 0)
            == total_urls,
        )

        check(
            "restart frontier remains empty",
            restarted.size() == 0,
        )

        restarted.close()

    finally:
        shutil.rmtree(workspace, ignore_errors=True)

    print("STAGE 5.8 RESULT: PASS")


def stage59():
    print("\n" + "=" * 72)
    print("STAGE 5.9 — FINAL STAGE 5 GATE")
    print("=" * 72)

    required = [
        "crawler_system/fault_tolerance.py",
        "crawler_system/distributed_persistence.py",
        "crawler_system/url_state.py",
        "crawler_system/frontier.py",
        "crawler_system/partitioned_frontier.py",
        "crawler_system/partition_registry.py",
        "crawler_system/partition_router.py",
        "crawler_system/coordinator.py",
        "crawler_system/worker_pool.py",
        "crawler_system/worker.py",
        "crawler_system/distributed_communication.py",
        "crawler_system/distributed_dispatcher.py",
        "crawler_system/worker_ownership.py",
    ]

    for relative in required:
        path = ROOT / relative

        check(
            f"required production file exists: {relative}",
            path.exists(),
        )

    compile_targets = [
        str(ROOT / relative)
        for relative in required
    ]

    result = subprocess.run(
        [sys.executable, "-m", "py_compile"]
        + compile_targets,
        cwd=str(ROOT),
    )

    check(
        "final production compilation",
        result.returncode == 0,
    )

    print("\n" + "=" * 72)
    print("STAGE 5 FINAL OVERALL GATE")
    print("=" * 72)

    print("5.1.2 PARTITION ARCHITECTURE: PASS")
    print("5.2   MASSIVE FRONTIER: PASS")
    print("5.3   HIGH-THROUGHPUT FETCHING: PASS")
    print("5.4   DISTRIBUTED WORKERS: PASS")
    print("5.5   DOMAIN-AWARE SCHEDULING: PASS")
    print("5.6   FAULT TOLERANCE: PASS")
    print("5.7   DISTRIBUTED PERSISTENCE: PASS")
    print("5.8   WEB-SCALE STRESS: PASS")
    print("5.9   FINAL GATE: PASS")
    print("=" * 72)
    print("RESULT: PASS")
    print("STAGE 5: 100% COMPLETE")
    print("READY FOR STAGE 6")
    print("=" * 72)


def main():
    print("=" * 72)
    print("OUR SEARCH — STAGE 5.6 → 5.9 ONE-SHOT ENGINEERING MISSION")
    print("=" * 72)

    # Locked regressions first.
    run_existing_gate("stage5_1_2_final_gate.py")
    run_existing_gate("stage5_2_final_gate.py")
    run_existing_gate("stage5_3_final_gate.py")
    run_existing_gate("stage5_4_5_final_gate.py")
    run_existing_gate("stage5_5_final_gate.py")

    stage56()
    stage57()
    stage58()
    stage59()

    print("\nSTAGE5_ONE_SHOT_MISSION_EXIT=0")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\n" + "=" * 72)
        print("STAGE 5 ONE-SHOT MISSION FAILED")
        print("=" * 72)
        print(f"ERROR: {exc}")
        print("STAGE5_ONE_SHOT_MISSION_EXIT=1")
        raise
