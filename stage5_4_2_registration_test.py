from crawler_system.worker_registry import WorkerRegistry


def main():
    registry = WorkerRegistry()

    node = registry.register("node-a", 4, now=100.0)

    assert node.node_id == "node-a"
    assert node.worker_count == 4
    assert node.active
    assert registry.count() == 1
    assert registry.total_worker_capacity() == 4

    try:
        registry.register("node-a", 8, now=101.0)
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate active registration accepted")

    registry.heartbeat("node-a", now=102.0)
    assert registry.get("node-a").last_heartbeat == 102.0

    registry.update_worker_count("node-a", 8)
    assert registry.get("node-a").worker_count == 8

    registry.unregister("node-a")
    assert not registry.get("node-a").active
    assert registry.count() == 0
    assert registry.count(active_only=False) == 1

    registry.register("node-a", 4, now=103.0)
    assert registry.get("node-a").active

    for bad_id in ("", "   "):
        try:
            registry.register(bad_id, 1)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid node id accepted")

    for bad_count in (0, -1):
        try:
            registry.register("bad", bad_count)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid worker count accepted")

    print("STAGE 5.4.2 REGISTRATION TEST: PASS")


if __name__ == "__main__":
    main()
