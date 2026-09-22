import json
import os
import tempfile

from index_storage.catalog import StorageCatalog


def test_snapshot_and_journal_compaction():
    with tempfile.TemporaryDirectory() as root:
        catalog = StorageCatalog(root)

        catalog.put("documents/a", {"size": 100})
        catalog.put("documents/b", {"size": 200})
        catalog.delete("documents/a")

        catalog.compact()

        snapshot = os.path.join(root, "storage.catalog")
        journal = os.path.join(root, "storage.catalog.log")

        assert os.path.exists(snapshot)
        assert os.path.exists(journal)
        assert open(journal, "rb").read() == b""

        reopened = StorageCatalog(root)

        assert reopened.get("documents/a") is None
        assert reopened.get("documents/b") == {"size": 200}
        assert reopened.list_keys() == ["documents/b"]


def test_compaction_is_repeatable():
    with tempfile.TemporaryDirectory() as root:
        catalog = StorageCatalog(root)

        for i in range(100):
            catalog.put(
                f"documents/{i}",
                {"size": i},
            )

        for _ in range(5):
            catalog.compact()

        reopened = StorageCatalog(root)

        assert reopened.count() == 100
        assert reopened.get("documents/0") == {"size": 0}
        assert reopened.get("documents/99") == {"size": 99}


def test_stale_snapshot_replays_journal():
    with tempfile.TemporaryDirectory() as root:
        catalog = StorageCatalog(root)

        catalog.put("documents/a", {"size": 100})
        catalog.compact()

        catalog.put("documents/b", {"size": 200})

        reopened = StorageCatalog(root)

        assert reopened.get("documents/a") == {"size": 100}
        assert reopened.get("documents/b") == {"size": 200}


def test_partial_final_record_recovery():
    with tempfile.TemporaryDirectory() as root:
        journal = os.path.join(root, "storage.catalog.log")

        valid = json.dumps(
            {
                "operation": "put",
                "key": "documents/a",
                "record": {"size": 100},
            },
            separators=(",", ":"),
        ).encode()

        partial = (
            b'{"operation":"put",'
            b'"key":"documents/b",'
            b'"record":'
        )

        with open(journal, "wb") as file:
            file.write(valid + b"\n" + partial)
            file.flush()
            os.fsync(file.fileno())

        catalog = StorageCatalog(root)

        assert catalog.get("documents/a") == {"size": 100}
        assert catalog.get("documents/b") is None

        with open(journal, "rb") as file:
            assert file.read() == valid + b"\n"


def test_corrupted_complete_record_fails():
    with tempfile.TemporaryDirectory() as root:
        journal = os.path.join(root, "storage.catalog.log")

        valid = json.dumps(
            {
                "operation": "put",
                "key": "documents/a",
                "record": {"size": 100},
            },
            separators=(",", ":"),
        ).encode()

        corrupted = b'{"operation":"put","key":'

        with open(journal, "wb") as file:
            file.write(valid + b"\n" + corrupted + b"\n")
            file.flush()
            os.fsync(file.fileno())

        try:
            StorageCatalog(root)
        except ValueError as exc:
            assert "corrupted catalog journal entry" in str(exc)
        else:
            raise AssertionError(
                "corrupted complete journal record was accepted"
            )


def test_delete_survives_restart():
    with tempfile.TemporaryDirectory() as root:
        catalog = StorageCatalog(root)

        catalog.put("documents/a", {"size": 100})
        catalog.put("documents/b", {"size": 200})
        assert catalog.delete("documents/a") is True

        reopened = StorageCatalog(root)

        assert reopened.exists("documents/a") is False
        assert reopened.exists("documents/b") is True
        assert reopened.count() == 1


def test_large_catalog():
    with tempfile.TemporaryDirectory() as root:
        catalog = StorageCatalog(root)

        for i in range(2000):
            catalog.put(
                f"documents/{i:05d}",
                {"size": i},
            )

        assert catalog.count() == 2000
        assert len(catalog.list_keys("documents/")) == 2000

        catalog.compact()

        reopened = StorageCatalog(root)

        assert reopened.count() == 2000
        assert reopened.get("documents/01000") == {"size": 1000}


def run():
    tests = [
        test_snapshot_and_journal_compaction,
        test_compaction_is_repeatable,
        test_stale_snapshot_replays_journal,
        test_partial_final_record_recovery,
        test_corrupted_complete_record_fails,
        test_delete_survives_restart,
        test_large_catalog,
    ]

    for test in tests:
        test()
        print(f"PASS: {test.__name__}")

    print()
    print("========================================")
    print("MILESTONE 8 CATALOG REGRESSION PASSED")
    print("========================================")


if __name__ == "__main__":
    run()
