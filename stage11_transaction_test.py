import os
import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.transactions import TransactionalIndexStorage


def open_storage(root):
    return TransactionalIndexStorage(
        LocalIndexStorage(root)
    )


def main():
    root = tempfile.mkdtemp(
        prefix="stage11_transactions_"
    )

    try:
        storage = open_storage(root)

        # Basic atomic commit.
        tx = storage.begin_transaction()

        tx.put(
            "documents/a",
            b"alpha",
        )

        tx.put(
            "documents/b",
            b"beta",
        )

        assert tx.get(
            "documents/a"
        ) == b"alpha"

        tx.commit()

        assert storage.get(
            "documents/a"
        ) == b"alpha"

        assert storage.get(
            "documents/b"
        ) == b"beta"

        print(
            "PASS: committed transaction applied"
        )

        # Rollback.
        tx = storage.begin_transaction()

        tx.put(
            "documents/c",
            b"gamma",
        )

        tx.delete(
            "documents/a"
        )

        tx.rollback()

        assert storage.get(
            "documents/c"
        ) is None

        assert storage.get(
            "documents/a"
        ) == b"alpha"

        print(
            "PASS: rollback discarded transaction"
        )

        # Crash before commit.
        txid = "crash-before-commit"

        storage.wal.append({
            "type": "begin",
            "transaction": txid,
        })

        storage.wal.append({
            "type": "put",
            "transaction": txid,
            "key": "documents/uncommitted",
            "data": b"discard-me".hex(),
        })

        storage = open_storage(root)

        assert storage.get(
            "documents/uncommitted"
        ) is None

        print(
            "PASS: crash before commit discarded transaction"
        )

        # Crash after durable COMMIT but before application.
        txid = "crash-after-commit"

        storage.wal.append({
            "type": "begin",
            "transaction": txid,
        })

        storage.wal.append({
            "type": "put",
            "transaction": txid,
            "key": "documents/recovery-a",
            "data": b"A".hex(),
        })

        storage.wal.append({
            "type": "put",
            "transaction": txid,
            "key": "documents/recovery-b",
            "data": b"B".hex(),
        })

        storage.wal.append({
            "type": "commit",
            "transaction": txid,
        })

        # Simulate process death by creating a fresh storage instance.
        storage = open_storage(root)

        assert storage.get(
            "documents/recovery-a"
        ) == b"A"

        assert storage.get(
            "documents/recovery-b"
        ) == b"B"

        print(
            "PASS: crash after commit recovered transaction"
        )

        # Operations are no longer allowed after commit.
        tx = storage.begin_transaction()
        tx.put(
            "documents/state",
            b"value",
        )
        tx.commit()

        try:
            tx.put(
                "documents/state",
                b"invalid",
            )
            raise AssertionError(
                "operation after commit was accepted"
            )
        except RuntimeError:
            pass

        print(
            "PASS: transaction state becomes immutable after commit"
        )

        print()
        print("=" * 56)
        print(
            "MILESTONE 11 LOCAL TRANSACTION TEST PASSED"
        )
        print("=" * 56)

    finally:
        shutil.rmtree(
            root,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
