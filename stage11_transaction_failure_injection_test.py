import shutil
import tempfile

from index_storage.local import LocalIndexStorage
from index_storage.transactions import TransactionalIndexStorage


class CrashAfterOneOperationStorage(LocalIndexStorage):
    def __init__(self, root):
        self._operation_count = 0
        super().__init__(root)

    def put(self, key, data):
        self._operation_count += 1

        if self._operation_count > 1:
            raise RuntimeError("simulated process crash")

        return super().put(key, data)

    def delete(self, key):
        self._operation_count += 1

        if self._operation_count > 1:
            raise RuntimeError("simulated process crash")

        return super().delete(key)


def main():
    root = tempfile.mkdtemp(
        prefix="stage11_failure_injection_"
    )

    try:
        storage = TransactionalIndexStorage(
            CrashAfterOneOperationStorage(root)
        )

        tx = storage.begin_transaction()

        tx.put(
            "documents/failure-a",
            b"A",
        )

        tx.put(
            "documents/failure-b",
            b"B",
        )

        try:
            tx.commit()
            raise AssertionError(
                "simulated crash did not occur"
            )
        except RuntimeError as exc:
            assert "simulated process crash" in str(exc)

        print(
            "PASS: crash occurred during transaction application"
        )

        # Restart using normal storage. The durable COMMIT record
        # must cause recovery to finish the transaction.
        storage = TransactionalIndexStorage(
            LocalIndexStorage(root)
        )

        assert storage.get(
            "documents/failure-a"
        ) == b"A"

        assert storage.get(
            "documents/failure-b"
        ) == b"B"

        print(
            "PASS: recovery completed partially applied transaction"
        )

        assert storage.get(
            "documents/failure-a"
        ) == b"A"

        assert storage.get(
            "documents/failure-b"
        ) == b"B"

        print(
            "PASS: final transaction state is complete and consistent"
        )

        print()
        print("=" * 60)
        print(
            "MILESTONE 11 FAILURE-INJECTION TEST PASSED"
        )
        print("=" * 60)

    finally:
        shutil.rmtree(
            root,
            ignore_errors=True,
        )


if __name__ == "__main__":
    main()
