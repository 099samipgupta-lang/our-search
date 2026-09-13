import subprocess
import sys


def main():
    print("=" * 72)
    print("STAGE 5.1.2 — FINAL PARTITION ARCHITECTURE GATE")
    print("=" * 72)

    tests = [
        "stage5_1_2_partition_architecture_test.py",
        "stage5_1_2_partition_concurrency_stress.py",
    ]

    for test in tests:
        print()
        print(f"RUNNING: {test}")
        print("-" * 72)

        result = subprocess.run(
            [sys.executable, test],
            check=False,
        )

        if result.returncode != 0:
            print()
            print("=" * 72)
            print("RESULT: FAIL")
            print(f"FAILED TEST: {test}")
            print("=" * 72)
            raise SystemExit(result.returncode)

    print()
    print("=" * 72)
    print("STAGE 5.1.2 FINAL RESULT: PASS")
    print("STAGE 5.1.2 PARTITION ARCHITECTURE: 100% COMPLETE")
    print("=" * 72)


if __name__ == "__main__":
    main()
