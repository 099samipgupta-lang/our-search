import subprocess
import sys


TESTS = [
    (
        "Architecture",
        "stage5_4_1_architecture_test.py",
    ),
    (
        "Multi-node ownership",
        "stage5_4_1_multi_node_test.py",
    ),
    (
        "Persistence/determinism",
        "stage5_4_1_persistence_determinism_test.py",
    ),
]


def run_test(name, script):
    print("=" * 72)
    print(f"RUNNING: {name}")
    print("=" * 72)

    result = subprocess.run(
        [sys.executable, script],
        text=True,
    )

    if result.returncode != 0:
        print(
            f"FAIL — {name}"
        )
        return False

    print(
        f"PASS — {name}"
    )
    return True


def main():
    print("=" * 72)
    print("STAGE 5.4.1 — FINAL DISTRIBUTED WORKER OWNERSHIP GATE")
    print("=" * 72)

    all_passed = True

    for name, script in TESTS:
        try:
            passed = run_test(
                name,
                script,
            )
        except Exception as error:
            print(
                f"FAIL — {name}: {error!r}"
            )
            passed = False

        all_passed = (
            all_passed and passed
        )

    print("=" * 72)

    if all_passed:
        print("RESULT: PASS")
        print(
            "STAGE 5.4.1 DISTRIBUTED WORKER ARCHITECTURE: 100% COMPLETE"
        )
        return 0

    print("RESULT: FAIL")
    print(
        "STAGE 5.4.1 DISTRIBUTED WORKER ARCHITECTURE: NOT COMPLETE"
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(
        main()
    )
