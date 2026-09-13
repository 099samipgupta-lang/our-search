import os
import subprocess
import sys


FINAL_GATES = [
    (
        "4.3 Adaptive Discovery Source Control",
        "stage4_3_14_adaptive_control_test.py",
    ),
    (
        "4.4 Whole Crawler Domain Discovery Integration",
        "stage4_4_9_whole_crawler_integration_test.py",
    ),
    (
        "4.5 Final Discovery Priority Gate",
        "stage4_5_10_final_priority_gate_test.py",
    ),
    (
        "4.6 Final Discovery Scalability Gate",
        "stage4_6_9_final_scalability_gate_test.py",
    ),
    (
        "4.7.5 Long-running Expansion Stability",
        "stage4_7_5_long_running_expansion_stability_test.py",
    ),
    (
        "4.7.6 Final Continuous Global Expansion",
        "stage4_7_6_final_continuous_global_expansion_gate.py",
    ),
]


def run_gate(name, filename):
    print()
    print("=" * 72)
    print(name)
    print(filename)
    print("=" * 72)

    if not os.path.isfile(filename):
        print(f"MISSING GATE: {filename}")
        return False

    result = subprocess.run(
        [sys.executable, filename],
        check=False,
    )

    if result.returncode != 0:
        print()
        print(f"FAILED GATE: {name}")
        print(f"RETURN CODE: {result.returncode}")
        return False

    print()
    print(f"PASSED GATE: {name}")
    return True


def main():
    print("=" * 72)
    print("OUR SEARCH")
    print("STAGE 4 FINAL OVERALL GATE")
    print("=" * 72)

    passed = 0
    failed = 0

    for name, filename in FINAL_GATES:
        if run_gate(name, filename):
            passed += 1
        else:
            failed += 1
            break

    print()
    print("=" * 72)
    print("STAGE 4 FINAL OVERALL SUMMARY")
    print("=" * 72)
    print(f"FINAL GATES PASSED: {passed}")
    print(f"FINAL GATES FAILED: {failed}")

    if failed:
        print("RESULT: FAIL")
        raise SystemExit(1)

    print()
    print("4.3 FINAL GATE: PASS")
    print("4.4 FINAL GATE: PASS")
    print("4.5 FINAL GATE: PASS")
    print("4.6 FINAL GATE: PASS")
    print("4.7.5 STABILITY GATE: PASS")
    print("4.7.6 CONTINUOUS GLOBAL EXPANSION GATE: PASS")
    print()
    print("========================================")
    print("STAGE 4 FINAL OVERALL GATE")
    print("RESULT: PASS")
    print("STAGE 4: 100% COMPLETE")
    print("READY FOR STAGE 5")
    print("========================================")


if __name__ == "__main__":
    main()
