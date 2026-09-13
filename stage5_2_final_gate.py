import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))


def run(label, command):
    print(f"RUNNING: {label}")
    result = subprocess.run(command, cwd=ROOT)
    if result.returncode != 0:
        print(f"FAILED: {label}")
        sys.exit(result.returncode)
    print(f"PASS: {label}")


print("=" * 72)
print("STAGE 5.2 — FINAL MASSIVE FRONTIER GATE")
print("=" * 72)

run(
    "Python compilation",
    [
        sys.executable,
        "-m",
        "py_compile",
        "crawler_system/url_state.py",
        "crawler_system/ready_frontier.py",
    ],
)

run(
    "URLState integration",
    [
        sys.executable,
        "-c",
        """
import os, tempfile, time
from crawler_system.url_state import URLStateStore

root = tempfile.mkdtemp(prefix='stage5_2_final_')
db = os.path.join(root, 'state.sqlite3')
s = URLStateStore(db)

urls = [
    ('https://a.example/1', 'a.example', 100),
    ('https://b.example/1', 'b.example', 90),
    ('https://c.example/1', 'c.example', 80),
]

for i, (url, host, priority) in enumerate(urls):
    assert s.add_discovered(
        url,
        f'doc-{i}',
        host,
        priority=priority,
    )

assert s.count() == 3
assert s.ready_size() == 3

row = s.claim_next('final-worker', now=time.time() + 100000)
assert row is not None
assert row['url'] == urls[0][0]
assert s.ready_size() == 2

s.mark_crawled(row['url'], status=200)

row = s.claim_next('final-worker', now=time.time() + 100010)
assert row is not None
s.mark_crawled(row['url'], status=200)

row = s.claim_next('final-worker', now=time.time() + 100020)
assert row is not None
s.mark_crawled(row['url'], status=200)

assert s.ready_size() == 0

s.close()

s = URLStateStore(db)
assert s.count() == 3
assert s.ready_size() == 0

rebuilt = s.rebuild_ready_frontier()
assert rebuilt == 0
assert s.ready_size() == 0

s.clear()
assert s.count() == 0
assert s.ready_size() == 0
s.close()

print('URLSTATE FINAL CONSISTENCY: PASS')
""",
    ],
)

run(
    "Massive frontier performance",
    [sys.executable, "stage5_2_frontier_performance_gate.py"],
)

print("=" * 72)
print("STAGE 5.2 FINAL RESULT: PASS")
print("STAGE 5.2 MASSIVE FRONTIER: 100% COMPLETE")
print("=" * 72)
