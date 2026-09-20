import os
import signal
import subprocess
import sys
import time


ENV_FILE = os.path.expanduser("~/.our_search_env")

if os.path.exists(ENV_FILE):
    with open(ENV_FILE, "r") as env_file:
        for line in env_file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            os.environ[key] = value


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

SERVERS = [
    {"name": "search-1", "port": 8083},
    {"name": "search-2", "port": 8084},
    {"name": "search-3", "port": 8085},
]

processes = {}
stopping = False


def start_server(name, port):
    print(f"[SUPERVISOR] Starting {name} on port {port}", flush=True)

    env = os.environ.copy()
    env["PORT"] = str(port)

    process = subprocess.Popen(
        [sys.executable, "run_search_server.py"],
        cwd=PROJECT_ROOT,
        env=env,
    )

    processes[name] = process


def stop_all():
    global stopping
    stopping = True

    print("[SUPERVISOR] Stopping all search servers...", flush=True)

    for name, process in list(processes.items()):
        if process.poll() is None:
            process.terminate()

    for name, process in list(processes.items()):
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


def handle_signal(signum, frame):
    stop_all()
    sys.exit(0)


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


print("OUR SEARCH SEARCH CLUSTER SUPERVISOR", flush=True)

for server in SERVERS:
    start_server(
        server["name"],
        server["port"],
    )

while not stopping:
    for server in SERVERS:
        name = server["name"]
        port = server["port"]

        process = processes.get(name)

        if process is None or process.poll() is not None:
            exit_code = None if process is None else process.returncode

            print(
                f"[SUPERVISOR] {name} stopped "
                f"(exit={exit_code}). Restarting...",
                flush=True,
            )

            start_server(name, port)

    time.sleep(2)
