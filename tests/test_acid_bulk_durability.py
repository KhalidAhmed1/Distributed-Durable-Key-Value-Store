import os
import random
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
import signal

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from client import KVClient


def wait_for_port(host: str, port: int, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"Server did not open {host}:{port} within {timeout_s}s")


def start_server_process(host: str, port: int, data_file: str) -> subprocess.Popen:
    cmd = [
        sys.executable,
        "server.py",
        "--host",
        host,
        "--port",
        str(port),
        "--data-file",
        data_file,
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_port(host, port, timeout_s=5.0)
    return proc


def kill_process(proc: subprocess.Popen) -> None:
    """
    Kill the server process with SIGKILL (-9) semantics where available.
    """
    try:
        if hasattr(signal, "SIGKILL"):
            os.kill(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except Exception:
        pass


@pytest.mark.acid
def test_bulk_write_all_or_nothing_on_crash():
    """
    Bulk writes combined with a hard kill should either be fully applied
    or not applied at all after restart (no partial groups).
    """
    host = "127.0.0.1"
    port = 65439  # test-only port

    base = Path("benchmarks") / "acid_tmp"
    base.mkdir(parents=True, exist_ok=True)
    data_path = base / f"bulk_durability.{time.time_ns()}.log"

    proc = start_server_process(host, port, str(data_path))
    client = KVClient(host=host, port=port, timeout=1.0)

    group_size = 5
    acknowledged_groups: list[list[str]] = []
    ack_lock = threading.Lock()
    stop = threading.Event()

    def writer():
        idx = 0
        while not stop.is_set():
            keys = [f"g{idx}_k{i}" for i in range(group_size)]
            items = [(k, "v") for k in keys]
            try:
                client.bulk_set(items)
            except Exception:
                # likely killed; exit
                break
            with ack_lock:
                acknowledged_groups.append(keys)
            idx += 1

    def killer():
        # Kill once after a short random delay
        time.sleep(random.uniform(0.05, 0.3))
        stop.set()
        kill_process(proc)

    wt = threading.Thread(target=writer, daemon=True)
    kt = threading.Thread(target=killer, daemon=True)
    wt.start()
    kt.start()
    wt.join(timeout=2.0)
    kt.join(timeout=2.0)

    kill_process(proc)
    try:
        proc.wait(timeout=2.0)
    except Exception:
        pass

    with ack_lock:
        groups = list(acknowledged_groups)

    # Restart and verify all-or-nothing per bulk group
    proc2 = start_server_process(host, port, str(data_path))
    client2 = KVClient(host=host, port=port, timeout=2.0)

    partially_applied = 0
    for keys in groups:
        present = 0
        for k in keys:
            if client2.get(k) is not None:
                present += 1
        if 0 < present < len(keys):
            partially_applied += 1

    kill_process(proc2)
    try:
        proc2.wait(timeout=2.0)
    except Exception:
        pass

    # All-or-nothing: no group should be partially applied
    assert partially_applied == 0

