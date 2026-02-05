import argparse
import os
import random
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
import signal

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
    # stdout/stderr suppressed to keep benchmark output clean
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_port(host, port, timeout_s=5.0)
    return proc


def kill_process(proc: subprocess.Popen) -> None:
    """
    Kill the server process in a way that closely matches `SIGKILL` (-9)
    semantics where available.

    On POSIX, we send SIGKILL; on other platforms we fall back to proc.kill().
    """
    try:
        if hasattr(signal, "SIGKILL"):
            os.kill(proc.pid, signal.SIGKILL)
        else:
            proc.kill()
    except Exception:
        pass


def run(
    data_file: str,
    host: str,
    port: int,
    duration_s: float,
    kill_min_ms: int,
    kill_max_ms: int,
    write_delay_ms: int,
) -> None:
    data_path = Path(data_file)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    if data_path.exists():
        data_path.unlink()

    proc = start_server_process(host, port, str(data_path))
    client = KVClient(host=host, port=port, timeout=1.0)

    acknowledged: list[str] = []
    ack_lock = threading.Lock()
    stop = threading.Event()
    killed = threading.Event()

    def writer_thread():
        i = 0
        while not stop.is_set():
            key = f"ack_{i}_{time.time_ns()}"
            try:
                client.set(key, "v")
            except Exception:
                # likely server down
                time.sleep(0.01)
                continue
            with ack_lock:
                acknowledged.append(key)
            i += 1
            if write_delay_ms > 0:
                time.sleep(write_delay_ms / 1000.0)

    def killer_thread():
        # Randomly kill once during the run window using a hard kill.
        sleep_ms = random.randint(kill_min_ms, kill_max_ms)
        time.sleep(sleep_ms / 1000.0)
        if killed.is_set():
            return
        kill_process(proc)
        killed.set()
        stop.set()

    wt = threading.Thread(target=writer_thread, daemon=True)
    kt = threading.Thread(target=killer_thread, daemon=True)
    wt.start()
    kt.start()

    # If killer doesn't fire for some reason, stop after duration
    time.sleep(duration_s)
    stop.set()

    # Ensure process is dead
    kill_process(proc)
    try:
        proc.wait(timeout=2.0)
    except Exception:
        pass

    with ack_lock:
        acked_keys = list(acknowledged)

    # Restart server with same data file and check for lost acknowledged keys
    proc2 = start_server_process(host, port, str(data_path))
    client2 = KVClient(host=host, port=port, timeout=2.0)

    lost = 0
    for k in acked_keys:
        try:
            if client2.get(k) is None:
                lost += 1
        except Exception:
            # If server flakes, count as lost (durability failure)
            lost += 1

    kill_process(proc2)
    try:
        proc2.wait(timeout=2.0)
    except Exception:
        pass

    total = len(acked_keys)
    lost_pct = (lost / total * 100.0) if total else 0.0
    print("Durability benchmark (acknowledged writes vs crash loss)")
    print(f"acked={total} lost={lost} lost_pct={lost_pct:.2f}%")
    if lost == 0:
        print("Result: PASS (100% durability for acknowledged writes)")
    else:
        print("Result: FAIL (some acknowledged keys were lost)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Durability benchmark via random kill")
    parser.add_argument("--data-file", default=os.path.join("benchmarks", "durability_data.log"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=65433)
    parser.add_argument("--duration", type=float, default=2.0, help="max run time window")
    parser.add_argument("--kill-min-ms", type=int, default=50)
    parser.add_argument("--kill-max-ms", type=int, default=800)
    parser.add_argument("--write-delay-ms", type=int, default=0, help="sleep between writes (0 = tight loop)")
    args = parser.parse_args()

    run(
        data_file=args.data_file,
        host=args.host,
        port=args.port,
        duration_s=args.duration,
        kill_min_ms=args.kill_min_ms,
        kill_max_ms=args.kill_max_ms,
        write_delay_ms=args.write_delay_ms,
    )


if __name__ == "__main__":
    main()

