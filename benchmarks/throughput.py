import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from client import KVClient
from server import KVServer


def prepopulate(client: KVClient, n: int, batch: int) -> None:
    if n <= 0:
        return
    i = 0
    while i < n:
        chunk = []
        end = min(n, i + batch)
        for j in range(i, end):
            chunk.append((f"pre_{j}", f"value_{j}"))
        client.bulk_set(chunk)
        i = end


def measure_write_throughput(client: KVClient, n: int) -> float:
    start = time.perf_counter()
    for i in range(n):
        client.set(f"w_{i}", f"v_{i}")
    elapsed = time.perf_counter() - start
    return n / elapsed if elapsed > 0 else float("inf")


def run(
    data_file: str,
    prepopulate_sizes: list[int],
    writes_per_round: int,
    prepopulate_batch: int,
) -> None:
    data_path = Path(data_file)
    data_path.parent.mkdir(parents=True, exist_ok=True)
    # Avoid failing if another process holds this file open (Windows).
    if data_path.exists():
        alt = data_path.with_name(f"{data_path.stem}.{time.time_ns()}{data_path.suffix}")
        data_path = alt

    server = KVServer(host="127.0.0.1", port=0, data_file=str(data_path))
    import threading

    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)
    client = KVClient(host=server.host, port=server.port)

    print("Write throughput vs pre-populated dataset size")
    print(f"data_file={data_path} writes_per_round={writes_per_round}")
    print("")
    print("prepopulated_keys,writes_per_sec")

    current = 0
    for target in prepopulate_sizes:
        if target < current:
            raise ValueError("prepopulate sizes must be non-decreasing")
        add = target - current
        prepopulate(client, add, prepopulate_batch)
        current = target
        wps = measure_write_throughput(client, writes_per_round)
        print(f"{current},{wps:.2f}")

    server.shutdown()
    time.sleep(0.2)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write throughput benchmark")
    parser.add_argument("--data-file", default=os.path.join("benchmarks", f"throughput_data.{time.time_ns()}.log"))
    parser.add_argument("--writes", type=int, default=5_000, help="writes measured per round")
    parser.add_argument("--batch", type=int, default=500, help="bulk-set batch size for prepopulation")
    parser.add_argument(
        "--prepopulate",
        default="0,1000,5000,20000,50000",
        help="comma-separated pre-population sizes",
    )
    args = parser.parse_args()

    sizes = [int(x.strip()) for x in args.prepopulate.split(",") if x.strip()]
    run(args.data_file, sizes, args.writes, args.batch)


if __name__ == "__main__":
    main()

