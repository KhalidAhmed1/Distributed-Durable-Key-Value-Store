import time

from client import KVClient
from server import KVServer


def run_benchmark(num_ops: int = 10_000) -> None:
    # Start an in-process server
    server = KVServer(host="127.0.0.1", port=0, data_file="benchmark_data.log")
    import threading

    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.1)

    client = KVClient(host=server.host, port=server.port)

    print(f"Running benchmark: {num_ops} Set operations")
    start = time.perf_counter()
    for i in range(num_ops):
        client.set(f"key_{i}", f"value_{i}")
    elapsed = time.perf_counter() - start
    ops_per_sec = num_ops / elapsed if elapsed > 0 else float("inf")

    print(f"Elapsed: {elapsed:.4f} seconds")
    print(f"Throughput: {ops_per_sec:.2f} ops/sec")

    server.shutdown()
    time.sleep(0.2)


if __name__ == "__main__":
    run_benchmark()

