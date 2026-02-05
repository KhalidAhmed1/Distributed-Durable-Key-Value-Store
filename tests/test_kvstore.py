import threading
import time
from pathlib import Path
import uuid

import pytest

from client import KVClient
from server import KVServer


def _make_test_dir() -> Path:
    # Avoid pytest's tmp_path in restricted environments by creating a unique
    # directory inside the repo workspace.
    base = Path("benchmarks") / "test_tmp"
    path = base / str(uuid.uuid4())
    path.mkdir(parents=True, exist_ok=True)
    return path


def start_server_in_thread(test_dir: Path) -> KVServer:
    data_file = test_dir / "data.log"
    server = KVServer(host="127.0.0.1", port=0, data_file=str(data_file))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    # Give the server a brief moment to start
    time.sleep(0.1)
    return server


@pytest.fixture
def server_and_client():
    test_dir = _make_test_dir()
    server = start_server_in_thread(test_dir)
    client = KVClient(host=server.host, port=server.port)
    try:
        yield server, client
    finally:
        server.shutdown()
        # Allow some time for shutdown
        time.sleep(0.1)


def test_set_then_get(server_and_client):
    server, client = server_and_client
    client.set("foo", "bar")
    assert client.get("foo") == "bar"


def test_set_then_delete_then_get(server_and_client):
    server, client = server_and_client
    client.set("key", "value")
    deleted = client.delete("key")
    assert deleted is True
    assert client.get("key") is None


def test_get_without_setting(server_and_client):
    server, client = server_and_client
    assert client.get("missing") is None


def test_set_then_set_same_key_then_get(server_and_client):
    server, client = server_and_client
    client.set("k", "v1")
    client.set("k", "v2")
    assert client.get("k") == "v2"


def test_bulk_set(server_and_client):
    server, client = server_and_client
    items = [("a", "1"), ("b", "2"), ("c", "3")]
    client.bulk_set(items)
    for k, v in items:
        assert client.get(k) == v


def test_concurrent_bulk_sets_isolated(server_and_client):
    """
    Concurrent bulk_set writes touching the same keys should not partially
    interleave: at the end, all keys should reflect one whole bulk write.
    """
    server, client = server_and_client
    keys = [f"k{i}" for i in range(5)]

    def worker(value: str, rounds: int):
        payload = [(k, value) for k in keys]
        for _ in range(rounds):
            client.bulk_set(payload)

    t1 = threading.Thread(target=worker, args=("v1", 20))
    t2 = threading.Thread(target=worker, args=("v2", 20))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # All keys must have the same value, equal to one of the writers'
    final_values = {k: client.get(k) for k in keys}
    distinct = set(final_values.values())
    assert distinct.issubset({"v1", "v2"})
    assert len(distinct) == 1


def test_persistence_across_restart():
    """
    Set then exit (gracefully) then Get after restart.
    """
    test_dir = _make_test_dir()
    data_file = test_dir / "data.log"

    # First server instance: set a value and shut down
    server1 = KVServer(host="127.0.0.1", port=0, data_file=str(data_file))
    thread1 = threading.Thread(target=server1.serve_forever, daemon=True)
    thread1.start()
    time.sleep(0.1)

    client1 = KVClient(host=server1.host, port=server1.port)
    client1.set("persist_key", "persist_value")
    # Graceful shutdown
    server1.shutdown()
    time.sleep(0.2)

    # Second server instance: should load state from same data file
    server2 = KVServer(host="127.0.0.1", port=0, data_file=str(data_file))
    thread2 = threading.Thread(target=server2.serve_forever, daemon=True)
    thread2.start()
    time.sleep(0.1)

    client2 = KVClient(host=server2.host, port=server2.port)
    assert client2.get("persist_key") == "persist_value"

    server2.shutdown()
    time.sleep(0.2)

