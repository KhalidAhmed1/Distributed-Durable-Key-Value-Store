import argparse
import json
import socket
import threading
from typing import Tuple

from kvstore import PersistentKVStore


class KVServer:
    """
    TCP server that exposes a JSON-over-TCP protocol for the PersistentKVStore.

    Each request and response is a single JSON object, one per line.
    Request examples:
      {"op": "set", "key": "k", "value": "v"}
      {"op": "get", "key": "k"}
      {"op": "delete", "key": "k"}
      {"op": "bulk_set", "items": [["k1", "v1"], ["k2", "v2"]]}
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 65432, data_file: str = "data.log"):
        self.host = host
        self.port = port
        self._store = PersistentKVStore(data_file=data_file)
        self._shutdown_event = threading.Event()
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.host, self.port))
        # If port was 0, update to actual chosen port
        self.port = self._server_sock.getsockname()[1]
        self._server_sock.listen()
        self._server_sock.settimeout(1.0)

    # Public API -----------------------------------------------------
    def serve_forever(self) -> None:
        try:
            while not self._shutdown_event.is_set():
                try:
                    conn, addr = self._server_sock.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()
        finally:
            self._server_sock.close()

    def shutdown(self) -> None:
        self._shutdown_event.set()
        # Connect to self to unblock accept
        try:
            with socket.create_connection((self.host, self.port), timeout=1):
                pass
        except OSError:
            pass
        # Best-effort flush/close underlying store log
        try:
            self._store.close()
        except Exception:
            pass

    # Internal helpers ----------------------------------------------
    def _handle_client(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        with conn:
            f = conn.makefile(mode="rwb")
            while True:
                line = f.readline()
                if not line:
                    break
                try:
                    request = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    self._send_response(f, {"status": "error", "error": "invalid_json"})
                    continue
                response = self._process_request(request)
                self._send_response(f, response)

    def _process_request(self, request: dict) -> dict:
        op = request.get("op")
        if op == "set":
            key = request.get("key")
            value = request.get("value")
            if key is None or value is None:
                return {"status": "error", "error": "missing_key_or_value"}
            self._store.set(str(key), str(value))
            return {"status": "ok"}
        if op == "get":
            key = request.get("key")
            if key is None:
                return {"status": "error", "error": "missing_key"}
            val = self._store.get(str(key))
            if val is None:
                return {"status": "not_found"}
            return {"status": "ok", "value": val}
        if op == "delete":
            key = request.get("key")
            if key is None:
                return {"status": "error", "error": "missing_key"}
            existed = self._store.delete(str(key))
            return {"status": "ok", "deleted": existed}
        if op == "bulk_set":
            items = request.get("items")
            if not isinstance(items, list):
                return {"status": "error", "error": "invalid_items"}
            normalized = []
            for pair in items:
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    return {"status": "error", "error": "invalid_items"}
                k, v = pair
                normalized.append((str(k), str(v)))
            self._store.bulk_set(normalized)
            return {"status": "ok"}
        return {"status": "error", "error": "unknown_op"}

    def _send_response(self, f, response: dict) -> None:
        data = json.dumps(response, ensure_ascii=False).encode("utf-8") + b"\n"
        f.write(data)
        f.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable TCP key-value store server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=65432)
    parser.add_argument("--data-file", default="data.log")
    args = parser.parse_args()

    server = KVServer(host=args.host, port=args.port, data_file=args.data_file)
    print(f"KVServer listening on {server.host}:{server.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Shutting down server...")
        server.shutdown()


if __name__ == "__main__":
    main()

