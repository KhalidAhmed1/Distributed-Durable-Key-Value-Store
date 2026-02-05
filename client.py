import json
import socket
from typing import List, Tuple, Optional


class KVClient:
    """
    Client for the TCP-based key-value store.

    Public methods:
      - get(key) -> Optional[str]
      - set(key, value) -> None
      - delete(key) -> bool
      - bulk_set(items: List[Tuple[str, str]]) -> None
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 65432, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    # Public API -----------------------------------------------------
    def set(self, key: str, value: str) -> None:
        req = {"op": "set", "key": key, "value": value}
        resp = self._send_request(req)
        if resp.get("status") != "ok":
            raise RuntimeError(f"Set failed: {resp}")

    def get(self, key: str) -> Optional[str]:
        req = {"op": "get", "key": key}
        resp = self._send_request(req)
        status = resp.get("status")
        if status == "ok":
            return resp.get("value")
        if status == "not_found":
            return None
        raise RuntimeError(f"Get failed: {resp}")

    def delete(self, key: str) -> bool:
        req = {"op": "delete", "key": key}
        resp = self._send_request(req)
        if resp.get("status") != "ok":
            raise RuntimeError(f"Delete failed: {resp}")
        return bool(resp.get("deleted"))

    def bulk_set(self, items: List[Tuple[str, str]]) -> None:
        # Normalize to list of [key, value] for JSON
        normalized = [[str(k), str(v)] for k, v in items]
        req = {"op": "bulk_set", "items": normalized}
        resp = self._send_request(req)
        if resp.get("status") != "ok":
            raise RuntimeError(f"BulkSet failed: {resp}")

    # Internal helpers ----------------------------------------------
    def _send_request(self, request: dict) -> dict:
        data = json.dumps(request, ensure_ascii=False).encode("utf-8") + b"\n"
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as sock:
            f = sock.makefile(mode="rwb")
            f.write(data)
            f.flush()
            line = f.readline()
            if not line:
                raise RuntimeError("No response from server")
            try:
                return json.loads(line.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid JSON response: {line!r}") from e

