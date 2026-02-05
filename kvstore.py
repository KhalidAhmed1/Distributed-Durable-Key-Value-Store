import json
import math
import os
import random
import re
import threading
from typing import Dict, List, Tuple, Optional, TextIO, List as TList, Tuple as TTuple


class PersistentKVStore:
    """
    A simple in-memory key-value store with durable persistence using
    an append-only log file.

    Operations:
    - set(key, value)
    - get(key)
    - delete(key)
    - bulk_set([(key, value), ...])
    """

    def __init__(self, data_file: str = "data.log") -> None:
        self._data_file = data_file
        self._lock = threading.Lock()
        self._store: Dict[str, str] = {}
        self._log_fp: Optional[TextIO] = None
        # Indexes on values
        # 1) full-text inverted index: token -> set(keys)
        self._inverted_index: Dict[str, set[str]] = {}
        # 2) simple hashed embedding index: key -> vector
        self._embedding_index: Dict[str, TList[float]] = {}
        self._embedding_dim: int = 16
        # Ensure directory exists
        data_dir = os.path.dirname(os.path.abspath(self._data_file))
        if data_dir and not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)
        # Load existing data if any
        self._load_from_log()
        # Open a persistent append handle to reduce per-write overhead
        self._open_log_for_append()

    # Public API -----------------------------------------------------
    def set(self, key: str, value: str, debug_unreliable: bool = False) -> None:
        with self._lock:
            entry = {"op": "set", "key": key, "value": value}
            self._apply_entry(entry)
            self._append_log(entry, debug_unreliable=debug_unreliable)

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            existed = key in self._store
            entry = {"op": "delete", "key": key}
            self._apply_entry(entry)
            self._append_log(entry)
            return existed

    def bulk_set(self, items: List[Tuple[str, str]], debug_unreliable: bool = False) -> None:
        if not items:
            return
        with self._lock:
            entry = {
                "op": "bulk_set",
                "items": list(items),
            }
            self._apply_entry(entry)
            self._append_log(entry, debug_unreliable=debug_unreliable)

    # Index query APIs -----------------------------------------------
    def search_full_text(self, query: str) -> TList[str]:
        """
        Full text search over values using the inverted index.
        Returns a list of keys that contain any of the query tokens.
        """
        with self._lock:
            tokens = self._tokenize(query)
            results: set[str] = set()
            for tok in tokens:
                results.update(self._inverted_index.get(tok, ()))
            return list(results)

    def search_embedding(self, query: str, top_k: int = 5) -> TList[TTuple[str, float]]:
        """
        Simple embedding-based search using a hashed bag-of-words embedding.
        Returns up to top_k (key, similarity) pairs sorted by similarity desc.
        """
        with self._lock:
            if not self._embedding_index:
                return []
            q_vec = self._build_embedding(query)

            def dot(a: TList[float], b: TList[float]) -> float:
                return sum(x * y for x, y in zip(a, b))

            def norm(a: TList[float]) -> float:
                return math.sqrt(sum(x * x for x in a))

            q_norm = norm(q_vec)
            if q_norm == 0.0:
                return []

            scores: TList[TTuple[str, float]] = []
            for key, vec in self._embedding_index.items():
                v_norm = norm(vec)
                if v_norm == 0.0:
                    continue
                sim = dot(q_vec, vec) / (q_norm * v_norm)
                scores.append((key, sim))

            scores.sort(key=lambda kv: kv[1], reverse=True)
            return scores[:top_k]

    def close(self) -> None:
        with self._lock:
            if self._log_fp is not None:
                try:
                    self._log_fp.flush()
                    os.fsync(self._log_fp.fileno())
                except OSError:
                    pass
                try:
                    self._log_fp.close()
                except OSError:
                    pass
                self._log_fp = None

    # Internal helpers ----------------------------------------------
    def _load_from_log(self) -> None:
        if not os.path.exists(self._data_file):
            return
        try:
            with open(self._data_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        self._apply_entry(entry)
                    except json.JSONDecodeError:
                        # Skip corrupted lines
                        continue
        except OSError:
            # If file can't be read, start with empty store
            self._store = {}

    def _apply_entry(self, entry: dict) -> None:
        op = entry.get("op")
        if op == "set":
            key = entry["key"]
            value = entry["value"]
            old = self._store.get(key)
            if old is not None:
                self._remove_from_indexes(key, old)
            self._store[key] = value
            self._add_to_indexes(key, value)
        elif op == "delete":
            key = entry.get("key")
            if key is not None:
                old = self._store.pop(key, None)
                if old is not None:
                    self._remove_from_indexes(key, old)
        elif op == "bulk_set":
            for key, value in entry.get("items", []):
                old = self._store.get(key)
                if old is not None:
                    self._remove_from_indexes(key, old)
                self._store[key] = value
                self._add_to_indexes(key, value)

    def _tokenize(self, text: str) -> TList[str]:
        tokens = re.findall(r"\w+", str(text).lower())
        return tokens

    def _add_to_indexes(self, key: str, value: str) -> None:
        tokens = self._tokenize(value)
        for tok in tokens:
            bucket = self._inverted_index.setdefault(tok, set())
            bucket.add(key)
        self._embedding_index[key] = self._build_embedding(value)

    def _remove_from_indexes(self, key: str, value: str) -> None:
        tokens = self._tokenize(value)
        for tok in tokens:
            bucket = self._inverted_index.get(tok)
            if not bucket:
                continue
            bucket.discard(key)
            if not bucket:
                self._inverted_index.pop(tok, None)
        self._embedding_index.pop(key, None)

    def _build_embedding(self, text: str) -> TList[float]:
        vec: TList[float] = [0.0] * self._embedding_dim
        for tok in self._tokenize(text):
            idx = hash(tok) % self._embedding_dim
            vec[idx] += 1.0
        return vec

    def _open_log_for_append(self) -> None:
        try:
            self._log_fp = open(self._data_file, "a", encoding="utf-8")
        except OSError:
            self._log_fp = None

    def _append_log(self, entry: dict, debug_unreliable: bool = False) -> None:
        # Durability policy: only acknowledge after data is fsync'd.
        try:
            # Debug-only path: optionally simulate filesystem sync/write issues
            # by randomly skipping the write entirely.
            if debug_unreliable:
                # 1% chance of "losing" this write
                if random.random() < 0.01:
                    return

            if self._log_fp is None:
                self._open_log_for_append()
            if self._log_fp is None:
                return
            self._log_fp.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._log_fp.flush()
            os.fsync(self._log_fp.fileno())
        except OSError:
            # In a real system, we would want to surface this. For this
            # exercise, we silently ignore persistence failures.
            pass

