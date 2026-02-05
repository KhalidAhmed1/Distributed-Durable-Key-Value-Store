import json
import math
import os
import random
import re
import threading
from typing import Dict, List, Tuple, Optional, TextIO, List as TList

class PersistentKVStore:
    """
    A Key-Value store with advanced features:
    
    Features:
    - Persistence via append-only log (ensures data survives process restarts)
    - Inverted Index for full-text search
    - Word embedding for semantic similarity searches
    - Durability via fsync to ensure data is flushed to disk
    """

    def __init__(self, data_file: str = "data.log") -> None:
        self._data_file = data_file
        self._lock = threading.Lock()            # Lock for thread-safe operations
        self._store: Dict[str, str] = {}         # In-memory key-value store
        self._log_fp: Optional[TextIO] = None    # File pointer to the append-only log

        # Indexes for search
        self._inverted_index: Dict[str, set] = {}     # Maps token -> set of keys
        self._embedding_index: Dict[str, TList[float]] = {}  # Maps key -> embedding vector
        self._embedding_dim: int = 16                 # Dimension of embedding vectors

        # Ensure directory exists
        data_dir = os.path.dirname(os.path.abspath(self._data_file))
        if data_dir and not os.path.exists(data_dir):
            os.makedirs(data_dir, exist_ok=True)

        # Load existing data from log
        self._load_from_log()

    # ------------------- Public API -------------------

    def set(self, key: str, value: str, debug_unreliable: bool = False) -> None:
        """Set a key-value pair in a thread-safe way."""
        with self._lock:
            self._set_internal(key, value, debug_unreliable)

    def get(self, key: str) -> Optional[str]:
        """Retrieve a value for a key. Returns None if key does not exist."""
        with self._lock:
            return self._store.get(key)

    def delete(self, key: str) -> bool:
        """Delete a key from the store. Returns True if key existed."""
        with self._lock:
            if key not in self._store:
                return False
            self._append_log({"op": "delete", "key": key})
            val = self._store.pop(key)
            self._update_indexes(key, val, remove=True)
            return True

    def bulk_set(self, items: List[Tuple[str, str]]) -> None:
        """
        Set multiple key-value pairs atomically.
        The entire batch is first logged to ensure atomicity.
        """
        with self._lock:
            self._append_log({"op": "bulk_set", "items": items})
            for key, value in items:
                self._set_internal(key, value, fast=True)

    # ------------------- Search API -------------------

    def search_full_text(self, query: str) -> List[str]:
        """Search keys using the inverted index (full-text search)."""
        tokens = self._tokenize(query)
        if not tokens:
            return []
        with self._lock:
            sets = [self._inverted_index.get(t, set()) for t in tokens]
            # Intersection of sets to match all tokens
            result_keys = set.intersection(*sets) if sets else set()
            return list(result_keys)

    def search_embedding(self, query: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """
        Semantic search using word embeddings.
        Returns top_k keys sorted by cosine similarity.
        """
        query_vec = self._build_embedding(query)
        results = []
        with self._lock:
            for key, doc_vec in self._embedding_index.items():
                sim = self._cosine_similarity(query_vec, doc_vec)
                results.append((key, sim))
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]

    # ------------------- Internal Helpers -------------------

    def _set_internal(self, key: str, value: str, debug_unreliable: bool = False, fast: bool = False) -> None:
        """
        Core internal method to set a key.
        - fast=True: skip logging (used when replaying log)
        """
        if not fast:
            self._append_log({"op": "set", "key": key, "value": value}, debug_unreliable)

        # Update indexes if key already exists
        old_val = self._store.get(key)
        if old_val:
            self._update_indexes(key, old_val, remove=True)

        self._store[key] = value
        self._update_indexes(key, value)

    def _update_indexes(self, key: str, value: str, remove: bool = False) -> None:
        """Update both inverted and embedding indexes."""
        tokens = self._tokenize(value)
        for tok in tokens:
            if remove:
                if tok in self._inverted_index:
                    self._inverted_index[tok].discard(key)
            else:
                if tok not in self._inverted_index:
                    self._inverted_index[tok] = set()
                self._inverted_index[tok].add(key)

        if remove:
            self._embedding_index.pop(key, None)
        else:
            self._embedding_index[key] = self._build_embedding(value)

    def _tokenize(self, text: str) -> List[str]:
        """Simple word tokenizer using regex."""
        return re.findall(r'\w+', text.lower())

    def _build_embedding(self, text: str) -> List[float]:
        """
        Build a simple word embedding vector.
        Each token contributes to a deterministic position based on hash.
        """
        vec = [0.0] * self._embedding_dim
        for tok in self._tokenize(text):
            idx = hash(tok) % self._embedding_dim
            vec[idx] += 1.0
        return vec

    def _cosine_similarity(self, v1: List[float], v2: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        dot = sum(a * b for a, b in zip(v1, v2))
        mag1 = math.sqrt(sum(a * a for a in v1))
        mag2 = math.sqrt(sum(b * b for b in v2))
        return dot / (mag1 * mag2) if mag1 > 0 and mag2 > 0 else 0.0

    # ------------------- Persistence & Durability -------------------

    def _append_log(self, entry: dict, debug_unreliable: bool = False) -> None:
        """
        Append a log entry to the persistent file.
        Ensures data durability with flush + fsync.
        """
        if debug_unreliable and random.random() < 0.01:
            return  # Simulate rare write failure (debugging purpose)

        if self._log_fp is None:
            self._log_fp = open(self._data_file, "a", encoding="utf-8")

        line = json.dumps(entry, ensure_ascii=False) + "\n"
        self._log_fp.write(line)

        # Flush and fsync for 100% durability even if process is killed
        self._log_fp.flush()
        os.fsync(self._log_fp.fileno())

    def _load_from_log(self) -> None:
        """Replay the log file to reconstruct in-memory store and indexes."""
        if not os.path.exists(self._data_file):
            return
        with open(self._data_file, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                entry = json.loads(line)
                op = entry.get("op")
                if op == "set":
                    self._set_internal(entry["key"], entry["value"], fast=True)
                elif op == "delete":
                    k = entry["key"]
                    val = self._store.pop(k, None)
                    if val:
                        self._update_indexes(k, val, remove=True)
                elif op == "bulk_set":
                    for k, v in entry["items"]:
                        self._set_internal(k, v, fast=True)

    def close(self) -> None:
        """Close the log file if open."""
        if self._log_fp:
            self._log_fp.close()
            self._log_fp = None
