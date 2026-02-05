import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from kvstore import PersistentKVStore


class ClusterNode:
    """
    A single node in the cluster running its own PersistentKVStore.
    """

    def __init__(self, node_id: str, data_dir: Path) -> None:
        self.id = node_id
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.store = PersistentKVStore(str(self.data_dir / "data.log"))
        self.is_up = True

    def stop(self) -> None:
        """Simulate node failure."""
        self.is_up = False
        self.store.close()

    def start(self) -> None:
        """Simulate node restart."""
        # Re-open a new store on the same log file.
        self.is_up = True
        self.store = PersistentKVStore(str(self.data_dir / "data.log"))


class Cluster:
    """
    A simple in-process 3-node **master-less** replication cluster.

    All nodes are peers. For each operation, we pick one *coordinator*
    node (the first node that is currently up), apply the operation there,
    and synchronously replicate it to the other up nodes.

    - Reads go through the current coordinator.
    - There is no long-lived primary; if a node goes down, subsequent
      operations automatically choose another up node as coordinator.
    """

    def __init__(self, base_dir: Path) -> None:
        base_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.nodes: Dict[str, ClusterNode] = {
            "n1": ClusterNode("n1", base_dir / "n1"),
            "n2": ClusterNode("n2", base_dir / "n2"),
            "n3": ClusterNode("n3", base_dir / "n3"),
        }

    # Internal helpers ------------------------------------------------
    def _coordinator_id(self) -> str:
        """Return the id of the first node that is currently up."""
        for nid, node in self.nodes.items():
            if node.is_up:
                return nid
        raise RuntimeError("No available node in cluster")

    def _coordinator(self) -> ClusterNode:
        return self.nodes[self._coordinator_id()]

    def _replicate_to_peers(self, coordinator_id: str, op: str, *args, **kwargs) -> None:
        """Replicate an operation from the coordinator to all other up nodes."""
        for nid, node in self.nodes.items():
            if nid == coordinator_id or not node.is_up:
                continue
            if op == "set":
                node.store.set(*args, **kwargs)
            elif op == "delete":
                node.store.delete(*args, **kwargs)
            elif op == "bulk_set":
                node.store.bulk_set(*args, **kwargs)

    # Public API ------------------------------------------------------
    @property
    def primary_id(self) -> str:
        with self._lock:
            # Expose current coordinator id for observability/tests.
            return self._coordinator_id()

    def mark_down(self, node_id: str) -> None:
        with self._lock:
            self.nodes[node_id].stop()

    def mark_up(self, node_id: str) -> None:
        with self._lock:
            self.nodes[node_id].start()

    def set(self, key: str, value: str) -> None:
        with self._lock:
            coord_id = self._coordinator_id()
            coord = self.nodes[coord_id]
            coord.store.set(key, value)
            self._replicate_to_peers(coord_id, "set", key, value)

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            coord = self._coordinator()
            return coord.store.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            coord_id = self._coordinator_id()
            coord = self.nodes[coord_id]
            existed = coord.store.delete(key)
            self._replicate_to_peers(coord_id, "delete", key)
            return existed

    def bulk_set(self, items: List[Tuple[str, str]]) -> None:
        with self._lock:
            coord_id = self._coordinator_id()
            coord = self.nodes[coord_id]
            coord.store.bulk_set(items)
            self._replicate_to_peers(coord_id, "bulk_set", items)

