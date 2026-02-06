"""
Master-less replication cluster where all nodes are equal.
Writes go to majority, reads ensure consistency via quorum reads.
"""
import threading
import time
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import Counter

from client import KVClient


class ClusterNode:
    """Represents an independent node in a masterless cluster."""
    
    def __init__(self, node_id: str, host: str, port: int, data_dir: Path) -> None:
        self.id = node_id
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.process = None
        self.client = KVClient(host, port)
        self.is_up = False

    def start(self) -> None:
        """Start the node server as a separate process."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        data_file = str(self.data_dir / f"{self.id}.log")
        
        self.process = subprocess.Popen(
            [sys.executable, "server.py", "--host", self.host, "--port", str(self.port), "--data-file", data_file],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        time.sleep(0.6)
        self.is_up = True

    def stop(self, hard=True) -> None:
        """Stop the node."""
        if self.process:
            if hard:
                self.process.kill()
            else:
                self.process.terminate()
            self.process.wait()
        self.is_up = False


class MasterlessCluster:
    """
    Master-less cluster with quorum-based reads and writes.
    All nodes are equal - no primary/secondary distinction.
    """
    
    def __init__(self, base_dir: Path) -> None:
        self.nodes: Dict[str, ClusterNode] = {
            "n1": ClusterNode("n1", "127.0.0.1", 65431, base_dir / "n1"),
            "n2": ClusterNode("n2", "127.0.0.1", 65432, base_dir / "n2"),
            "n3": ClusterNode("n3", "127.0.0.1", 65433, base_dir / "n3"),
        }
        self._lock = threading.Lock()
        
        # Start all nodes
        for node in self.nodes.values():
            node.start()
    
    def _get_alive_nodes(self) -> List[ClusterNode]:
        """Return list of currently running nodes."""
        return [n for n in self.nodes.values() if n.is_up]
    
    def _quorum_size(self) -> int:
        """Calculate majority quorum size (N/2 + 1)."""
        total = len(self.nodes)
        return (total // 2) + 1
    
    def set(self, key: str, value: str) -> None:
        """
        Write to a quorum of nodes (majority).
        Fails if quorum cannot be reached.
        """
        with self._lock:
            alive = self._get_alive_nodes()
            quorum = self._quorum_size()
            
            if len(alive) < quorum:
                raise RuntimeError(f"Insufficient nodes for quorum: need {quorum}, have {len(alive)}")
            
            success_count = 0
            for node in alive:
                try:
                    node.client.set(key, value)
                    success_count += 1
                    if success_count >= quorum:
                        return  # Quorum achieved
                except Exception:
                    continue
            
            if success_count < quorum:
                raise RuntimeError(f"Failed to reach write quorum: {success_count}/{quorum}")
    
    def get(self, key: str) -> Optional[str]:
        """
        Quorum read: read from majority of nodes and return most recent value.
        Uses simple voting - the value seen by majority wins.
        """
        with self._lock:
            alive = self._get_alive_nodes()
            quorum = self._quorum_size()
            
            if len(alive) < quorum:
                raise RuntimeError(f"Insufficient nodes for quorum: need {quorum}, have {len(alive)}")
            
            values: List[Optional[str]] = []
            for node in alive:
                try:
                    val = node.client.get(key)
                    values.append(val)
                except Exception:
                    continue
            
            if len(values) < quorum:
                raise RuntimeError(f"Failed to reach read quorum: {len(values)}/{quorum}")
            
            # Vote: return the value that appears most frequently
            counter = Counter(values)
            most_common_value, _ = counter.most_common(1)[0]
            return most_common_value
    
    def delete(self, key: str) -> bool:
        """Delete from quorum of nodes."""
        with self._lock:
            alive = self._get_alive_nodes()
            quorum = self._quorum_size()
            
            if len(alive) < quorum:
                raise RuntimeError(f"Insufficient nodes for quorum: need {quorum}, have {len(alive)}")
            
            success_count = 0
            deleted_any = False
            
            for node in alive:
                try:
                    result = node.client.delete(key)
                    if result:
                        deleted_any = True
                    success_count += 1
                    if success_count >= quorum:
                        break
                except Exception:
                    continue
            
            if success_count < quorum:
                raise RuntimeError(f"Failed to reach delete quorum: {success_count}/{quorum}")
            
            return deleted_any
    
    def bulk_set(self, items: List[Tuple[str, str]]) -> None:
        """Bulk set to quorum of nodes."""
        with self._lock:
            alive = self._get_alive_nodes()
            quorum = self._quorum_size()
            
            if len(alive) < quorum:
                raise RuntimeError(f"Insufficient nodes for quorum: need {quorum}, have {len(alive)}")
            
            success_count = 0
            for node in alive:
                try:
                    node.client.bulk_set(items)
                    success_count += 1
                    if success_count >= quorum:
                        return
                except Exception:
                    continue
            
            if success_count < quorum:
                raise RuntimeError(f"Failed to reach bulk_set quorum: {success_count}/{quorum}")
    
    def mark_down(self, node_id: str) -> None:
        """Simulate node failure."""
        if node_id in self.nodes:
            self.nodes[node_id].stop(hard=True)
    
    def mark_up(self, node_id: str) -> None:
        """Restart a node."""
        if node_id in self.nodes:
            self.nodes[node_id].start()
    
    def shutdown(self):
        """Shutdown all nodes."""
        for node in self.nodes.values():
            node.stop(hard=False)