import threading
import time
import subprocess
import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Import the TCP-based KV client
from client import KVClient 

class ClusterNode:
    """Represents an independent node (server) running in a separate process."""
    
    def __init__(self, node_id: str, host: str, port: int, data_dir: Path) -> None:
        self.id = node_id               # Unique node ID
        self.host = host                # Node host (IP)
        self.port = port                # Node port
        self.data_dir = data_dir        # Directory to store node data
        self.process = None             # Reference to the subprocess running the node
        self.client = KVClient(host, port)  # TCP client for interacting with this node
        self.is_up = False              # Node status flag

    def start(self) -> None:
        """Start the node server as a separate OS process (TCP-based)."""
        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        data_file = str(self.data_dir / f"{self.id}.log")
        
        # Run server.py as a separate subprocess
        self.process = subprocess.Popen(
            [sys.executable, "server.py", "--host", self.host, "--port", str(self.port), "--data-file", data_file],
            stdout=subprocess.DEVNULL,  # Suppress stdout
            stderr=subprocess.DEVNULL   # Suppress stderr
        )
        
        # Give a short delay to ensure the socket is open before use
        time.sleep(0.6)
        self.is_up = True

    def stop(self, hard=True) -> None:
        """Stop the node.
        If hard=True, simulates a crash by sending SIGKILL.
        Otherwise, terminate gracefully.
        """
        if self.process:
            if hard:
                self.process.kill()   # Force kill the process
            else:
                self.process.terminate()  # Graceful termination
            self.process.wait()       # Wait for the process to exit
        self.is_up = False

class Cluster:
    """Manages a cluster of 3 nodes with election and replication support."""
    
    def __init__(self, base_dir: Path) -> None:
        # Initialize 3 cluster nodes
        self.nodes: Dict[str, ClusterNode] = {
            "n1": ClusterNode("n1", "127.0.0.1", 65431, base_dir),
            "n2": ClusterNode("n2", "127.0.0.1", 65432, base_dir),
            "n3": ClusterNode("n3", "127.0.0.1", 65433, base_dir),
        }
        self._lock = threading.Lock()  # Lock for thread-safe operations
        
        # Start all nodes on initialization
        for node in self.nodes.values():
            node.start()

    def _coordinator_id(self) -> str:
        """Election algorithm: pick the first available node as Primary."""
        for nid in ["n1", "n2", "n3"]:
            if self.nodes[nid].is_up:
                return nid
        raise RuntimeError("CRITICAL: All nodes in the cluster are down!")

    @property
    def primary_id(self) -> str:
        """Return the current primary node ID in a thread-safe manner."""
        with self._lock:
            return self._coordinator_id()

    def set(self, key: str, value: str) -> None:
        """Write to the primary node, then replicate to the secondaries."""
        with self._lock:
            pid = self._coordinator_id()
            
            # Write to primary
            self.nodes[pid].client.set(key, value)
            
            # Replicate to other nodes
            for nid, node in self.nodes.items():
                if nid != pid and node.is_up:
                    try:
                        node.client.set(key, value)
                    except Exception:
                        pass  # Ignore failures during replication

    def get(self, key: str) -> Optional[str]:
        """Read from the primary node only to ensure consistency."""
        with self._lock:
            pid = self._coordinator_id()
            return self.nodes[pid].client.get(key)

    def delete(self, key: str) -> bool:
        """Delete key from primary and replicate deletion to secondaries."""
        with self._lock:
            pid = self._coordinator_id()
            res = self.nodes[pid].client.delete(key)
            
            for nid, node in self.nodes.items():
                if nid != pid and node.is_up:
                    try:
                        node.client.delete(key)
                    except:
                        pass
            return res

    def bulk_set(self, items: List[Tuple[str, str]]) -> None:
        """Set multiple key-value pairs at once on primary and replicate."""
        with self._lock:
            pid = self._coordinator_id()
            self.nodes[pid].client.bulk_set(items)
            
            for nid, node in self.nodes.items():
                if nid != pid and node.is_up:
                    try:
                        node.client.bulk_set(items)
                    except:
                        pass

    def mark_down(self, node_id: str) -> None:
        """Simulate a node failure (for failover testing)."""
        if node_id in self.nodes:
            self.nodes[node_id].stop(hard=True)

    def mark_up(self, node_id: str) -> None:
        """Restart a previously down node."""
        if node_id in self.nodes:
            self.nodes[node_id].start()

    def shutdown(self):
        """Shut down the entire cluster gracefully."""
        for node in self.nodes.values():
            node.stop(hard=False)
