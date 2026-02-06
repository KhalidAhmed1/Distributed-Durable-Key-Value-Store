"""Tests for master-less cluster implementation."""
from pathlib import Path
import uuid
import pytest

from cluster_masterless import MasterlessCluster


def make_masterless_cluster() -> MasterlessCluster:
    base = Path("benchmarks") / "masterless_tmp" / str(uuid.uuid4())
    return MasterlessCluster(base)


def test_masterless_quorum_write():
    """Test that writes succeed with quorum of nodes."""
    cluster = make_masterless_cluster()
    
    # All 3 nodes up - write should succeed
    cluster.set("key1", "value1")
    assert cluster.get("key1") == "value1"
    
    cluster.shutdown()


def test_masterless_survives_single_node_failure():
    """Master-less cluster should work with 2/3 nodes (quorum=2)."""
    cluster = make_masterless_cluster()
    
    # Write with all nodes up
    cluster.set("k1", "v1")
    
    # Kill one node - quorum still possible (2/3)
    cluster.mark_down("n1")
    
    # Writes and reads should still work
    cluster.set("k2", "v2")
    assert cluster.get("k1") == "v1"
    assert cluster.get("k2") == "v2"
    
    cluster.shutdown()


def test_masterless_fails_without_quorum():
    """Cluster should fail when quorum is lost (only 1/3 nodes)."""
    cluster = make_masterless_cluster()
    
    # Write initial data
    cluster.set("k", "v")
    
    # Kill 2 nodes - no quorum (need 2, have 1)
    cluster.mark_down("n1")
    cluster.mark_down("n2")
    
    # Both reads and writes should fail
    with pytest.raises(RuntimeError, match="Insufficient nodes"):
        cluster.set("k2", "v2")
    
    with pytest.raises(RuntimeError, match="Insufficient nodes"):
        cluster.get("k")
    
    cluster.shutdown()


def test_masterless_node_recovery():
    """Test that a failed node can rejoin and catch up."""
    cluster = make_masterless_cluster()
    
    # Write with all nodes
    cluster.set("before", "failure")
    
    # Kill one node
    cluster.mark_down("n3")
    
    # Write while n3 is down
    cluster.set("during", "downtime")
    
    # Bring n3 back up
    cluster.mark_up("n3")
    
    # New writes should replicate to all nodes
    cluster.set("after", "recovery")
    
    # Reads should work (quorum ensures consistency)
    assert cluster.get("before") == "failure"
    assert cluster.get("after") == "recovery"
    
    cluster.shutdown()


def test_masterless_bulk_operations():
    """Test bulk operations with quorum."""
    cluster = make_masterless_cluster()
    
    items = [("a", "1"), ("b", "2"), ("c", "3")]
    cluster.bulk_set(items)
    
    # Kill one node
    cluster.mark_down("n1")
    
    # Bulk should still work with quorum
    cluster.bulk_set([("d", "4"), ("e", "5")])
    
    # Verify all keys
    for k, v in items + [("d", "4"), ("e", "5")]:
        assert cluster.get(k) == v
    
    cluster.shutdown()


def test_masterless_no_single_point_of_failure():
    """
    Unlike primary-secondary, any 2 nodes can form quorum.
    Test that different pairs of nodes can handle requests.
    """
    cluster = make_masterless_cluster()
    
    # Test scenario 1: n1 + n2 alive
    cluster.mark_down("n3")
    cluster.set("k1", "v1")
    assert cluster.get("k1") == "v1"
    cluster.mark_up("n3")
    
    # Test scenario 2: n1 + n3 alive
    cluster.mark_down("n2")
    cluster.set("k2", "v2")
    assert cluster.get("k2") == "v2"
    cluster.mark_up("n2")
    
    # Test scenario 3: n2 + n3 alive
    cluster.mark_down("n1")
    cluster.set("k3", "v3")
    assert cluster.get("k3") == "v3"
    
    cluster.shutdown()