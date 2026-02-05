from pathlib import Path
import uuid

from cluster import Cluster


def make_cluster() -> Cluster:
    base = Path("benchmarks") / "cluster_tmp" / str(uuid.uuid4())
    return Cluster(base)


def test_replication_to_secondaries():
    cluster = make_cluster()

    # Initial primary should be n1
    assert cluster.primary_id == "n1"

    # Write through primary
    cluster.set("k", "v")

    # All nodes should see the same value by reading via current primary
    # After stopping the primary, new primary should still see the value.
    assert cluster.get("k") == "v"

    # Kill current primary and ensure we can still read after election
    old_primary = cluster.primary_id  # n1
    cluster.mark_down(old_primary)    # bring n1 down
    # Next operation should trigger election
    assert cluster.get("k") == "v"
    new_primary = cluster.primary_id
    assert new_primary != old_primary  # should now be one of the secondaries
    assert cluster.get("k") == "v"


def test_primary_failover_on_write():
    cluster = make_cluster()

    # Write some data
    cluster.bulk_set([("a", "1"), ("b", "2")])
    assert cluster.get("a") == "1"

    # Mark primary down and then perform another write which should
    # trigger election implicitly.
    old_primary = cluster.primary_id
    cluster.mark_down(old_primary)
    cluster.set("c", "3")

    # Primary should have changed
    new_primary = cluster.primary_id
    assert new_primary != old_primary

    # All writes should still be readable via the cluster API
    assert cluster.get("a") == "1"
    assert cluster.get("b") == "2"
    assert cluster.get("c") == "3"

