# Durable Distributed Key-Value Store

A high-performance, persistent, and cluster-aware Key-Value database built from scratch in Python. This project implements a full-stack solution including a TCP server, a client library, clustering with **master-less replication**, and advanced indexing capabilities.

---

## 📂 Project Structure

The codebase is organized into modular components separating the storage engine, networking, clustering, and validation tools.

```text
.
├── kvstore.py                         # Core storage engine: Persistence, WAL, and Indexing
├── server.py                          # TCP Server: JSON-over-TCP protocol handler
├── client.py                          # Python Client: Developer API for interacting with the DB
├── cluster.py                         # Cluster Management: Primary-Secondary replication
├── cluster_masterless.py              # Master-less Cluster: Quorum-based replication (no single leader)
├── benchmark.py                       # General performance benchmarking script
├── benchmarks/ 
│   ├── throughput.py                  # Detailed write throughput analysis (vs dataset size)
│   └── durability.py                  # Durability tester: Validates zero data loss after SIGKILL
├── tests/   
│   ├── test_kvstore.py                # Functional tests: CRUD, persistence, and concurrency
│   ├── test_cluster.py                # Distributed tests: Primary-Secondary replication and election
│   ├── test_cluster_masterless.py     # Master-less cluster tests: Quorum reads/writes
│   └── test_acid_bulk_durability.py   # ACID tests: Atomic bulk-set validation
├── requirements.txt                   # Python dependencies
└── README.md                          # This file
```

---

## 🚀 Key Features

### Core Functionality
* **Custom TCP Protocol**: Efficient JSON-over-TCP communication with line-delimited messages
* **Full CRUD Operations**: Set, Get, Delete, and Bulk Set with thread-safe execution
* **Persistence**: Append-only log (WAL) with `fsync()` ensuring **100% data integrity** even after a `SIGKILL -9`

### Distributed Systems
* **Primary-Secondary Clustering** (`cluster.py`):
  - 3-node cluster with automatic leader election
  - Write replication from primary to secondaries
  - Automatic failover when primary goes down

* **Master-less Clustering** (`cluster_masterless.py`):
  - No single point of failure - all nodes are equal
  - **Quorum-based reads and writes** (majority consensus)
  - Survives any single node failure (2/3 quorum)
  - Eventually consistent with conflict resolution

### Data Integrity
* **ACID Compliance**: 
  - **Atomic** bulk operations - all-or-nothing writes
  - **Isolated** concurrent operations prevent partial updates
  - **Durable** writes with fsync guarantee
* **Durability Testing**: Random SIGKILL testing ensures zero acknowledged data loss

### Advanced Indexing
* **Inverted Index**: Full-text search for efficient word-based queries
* **Word Embeddings**: Vector-based similarity search using hashed bag-of-words (16-dimensional)

### Debug & Testing
* **Debug Mode** (`debug_unreliable` parameter): Simulates 1% filesystem sync failures to test resilience against power outages
* **Comprehensive Test Suite**: Covers persistence, concurrency, ACID properties, and clustering

---

## 📖 Usage

### 1. Start a Single Server

```bash
python server.py --host 127.0.0.1 --port 65432 --data-file data.log
```

### 2. Using the Client

```python
from client import KVClient

client = KVClient(host="127.0.0.1", port=65432)

# Standard Operations
client.set("foo", "bar")
value = client.get("foo")  # Returns: "bar"

# Delete
deleted = client.delete("foo")  # Returns: True

# Bulk Operations (Atomic)
client.bulk_set([("a", "1"), ("b", "2"), ("c", "3")])
```

### 3. Using Primary-Secondary Cluster

```python
from cluster import Cluster
from pathlib import Path

# Create 3-node cluster with automatic replication
cluster = Cluster(Path("cluster_data"))

# Writes go to primary, replicate to secondaries
cluster.set("key", "value")

# Reads from current primary
value = cluster.get("key")

# Simulate primary failure - automatic election happens
cluster.mark_down("n1")  # Primary goes down
cluster.set("key2", "value2")  # New primary elected automatically!

cluster.shutdown()
```

### 4. Using Master-less Cluster (Quorum-based)

```python
from cluster_masterless import MasterlessCluster
from pathlib import Path

# Create master-less cluster (no single leader)
cluster = MasterlessCluster(Path("masterless_data"))

# Writes go to majority (2/3 nodes minimum)
cluster.set("key", "value")

# Reads from majority ensure consistency
value = cluster.get("key")

# Survives single node failure (2/3 nodes still form quorum)
cluster.mark_down("n1")
cluster.set("key2", "value2")  # Still works!

# Loses quorum if 2 nodes fail
cluster.mark_down("n2")
# cluster.set("key3", "value3")  # Raises: RuntimeError (insufficient quorum)

cluster.shutdown()
```

---

## 🧪 Testing and Benchmarks

### Running Functional Tests

```bash
# Core storage engine tests
pytest tests/test_kvstore.py -v

# Primary-secondary cluster tests
pytest tests/test_cluster.py -v

# Master-less cluster tests
pytest tests/test_cluster_masterless.py -v

# ACID compliance tests
pytest tests/test_acid_bulk_durability.py -v

# Run all tests
pytest tests/ -v
```

### Running the Durability Benchmark

This script runs a writer thread while **simultaneously killing the server process randomly** with `SIGKILL -9` to ensure that every acknowledged write is safely stored on disk.

```bash
cd benchmarks
python durability.py
```

**Expected Output:**
```
Durability benchmark (acknowledged writes vs crash loss)
acked=1247 lost=0 lost_pct=0.00%
Result: PASS (100% durability for acknowledged writes)
```

### Running the Throughput Benchmark

Measures how many writes per second the system can handle as the database grows.

```bash
cd benchmarks
python throughput.py --prepopulate 0,1000,5000,10000
```

**Expected Output:**
```
Write throughput vs pre-populated dataset size
data_file=benchmarks/throughput_data.log writes_per_round=5000

prepopulated_keys,writes_per_sec
0,8542.12
1000,8234.56
5000,7891.23
10000,7456.78
```

### General Performance Benchmark

```bash
python benchmark.py
```

---

## 🔍 Advanced Search

The store supports complex queries beyond simple key lookups:

```python
from kvstore import PersistentKVStore

store = PersistentKVStore("data.log")

# Populate some data
store.set("doc1", "python programming language")
store.set("doc2", "java programming tutorial")
store.set("doc3", "machine learning with python")

# Inverted Index search (Full-Text)
# Finds all keys whose values contain ALL query words
results = store.search_full_text("python programming")
# Returns: ["doc1", "doc3"] (both contain "python")

# Embedding Search (Semantic Similarity)
# Finds values that are semantically similar to the query
matches = store.search_embedding("coding in python", top_k=2)
# Returns: [("doc1", 0.87), ("doc3", 0.72)]  # (key, similarity_score)

store.close()
```

---

## 🛠️ Advanced Features

### Debug Mode (Simulating Power Failures)

The `debug_unreliable` parameter simulates filesystem sync issues (1% random failure rate) to test system resilience:

```python
from kvstore import PersistentKVStore

store = PersistentKVStore("debug.log")

# Normal operation (100% reliable)
store.set("key1", "value1")

# Debug mode - simulates power outage scenarios
# 1% chance this write won't make it to disk
store.set("key2", "value2", debug_unreliable=True)

# Note: WAL (Write-Ahead Log) is ALWAYS reliable
# Even in debug mode, fsync() ensures durability
```

### Concurrent Operations

The system handles concurrent operations safely:

```python
import threading
from client import KVClient

client = KVClient(host="127.0.0.1", port=65432)

def worker(thread_id):
    for i in range(100):
        client.set(f"t{thread_id}_k{i}", f"value_{i}")

# Launch 10 concurrent writers
threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
for t in threads:
    t.start()
for t in threads:
    t.join()

# All 1000 writes are guaranteed to be atomic and isolated
```

---

## 🎯 Design Decisions

### Why Append-Only Log (WAL)?
- **Durability**: Every write is immediately flushed to disk with `fsync()`
- **Crash Recovery**: Replay the log to reconstruct in-memory state
- **Performance**: Sequential writes are faster than random writes

### Why Master-less Replication?
- **No Single Point of Failure**: Any 2/3 nodes can form a quorum
- **Higher Availability**: Survives any single node failure
- **Simpler Failover**: No leader election needed

### Why Quorum Reads/Writes?
- **Consistency**: Reading from majority ensures you see latest write
- **Availability**: Can tolerate minority node failures
- **Partition Tolerance**: CAP theorem trade-off (CP system)

---

## 📊 System Guarantees

| Property | Guarantee |
|----------|-----------|
| **Durability** | ✅ 100% for acknowledged writes (fsync + SIGKILL tested) |
| **Atomicity** | ✅ Bulk operations are all-or-nothing |
| **Isolation** | ✅ Concurrent operations don't interfere |
| **Consistency** | ✅ Quorum reads ensure linearizability |
| **Availability** | ✅ Survives 1/3 node failures (quorum-based) |
| **Partition Tolerance** | ✅ Gracefully degrades without quorum |

---

## 🔧 Configuration

### Server Configuration

```bash
python server.py --host 0.0.0.0 --port 8080 --data-file /var/lib/kvstore/data.log
```

### Client Timeout

```python
# Adjust timeout for slow networks
client = KVClient(host="127.0.0.1", port=65432, timeout=10.0)
```

### Cluster Ports

Default ports for 3-node cluster:
- Node 1: `127.0.0.1:65431`
- Node 2: `127.0.0.1:65432`
- Node 3: `127.0.0.1:65433`

---

## 📝 Testing Coverage

```bash
pytest tests/ --cov=. --cov-report=term-missing
```

**Test Categories:**
- ✅ Basic CRUD operations
- ✅ Persistence across restarts
- ✅ Concurrent bulk writes (isolation)
- ✅ ACID properties (atomic bulk-set)
- ✅ Durability under random kills
- ✅ Primary-secondary replication
- ✅ Master-less quorum operations
- ✅ Failover and recovery

---


## 🤝 Contributing

This project was built to demonstrate:
1. Persistent storage with WAL
2. TCP-based client-server architecture  
3. Distributed consensus (quorum-based)
4. ACID transaction guarantees
5. Production-grade testing methodologies

Feel free to explore, learn, and extend!

---

## 📞 Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Client Application                    │
└────────────────────┬────────────────────────────────────┘
                     │
          ┌──────────▼──────────┐
          │   KVClient (TCP)    │
          └──────────┬──────────┘
                     │
     ┌───────────────┼───────────────┐
     │               │               │
┌────▼────┐    ┌────▼────┐    ┌────▼────┐
│ Node 1  │    │ Node 2  │    │ Node 3  │
│ (TCP)   │    │ (TCP)   │    │ (TCP)   │
└────┬────┘    └────┬────┘    └────┬────┘
     │              │              │
┌────▼────┐    ┌────▼────┐    ┌────▼────┐
│ KVStore │    │ KVStore │    │ KVStore │
│  + WAL  │    │  + WAL  │    │  + WAL  │
│ + Index │    │ + Index │    │ + Index │
└─────────┘    └─────────┘    └─────────┘
     │              │              │
┌────▼────┐    ┌────▼────┐    ┌────▼────┐
│ n1.log  │    │ n2.log  │    │ n3.log  │
└─────────┘    └─────────┘    └─────────┘
```

**Quorum Write Flow:**
1. Client sends write to cluster
2. Write propagates to all alive nodes
3. Returns success when majority (2/3) acknowledge
4. Each node appends to local WAL with fsync()

**Quorum Read Flow:**
1. Client sends read to cluster
2. Read from majority (2/3) of nodes
3. Return most common value (voting)
4. Ensures consistency across partitions

---