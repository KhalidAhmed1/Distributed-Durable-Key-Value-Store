# Durable Distributed Key-Value Store

A high-performance, persistent, and cluster-aware Key-Value database built from scratch in Python. This project implements a full-stack solution including a TCP server, a client library, clustering with master-less replication, and advanced indexing capabilities.

## 📂 Project Structure

The codebase is organized into modular components separating the storage engine, networking, and validation tools.

```text
.
├── kvstore.py                         # Core storage engine: Persistence, WAL, and Indexing
├── server.py                          # TCP Server: JSON-over-TCP protocol handler
├── client.py                          # Python Client: Developer API for interacting with the DB
├── cluster.py                         # Cluster Management: Master-less replication and failover
├── benchmark.py                       # General performance benchmarking script
├── benchmarks/ 
|    ├── throughput.py                 # Detailed write throughput analysis (vs dataset size)
|    └── durability.py                 # Durability tester: Validates zero data loss after SIGKILL
├── tests/   
|    ├── test_kvstore.py               # Functional tests: CRUD, persistence, and concurrency
|    ├── test_cluster.py               # Distributed tests: Replication and automatic election
|    └── test_acid_bulk_durability.py  # ACID tests: Atomic bulk-set validation

```

## 🚀 Key Features

* **Custom TCP Protocol**: Efficient JSON-over-TCP communication.
* **Persistence**: Append-only log (WAL) with `fsync` ensuring 100% data integrity even after a `SIGKILL`.
* **Clustering**: Master-less replication across 3 nodes with automatic failover and election logic.
* **ACID Compliance**: Atomic and isolated bulk operations to prevent partial writes.
* **Advanced Indexing**:
* **Inverted Index**: Full-text search for word matches.
* **Word Embeddings**: Vector-based similarity search using hashed bag-of-words.



---

## 📖 Usage

### 1. Start the Server

```bash
python server.py --host 127.0.0.1 --port 65432 --data-file data.log

```

### 2. Using the Client

```python
from client import KVClient

client = KVClient(host="127.0.0.1", port=65432)

# Standard Operations
client.set("foo", "bar")
value = client.get("foo")

# Bulk Operations (Atomic)
client.bulk_set([("a", "1"), ("b", "2")])

```

---

## 🧪 Testing and Benchmarks

### Running Functional Tests

```bash
pytest test_kvstore.py
pytest test_cluster.py

```

### Running the Durability Benchmark

This script runs a writer thread while simultaneously killing the server process randomly with `SIGKILL -9` to ensure that every acknowledged write is safely stored on disk.

```bash
python durability.py

```

### Running the Throughput Benchmark

Measures how many writes per second the system can handle as the database grows.

```bash
python throughput.py --prepopulate 0,1000,5000,10000

```

---

## 🔍 Advanced Search

The store supports complex queries beyond simple key lookups:

```python
# Inverted Index search (Full-Text)
results = store.search_full_text("query_word")

# Embedding Search (Similarity)
# Finds values that are semantically similar to the input string
matches = store.search_embedding("related text")

```

## 🛠 Implementation Details

* **Durability**: Every write operation is serialized to JSON and appended to a log file. During startup, the log is replayed to rebuild the state.
* **Master-less Cluster**: Nodes use a deterministic coordinator selection. If the primary node is marked "down," the next available node in the sequence automatically takes over.
* **Unreliable Disk Simulation**: Includes a `debug_unreliable` flag (1% failure rate) to test system resilience against filesystem synchronization issues.