# LakeSync

[![CI](https://github.com/radekdymacz/lakesync/actions/workflows/ci.yml/badge.svg)](https://github.com/radekdymacz/lakesync/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**Offline-first sync engine for browser apps — tracks column-level changes locally and lands them as Parquet in an Iceberg lakehouse.**

LakeSync synchronises data between browser clients and a shared lakehouse backend. Instead of syncing entire rows, it tracks changes at the **column level** — so when one user edits a title and another marks a task complete, both changes merge cleanly without conflict. Data flows through a sync gateway into **Apache Parquet files** managed by an **Iceberg-compatible catalogue**, giving you a full audit trail and time-travel queries out of the box.

## Why LakeSync?

Most sync engines force a choice: real-time collaboration (Firebase, Supabase) **or** analytical power (data lakes). LakeSync bridges that gap.

| | Traditional Sync | Data Lake | LakeSync |
|---|---|---|---|
| Offline-first | Yes | No | **Yes** |
| Column-level conflict resolution | Rarely | N/A | **Yes** |
| Data lands in Parquet/Iceberg | No | Yes | **Yes** |
| Time-travel queries | No | Yes | **Yes** |
| Runs on the edge (CF Workers) | Sometimes | No | **Yes** |

**The key insight:** every client mutation is a _delta_ — a small, timestamped change to specific columns. These deltas are the sync protocol, the conflict resolution input, _and_ the data lake records. One data model serves three purposes.

## How It Works

### The Journey of a Single Change

When a user edits a field in the browser, here's what happens:

```mermaid
sequenceDiagram
    participant User
    participant LocalDB as Local DB (sql.js)
    participant Tracker as SyncTracker
    participant Queue as IDB Queue
    participant Transport as HTTP Transport
    participant Gateway as Sync Gateway
    participant Store as Object Store (R2/S3)
    participant Cat as Catalogue (Nessie)

    User->>LocalDB: UPDATE todos SET title = 'Buy milk'
    LocalDB-->>Tracker: Row diff detected
    Tracker->>Tracker: Extract column-level delta
    Tracker->>Tracker: Stamp with HLC timestamp
    Tracker->>Queue: Enqueue delta (pending)

    Note over Queue,Transport: Background sync cycle (every 10s or on tab focus)

    Queue-->>Transport: Peek pending deltas
    Transport->>Gateway: POST /sync/:id/push {deltas, clientId, hlc}
    Gateway->>Gateway: Validate HLC (clock drift check)
    Gateway->>Gateway: Resolve conflicts (LWW per column)
    Gateway->>Gateway: Append to DeltaBuffer
    Gateway-->>Transport: {serverHlc, accepted}
    Transport-->>Queue: Ack — remove from queue

    Note over Gateway,Cat: Periodic flush (time or size threshold)

    Gateway->>Store: Write Parquet file
    Gateway->>Cat: Commit Iceberg snapshot
```

### Column-Level Conflict Resolution

Traditional sync engines resolve conflicts at the row level — if two users edit the same row, one wins and the other loses. LakeSync resolves at the **column level** using Last-Write-Wins (LWW) with Hybrid Logical Clocks:

```mermaid
graph LR
    subgraph "Client A (t=100)"
        A["UPDATE row-1<br/>title = 'Buy milk'"]
    end
    subgraph "Client B (t=101)"
        B["UPDATE row-1<br/>completed = true"]
    end
    subgraph "Gateway (merged)"
        M["row-1:<br/>title = 'Buy milk' ← from A<br/>completed = true ← from B"]
    end
    A --> M
    B --> M
```

Both changes are preserved because they touch different columns. The HLC timestamp determines the winner only when two clients modify the _same_ column.

### Offline-First Sync Cycle

The client works fully offline. Mutations queue in IndexedDB and sync when connectivity returns:

```mermaid
stateDiagram-v2
    [*] --> Pending: User mutation
    Pending --> Sending: Sync cycle starts
    Sending --> Acked: Gateway accepts
    Sending --> Pending: Network failure (nack)
    Pending --> Pending: Exponential backoff
    Pending --> DeadLettered: Max retries exceeded
    Acked --> [*]
    DeadLettered --> [*]
```

Failed pushes use exponential backoff (1s, 2s, 4s... capped at 30s) and are dead-lettered after 10 retries to prevent queue starvation.

## Architecture

```mermaid
graph TB
    subgraph "Browser"
        UI[Application UI]
        SC[SyncCoordinator]
        ST[SyncTracker]
        DB[(LocalDB<br/>sql.js + IDB)]
        Q[(IDB Queue)]
    end

    subgraph "Edge (Cloudflare Workers)"
        GW[SyncGateway<br/>Durable Object]
        BUF[DeltaBuffer<br/>Log + Index]
        AUTH[JWT Auth]
    end

    subgraph "Storage"
        R2[(R2 / MinIO / S3)]
        PQ[Parquet Files]
    end

    subgraph "Catalogue"
        NS[Nessie]
        ICE[Iceberg Snapshots]
    end

    subgraph "Analytics"
        CMP[Compactor]
        DDB[DuckDB-WASM]
    end

    UI --> SC
    SC --> ST
    ST --> DB
    ST --> Q
    SC -->|HTTP/WS| AUTH
    AUTH --> GW
    GW --> BUF
    BUF -->|flush| R2
    R2 --> PQ
    GW -->|commit| NS
    NS --> ICE
    CMP -->|compact| R2
    CMP -->|commit| NS
    DDB -->|query| PQ
```

### Key Design Decisions

- **HLC timestamps** (branded bigints) — 48-bit wall clock + 16-bit counter, giving monotonic ordering across distributed clients without coordination
- **Deterministic delta IDs** — SHA-256 hash of `(clientId, hlc, table, rowId, columns)` enables idempotent push
- **DeltaBuffer** — dual structure (append log for ordering + row index for conflict resolution) gives O(1) conflict checks and O(n) flush
- **Result\<T, E\>** everywhere — no exceptions cross API boundaries; all errors are typed and composable
- **Parquet as the flush format** — columnar storage is a natural fit for column-level deltas, and Iceberg gives you schema evolution + time travel for free

## Conflict Semantics

LakeSync uses **column-level Last-Write-Wins (LWW)** with deterministic tiebreaks. Here are the exact rules:

### Same Row, Different Columns

No conflict. Both changes are preserved in the merged result. This is the common case — two users editing different fields of the same record.

### Same Row, Same Column

The delta with the **higher HLC timestamp** wins. If timestamps are identical (rare — requires same millisecond + same counter), the **lexicographically higher `clientId`** wins. This tiebreak is deterministic: all clients converge to the same state regardless of the order they receive deltas.

### DELETE vs UPDATE

Treated as a whole-row conflict resolved by HLC comparison:
- If the DELETE has the higher HLC, the row stays deleted (tombstoned)
- If the UPDATE has the higher HLC, the row is **resurrected** with the UPDATE's columns

### What LakeSync Guarantees

- **Convergence**: all clients that sync the same set of deltas will arrive at the same state
- **Causality per client**: deltas from a single client are always applied in order
- **No data loss between columns**: edits to different columns of the same row never clobber each other
- **Idempotent push**: re-pushing the same delta (same `deltaId`) is a no-op

### What LakeSync Does Not Guarantee

- **No global total order**: deltas from different clients are ordered by HLC, not by a central sequencer
- **No merge for same-column conflicts**: when two clients edit the same column, one value wins entirely — there is no character-level or list-level merge
- **No intent preservation**: LWW does not know _why_ a value changed, only _when_. A "set price to $10" and "set price to $20" conflict is resolved by timestamp, not business logic

### Recommended Patterns

| Scenario | Approach |
|----------|----------|
| Collaborative text editing | Use one column per field (title, description), not one column for the whole document |
| Counters / accumulators | Model as append-only deltas with application-level reduction, not as a single mutable column |
| Dependent fields (e.g. subtotal + tax + total) | Compute derived fields on read, store only the independent inputs |
| Strong consistency required | LakeSync is eventually consistent — use a traditional database for strict serialisability |

> For the full delta format, wire protocol, and storage layout, see [docs/delta-spec.md](docs/delta-spec.md). For the security model and threat analysis, see [docs/security.md](docs/security.md).

## When Not to Use LakeSync

LakeSync is designed for **collaborative apps with independent field edits** (task managers, forms, CMS, inventory). It is not the right fit for every use case:

- **Financial ledgers or double-entry accounting** — LWW can silently drop a concurrent write. Use a serialisable transaction log instead.
- **Heavy same-column contention** — if many users frequently edit the exact same field (e.g. a shared counter or a wiki paragraph), LWW will discard all but the latest write. Consider a CRDT-based editor for character-level merging.
- **Strict CRDT semantics** — LakeSync's conflict resolution is LWW, not an operation-based or state-based CRDT. If you need grow-only sets, PN-counters, or RGA sequences, use a dedicated CRDT library.
- **Sub-second real-time** — the default sync cycle is 10 seconds (configurable). For <100ms latency, use a WebSocket-native solution like Liveblocks or Yjs.
- **Large binary blobs** — deltas are JSON-serialised column values. Syncing images or files should go through an object store with LakeSync tracking only the metadata/URL.

## Interoperability

### Iceberg Compatibility

LakeSync writes **Apache Iceberg v2** table metadata. Flushed data lands as Parquet files registered through the [Iceberg REST Catalogue API](https://iceberg.apache.org/spec/#iceberg-rest-catalog), with Nessie as the default catalogue backend.

### Query Engines

Once data is flushed, it is standard Parquet + Iceberg — queryable by any engine that speaks the format:

| Engine | Access Method |
|--------|--------------|
| **DuckDB** | Direct Parquet reads or Iceberg metadata |
| **Apache Spark** | Iceberg connector |
| **Trino / Presto** | Iceberg connector |
| **Snowflake** | External Iceberg tables |
| **BigQuery** | BigLake Iceberg tables |
| **Pandas / Polars** | `read_parquet()` on the object store |

### Object Store Support

| Store | Status |
|-------|--------|
| Cloudflare R2 | Primary target (gateway-worker) |
| AWS S3 | Supported via `@lakesync/adapter` |
| MinIO | Supported (S3-compatible, used in integration tests) |
| GCS / Azure Blob | Not yet tested — PRs welcome |

## Quick Start

### Install

```bash
npm install @lakesync/client @lakesync/core
```

### Sync in 10 Lines

```typescript
import { LocalDB, SyncCoordinator, HttpTransport } from "@lakesync/client";

const db = await LocalDB.open({ name: "my-app", backend: "idb" });

const transport = new HttpTransport({
  baseUrl: "https://your-gateway.workers.dev",
  gatewayId: "my-gateway",
  token: "your-jwt-token",
});

const coordinator = new SyncCoordinator(db, transport);
coordinator.startAutoSync();

// Track mutations — deltas are extracted and queued automatically
await coordinator.tracker.insert("todos", "row-1", {
  title: "Buy milk",
  completed: 0,
});
```

### Run Locally

```bash
git clone https://github.com/radekdymacz/lakesync.git
cd lakesync
bun install
bun run build
bun run test
```

### Deploy the Gateway

```bash
cd apps/gateway-worker
wrangler r2 bucket create lakesync-data  # once
wrangler deploy
```

See the [Todo App](apps/examples/todo-app/) for a complete working example, or the [Gateway Worker README](apps/gateway-worker/README.md) for deployment details.

## Packages

| Package | Description |
|---------|-------------|
| [`@lakesync/core`](packages/core) | HLC timestamps, delta types, LWW conflict resolution, Result type |
| [`@lakesync/client`](packages/client) | Client SDK: SyncCoordinator, SyncTracker, HTTP + local transports, IDB queue & persistence |
| [`@lakesync/gateway`](packages/gateway) | Sync gateway with delta buffer, conflict resolution, Parquet/JSON flush |
| [`@lakesync/adapter`](packages/adapter) | Storage adapter interface + MinIO/S3 implementation |
| [`@lakesync/proto`](packages/proto) | Protobuf codec for the wire protocol |
| [`@lakesync/parquet`](packages/parquet) | Parquet read/write via parquet-wasm |
| [`@lakesync/catalogue`](packages/catalogue) | Iceberg REST catalogue client (Nessie-compatible) |
| [`@lakesync/compactor`](packages/compactor) | Parquet compaction + equality delete files |
| [`@lakesync/analyst`](packages/analyst) | Time-travel queries + analytics via DuckDB-WASM |

| App | Description |
|-----|-------------|
| [`gateway-worker`](apps/gateway-worker) | Cloudflare Workers deployment: Durable Object gateway, R2 storage, JWT auth |
| [`todo-app`](apps/examples/todo-app) | Reference implementation: offline-first todo list with column-level sync |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

## Licence

Licensed under the [Apache Licence 2.0](LICENSE).
