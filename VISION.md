# Vision

**Declare what data goes where. The engine handles the rest.**

LakeSync is an **open-source, offline-first sync engine** for TypeScript apps. Your data lives in SQLite on the device, syncs through a lightweight gateway, and flushes to the backend you choose. Same client code either way.

This document is the public product vision. It matches the README; it does not describe a different product.

## The problem

Most sync engines lock you into a single backend. Most data lakes are not offline-first. Browser apps still have to invent their own outbox, conflict rules, and catch-up protocol — then throw that away when the storage target changes.

Teams end up with one stack for the local app, another for operational SQL, and a third for analytics. Data does not flow; it is copied.

## What LakeSync is

A declarative sync engine with three layers:

1. **Client** — mutations write to local SQLite (sql.js WASM) at zero latency. Deltas queue in a persistent IndexedDB outbox that survives refresh and crash. When the network returns, the outbox drains.
2. **Gateway** — a thin merge point (Cloudflare Durable Objects or self-hosted Node/Bun). Hybrid Logical Clocks plus column-level last-write-wins preserve concurrent edits to different fields. Sync rules filter what each client may pull. WebSocket broadcast is optional; HTTP polling is the fallback.
3. **Adapters** — the storage and source layer. Every adapter is both a readable source and a writable destination. Swap backends without changing client code.

Local SQLite is one destination among many, not the whole product. Deltas can also materialise into Postgres, MySQL, or BigQuery tables, or land as Parquet in an Iceberg catalogue on S3/R2.

## What it is not

- Not a closed BaaS. The library is Apache 2.0; you can self-host the gateway.
- Not a warehouse-only product. Offline browser apps are a first-class consumer.
- Not a promise that every SaaS API is already wired up. Shipped connectors today are Jira and Salesforce; the adapter/poller interfaces are the extension point for the rest.

## Backends, sized to the data

| Scale | Path |
|---|---|
| Small / operational | Client SQLite → Gateway → Postgres or MySQL |
| Analytics | Client SQLite → Gateway → BigQuery (or fan-out a replica) |
| Large / lake | Client SQLite → Gateway → Apache Iceberg on S3/R2 |

Route by table with `CompositeAdapter`. Replicate with `FanOutAdapter`. Age data from hot to cold with `LifecycleAdapter`. Move between backends with `migrateAdapter()`.

## Principles

- **Declare the flow, don't orchestrate it.** Sync rules and adapters say what goes where. The engine merges, batches, and flushes.
- **Offline-first.** The app works on a plane. Catch-up is automatic.
- **Pluggable storage.** Sync is decoupled from the lake. The adapter interface is the extension point.
- **Column-level truth.** Conflicts resolve per column, not per row.
- **Batch to the lake.** Never flush per-sync to Iceberg.
- **Open APIs, no thrown exceptions.** Public surfaces return `Result<T, E>`.
- **Open source.** Apache 2.0. Self-host or run at the edge.

## Who it is for

- Browser and TypeScript apps that need a local, queryable database and later sync
- Teams that want Postgres today and Iceberg tomorrow without rewriting the client
- Agents and dashboards that need a filtered slice of a larger system, not a full replica
- Operators who want one pipeline from SaaS sources (Jira, Salesforce, …) into the same destinations

## North star

Apps, agents, and backends share one sync engine. You declare the data and the rules. The engine handles the rest.
