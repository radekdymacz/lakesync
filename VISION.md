# Vision

**Declare what data goes where. The engine handles the rest.**

LakeSync is NativeKloud's **shared, open-source sync library** — not a marketed end-user product. NativeKloud apps (duebox, ogar, Atlas, and others) reuse the same engine: local SQLite on the device, a thin gateway, and pluggable backends. Same client code in every product.

This document is the product vision for that library. It matches the README; it does not describe a different product.

## The problem

Most sync engines lock you into a single backend. Most data lakes are not offline-first. Browser apps still have to invent their own outbox, conflict rules, and catch-up protocol — then throw that away when the storage target changes.

NativeKloud products would otherwise each grow a private sync stack. Data would not flow between apps, agents, and backends; it would be copied.

## What LakeSync is

A declarative TypeScript sync engine with three layers:

1. **Client** — mutations write to local SQLite (sql.js WASM) at zero latency. Deltas queue in a persistent IndexedDB outbox that survives refresh and crash. When the network returns, the outbox drains.
2. **Gateway** — a thin merge point (Cloudflare Durable Objects or self-hosted Node/Bun). Hybrid Logical Clocks plus column-level last-write-wins preserve concurrent edits to different fields. Sync rules filter what each client may pull. WebSocket broadcast is optional; HTTP polling is the fallback.
3. **Adapters** — the storage and source layer. Every adapter is both a readable source and a writable destination. Swap backends without changing client code.

Local SQLite is one destination among many, not the whole product. Deltas can also materialise into Postgres, MySQL, or BigQuery tables, or land as Parquet in an Iceberg catalogue on S3/R2.

## What it is not

- Not a consumer SaaS or a closed BaaS. The library is Apache 2.0; NativeKloud products embed it, and anyone can self-host the gateway.
- Not a warehouse-only product. Offline browser apps are a first-class consumer.
- Not a promise that every SaaS API is already wired up. Shipped connectors today are Jira and Salesforce; the adapter/poller interfaces are the extension point for the rest.

## Who uses it

LakeSync is infrastructure for NativeKloud products that need a local, queryable database and later sync:

- **duebox**, **ogar**, **Atlas** — and any future NK app that should share the same sync, conflict, and adapter model
- Agents and dashboards that need a filtered slice of a larger system, not a full replica
- Operators who want one pipeline from SaaS sources (Jira, Salesforce, …) into the same destinations

The library is the public-facing artefact. The products are consumers.

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
- **One engine, many products.** NativeKloud apps share this library instead of each inventing sync.
- **Open source.** Apache 2.0. Self-host or run at the edge.

## North star

NativeKloud apps, agents, and backends share one sync engine. You declare the data and the rules. The engine handles the rest.
