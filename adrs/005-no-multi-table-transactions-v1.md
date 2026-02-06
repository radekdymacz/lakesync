# ADR-005: No multi-table transactions in v1.0

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

Multi-table transactions allow atomic changes across multiple tables — either all tables are updated or none are. Implementing this in a distributed sync protocol requires:

- **Distributed transactions:** Two-phase commit (2PC) or three-phase commit across all participating tables.
- **Ordering guarantees:** Ensuring deltas for related tables are applied in the correct order across all replicas.
- **Rollback mechanisms:** Undoing partial changes if any table in the transaction fails.
- **Deadlock detection:** Preventing circular waits when multiple clients lock tables in different orders.

This complexity is substantial and would significantly delay the delivery of the core sync functionality.

## Decision

v1.0 does not support multi-table transactions. Each table syncs independently — its deltas are pushed, resolved, and committed without coordination with other tables. Cross-table consistency is the application's responsibility.

Tables are fully independent units of synchronisation:
- Each table has its own delta log.
- Conflict resolution operates within a single table.
- The gateway processes each table's deltas independently.

## Consequences

### Positive

- **Drastically simplifies the sync protocol:** No need for distributed transaction coordinators, two-phase commit, or cross-table locking.
- **Tables are parallelisable:** The gateway can process deltas for different tables concurrently without coordination, improving throughput.
- **Independent failure domains:** A failure in one table's sync does not block or roll back other tables.
- **Faster time to market:** Removing cross-table coordination eliminates a major source of complexity and edge cases.

### Negative

- **No atomic cross-table updates:** An application that updates an `orders` table and an `order_items` table cannot guarantee both updates are visible atomically. Readers may see the order without its items (or vice versa) during the sync window.
- **Application-level coordination burden:** Applications needing cross-table consistency must implement their own mechanisms, which shifts complexity to the application developer.

### Risks

- **Data inconsistency windows:** During sync, related tables may be temporarily inconsistent. Applications must tolerate or handle these windows.
- **User expectations:** Developers accustomed to relational database transactions may expect multi-table atomicity. Clear documentation is needed to set expectations.
- **Migration complexity:** Adding multi-table transactions in a future version may require protocol changes that are not backward-compatible with v1.0 clients.

## Alternatives Considered

- **Two-phase commit (2PC):** Rejected due to blocking behaviour — if the coordinator fails during the commit phase, all participants are locked until recovery. This is unacceptable for a sync system where clients may go offline.
- **Saga pattern (built-in):** Rejected for v1.0 because implementing compensating transactions for arbitrary table operations is complex and error-prone. Applications that need saga-like behaviour can implement it themselves using LakeSync's per-table guarantees.
- **Eventual cross-table consistency with causal ordering:** Considered but rejected for v1.0. This would require vector clocks or dependency tracking across tables, adding significant metadata overhead. May be revisited in Phase 2.
- **Bundled delta groups:** A simpler approach where deltas for multiple tables are submitted as a group and applied together. Rejected because it still requires atomic commit across tables on the gateway, which introduces the same coordination complexity as full transactions.
