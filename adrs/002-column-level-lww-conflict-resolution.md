# ADR-002: Column-level Last-Write-Wins conflict resolution

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

Distributed sync requires a conflict resolution strategy when two or more clients modify the same row concurrently (i.e., without having seen each other's changes). The candidates are:

- **Operational Transform (OT):** Transforms operations against each other. Well-suited for text editing but overly complex for structured row/column data.
- **CRDTs (Conflict-free Replicated Data Types):** Mathematically guaranteed convergence. Powerful but imposes constraints on data types and adds significant implementation complexity.
- **Row-level Last-Write-Wins (LWW):** The entire row with the latest timestamp wins. Simple but discards non-conflicting column changes — if Client A changes column X and Client B changes column Y, one client's change is lost entirely.
- **Column-level LWW:** Each column is resolved independently using timestamps. Non-conflicting column changes are preserved.

## Decision

Column-level LWW using Hybrid Logical Clock (HLC) timestamps with `clientId` tiebreak for equal timestamps. The resolution algorithm is:

1. For each column present in both conflicting deltas, the value with the higher HLC wins.
2. If HLC timestamps are exactly equal (same wall clock, same logical counter), the higher `clientId` (lexicographic comparison) wins. This ensures deterministic, total ordering.
3. Columns present in only one delta are always included in the merged result — they represent non-conflicting changes.

## Consequences

### Positive

- **Deterministic:** Given the same set of deltas, any node produces the same merged result regardless of processing order.
- **Commutative:** `merge(A, B) === merge(B, A)` — the order in which deltas arrive does not affect the outcome.
- **Convergent:** All replicas eventually reach the same state, satisfying strong eventual consistency.
- **Simple to implement and reason about:** The algorithm is a straightforward per-column comparison, easy to test and debug.
- **Preserves non-conflicting changes:** If Client A modifies `name` and Client B modifies `email`, both changes are preserved in the merged result.

### Negative

- Cannot handle semantic conflicts — for example, two clients incrementing a counter will result in only one increment being preserved, not both. Applications requiring counter semantics need application-level logic.
- Relies on reasonably synchronised clocks. Badly skewed clocks can cause "older" writes to win if their wall clock is ahead. HLC mitigates this but does not eliminate it entirely (see ADR-006 for drift bounds).
- No user-facing conflict notification — conflicts are resolved silently. Applications requiring user review of conflicts must implement their own layer.

### Risks

- **DELETE vs UPDATE interaction:** Resolved by HLC ordering. A later DELETE tombstones the row; a later UPDATE resurrects it. This can surprise users if a deleted row reappears due to a concurrent update with a higher HLC. Documenting this behaviour is essential.
- **Clock skew beyond 5 seconds:** Deltas with HLC drift exceeding the 5-second threshold are rejected (see ADR-006), which could cause legitimate writes to be dropped if a client's clock is significantly wrong.

## Alternatives Considered

- **Operational Transform:** Rejected as overly complex for structured tabular data. OT is designed for sequential text operations, not independent column mutations.
- **CRDTs:** Rejected for v1.0 due to implementation complexity and constraints on data types. Phase 2 may introduce CRDT-based counters or sets for specific column types.
- **Row-level LWW:** Rejected because it discards non-conflicting column changes. In a sync system where multiple clients frequently edit different columns of the same row, this leads to unacceptable data loss.
- **Application-level resolution (manual merge):** Rejected for v1.0 as it requires UI/UX for conflict presentation and user interaction. Column-level LWW provides a sensible automatic default.
