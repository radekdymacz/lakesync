# ADR-009: Deterministic delta identity and idempotent push

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

After a gateway restart or network failure, clients re-push unacknowledged deltas to ensure no data is lost. Without deduplication, this re-push creates duplicate entries in the delta log, leading to:

- **Duplicate application:** The same column change applied twice (harmless for idempotent values but incorrect for computed/derived state).
- **Inflated storage:** Duplicate deltas consume storage in the delta log and increase compaction work.
- **Incorrect conflict resolution:** Duplicate deltas with the same HLC could interfere with the LWW algorithm if not properly deduplicated.

The gateway needs a mechanism to detect and discard duplicate deltas efficiently.

## Decision

Each `RowDelta` carries a deterministic `deltaId` computed as a SHA-256 hash of its identity fields:

```
deltaId = SHA-256({
  clientId,
  hlc,
  table,
  rowId,
  columns
})
```

The input to the hash is produced using `fast-json-stable-stringify`, which serialises the object with keys in sorted order, ensuring that the same logical delta always produces the same hash regardless of JavaScript object key ordering.

The gateway deduplicates on `deltaId`:
- On receiving a delta, the gateway checks `DeltaBuffer.hasDelta(deltaId)`.
- If the `deltaId` already exists in the buffer, the push is acknowledged as a no-op (success response, no re-insertion).
- If the `deltaId` is new, the delta is appended to the buffer normally.

This makes the push operation idempotent — pushing the same delta any number of times has the same effect as pushing it once.

## Consequences

### Positive

- **Idempotent re-push after any failure mode:** Clients can safely re-push their entire unacknowledged delta queue after a disconnect, gateway restart, or network partition. Duplicates are silently discarded.
- **Exactly-once semantics without server-side transaction logs:** The deterministic hash serves as a natural deduplication key. No need for a separate transaction log or sequence number tracking on the server.
- **Deterministic across clients:** Two different clients producing the same logical delta (same fields, same values) will compute the same `deltaId`. This is a useful property for testing and debugging.
- **SHA-256 collision probability is negligible:** With a 256-bit hash, the probability of collision is approximately 1 in 2^128 for birthday-bound scenarios — far beyond any practical concern.
- **Simple implementation:** The deduplication logic is a single hash lookup, easy to implement and test.

### Negative

- **Adds ~50 bytes per delta for the hash:** Each delta carries a 32-byte SHA-256 hash (64 bytes in hex encoding, 44 bytes in base64). For small deltas, this is a meaningful overhead percentage.
- **Requires `fast-json-stable-stringify` for canonical serialisation:** Standard `JSON.stringify` does not guarantee key order, which would make the hash non-deterministic. The `fast-json-stable-stringify` dependency is required for correctness.
- **Hash computation cost:** SHA-256 computation on every delta adds CPU overhead. For typical delta sizes (<1KB), this is negligible (<1 microsecond per hash on modern hardware).

### Risks

- **`DeltaBuffer.hasDelta()` is O(n) on the log:** The current implementation performs a linear scan of the buffer to check for existing `deltaId` values. This is acceptable for Phase 1 buffer sizes (thousands to tens of thousands of deltas) but will need a hash index (e.g., `Set<string>`) in Phase 2 for larger workloads.
- **`fast-json-stable-stringify` correctness:** The deduplication correctness depends entirely on `fast-json-stable-stringify` producing identical output for semantically identical inputs. Edge cases (e.g., `undefined` values, `NaN`, `-0`) must be tested thoroughly.
- **Hash does not cover all fields:** The `deltaId` is computed from identity fields only (`clientId`, `hlc`, `table`, `rowId`, `columns`). Metadata fields (e.g., `deltaType`) are not included. If a client sends the same identity fields with a different `deltaType`, it would be treated as a duplicate. This is by design — the same identity fields should not produce different delta types.

## Alternatives Considered

- **Server-assigned sequence numbers:** The gateway assigns a monotonically increasing sequence number to each delta. Clients track the last acknowledged sequence number and resume from there. Rejected because it requires server-side state (the sequence counter) that must survive restarts, adding persistence complexity. It also does not handle the case where the client's push succeeded but the acknowledgement was lost (the client would re-push, and without content-based deduplication, duplicates would be inserted).
- **Client-assigned UUIDs:** Each client generates a random UUID for each delta. Rejected because random UUIDs are not deterministic — if a client re-creates a delta after a crash (from its local state), the new UUID would differ from the original, defeating deduplication.
- **Content-addressable storage (CAS) with full delta hash:** Hash the entire delta (including metadata, timestamps, everything). Rejected because any metadata change (e.g., a retry counter) would produce a different hash, preventing deduplication of semantically identical deltas.
- **Idempotency keys (client-generated, opaque):** Clients generate an opaque idempotency key for each operation. Rejected because it pushes deduplication responsibility to the client without guaranteeing determinism. Two clients producing the same logical delta would generate different idempotency keys.
