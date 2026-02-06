# ADR-006: Hybrid Logical Clocks (64-bit, 5s drift)

**Status:** Decided
**Date:** 2026-02-06
**Deciders:** Radek Dymacz

## Context

LakeSync needs a timestamp scheme that provides causal ordering across distributed clients. Clients may be on different machines with unsynchronised clocks, and the ordering must be deterministic for conflict resolution (see ADR-002). The candidates are:

- **Lamport clocks:** A simple logical counter incremented on each event and updated on message receipt. Provides causal ordering but has no relation to wall-clock time — two events seconds apart may have adjacent Lamport timestamps, making them useless for "latest write" semantics.
- **Vector clocks:** A vector of counters, one per client. Provides exact causal ordering and can detect concurrent events. However, space grows linearly with the number of clients — with thousands of clients, each timestamp becomes kilobytes.
- **Hybrid Logical Clocks (HLC):** Combines wall-clock time with a logical counter. Provides causal ordering while maintaining a close relationship to physical time. Bounded space regardless of client count.

## Decision

64-bit Hybrid Logical Clock with the following structure:

```
Bits 63-16 (48 bits): Wall clock time in milliseconds since Unix epoch
Bits 15-0  (16 bits): Logical counter
```

Key properties:
- **48-bit wall clock:** Supports timestamps until the year 10889 at millisecond resolution.
- **16-bit logical counter:** Allows up to 65,535 events within the same millisecond before forcing wall advancement.
- **Maximum allowed drift:** 5 seconds. If a received HLC's wall component exceeds the local wall clock by more than 5 seconds, the delta is rejected.
- **Monotonic-safe:** Backward NTP adjustments are absorbed into the logical counter. The HLC never goes backwards — if `Date.now()` returns a value less than the current HLC's wall component, the logical counter is incremented instead.

The `now()` algorithm:
1. Read `Date.now()` as `physicalTime`.
2. If `physicalTime > hlc.wallMs`: set `wallMs = physicalTime`, `counter = 0`.
3. If `physicalTime <= hlc.wallMs`: increment `counter`.
4. If `counter > 65535`: set `wallMs = hlc.wallMs + 1`, `counter = 0` (forced wall advancement).
5. Return `(wallMs << 16) | counter` as a 64-bit value.

The `receive(remoteHlc)` algorithm:
1. Read `Date.now()` as `physicalTime`.
2. If `remoteHlc.wallMs - physicalTime > 5000`: reject (drift exceeded).
3. Set `wallMs = max(physicalTime, hlc.wallMs, remoteHlc.wallMs)`.
4. Adjust counter based on which wall component(s) matched the maximum.
5. Return updated HLC.

## Consequences

### Positive

- **Bounded space:** A single 64-bit integer per event, regardless of the number of clients. This is critical for embedding timestamps in every delta without bloating payloads.
- **Causal ordering preserved:** If event A causally precedes event B (A happened-before B), then `HLC(A) < HLC(B)`.
- **Wall-clock affinity:** HLC timestamps are close to real time, making them meaningful for human inspection and debugging.
- **Monotonic guarantee:** The clock never goes backwards, even when NTP adjusts the system clock. This prevents the "time went backwards" class of bugs.
- **Counter overflow handling:** Forced wall advancement by 1ms when the counter overflows ensures the clock always progresses, even under extreme event rates (>65,535 events/ms).
- **Compatible with Protobuf `fixed64` encoding:** The 64-bit integer maps directly to Protobuf's `fixed64` type (see ADR-008), avoiding varint encoding overhead for timestamps.

### Negative

- **5-second drift window is a tradeoff:** Too tight and legitimate events from poorly synchronised clients are rejected. Too loose and conflict resolution becomes less meaningful (a 5-second-old write could beat a current write).
- **No sub-millisecond precision:** The wall clock component has millisecond resolution. Events within the same millisecond are distinguished only by the logical counter, which does not reflect actual time ordering within that millisecond.
- **BigInt required in JavaScript:** 64-bit integers exceed JavaScript's `Number.MAX_SAFE_INTEGER`. All HLC operations must use `BigInt`, which has performance implications compared to regular numbers.

### Risks

- **Clock synchronisation dependency:** The 5-second drift window assumes clients have reasonably synchronised clocks (e.g., via NTP). Clients with grossly incorrect clocks will have their deltas rejected, potentially causing data loss if not handled gracefully.
- **Leap seconds:** Leap second insertions could cause brief clock anomalies. The monotonic-safe design absorbs these, but applications should be aware.
- **Counter exhaustion under extreme load:** More than 65,535 events in a single millisecond forces artificial wall advancement, which could cause the HLC to drift ahead of real time. This is self-correcting (real time catches up) but could temporarily affect conflict resolution fairness.

## Alternatives Considered

- **Lamport clocks:** Rejected because they have no relation to wall-clock time. LakeSync's conflict resolution (ADR-002) requires "latest write" semantics, which demands timestamps with wall-clock affinity.
- **Vector clocks:** Rejected because space grows linearly with the number of clients. With potentially thousands of sync clients, each timestamp would become prohibitively large. Vector clocks also complicate serialisation and comparison.
- **TrueTime (Google Spanner-style):** Rejected because it requires specialised hardware (GPS receivers, atomic clocks) for bounded clock uncertainty. LakeSync must run on commodity hardware and consumer devices.
- **Centrally-issued timestamps (server clock only):** Rejected because it requires the server to be available for every event, preventing offline operation. LakeSync clients must be able to generate timestamps independently while offline.
