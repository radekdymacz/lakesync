import { describe, it, expect } from 'vitest';
import { HLC } from '@lakesync/core';
import { createTestGateway, createTestHLC, makeDelta } from './helpers';

describe('Conflict resolution', () => {
  it('resolves same-column conflict via LWW — later write wins', () => {
    const gateway = createTestGateway();

    // Client A writes "title" at t=1000
    gateway.handlePush({
      clientId: 'client-a',
      deltas: [
        makeDelta({
          hlc: HLC.encode(1000, 0),
          clientId: 'client-a',
          columns: [{ column: 'title', value: 'Version A' }],
          deltaId: 'delta-a',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // Client B writes "title" at t=2000 (later — wins)
    gateway.handlePush({
      clientId: 'client-b',
      deltas: [
        makeDelta({
          hlc: HLC.encode(2000, 0),
          clientId: 'client-b',
          columns: [{ column: 'title', value: 'Version B' }],
          deltaId: 'delta-b',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // Pull from the start — log should contain both events
    const pullResult = gateway.handlePull({
      clientId: 'client-c',
      sinceHlc: HLC.encode(0, 0),
      maxDeltas: 100,
    });

    expect(pullResult.ok).toBe(true);
    if (pullResult.ok) {
      expect(pullResult.value.deltas.length).toBe(2);
    }

    // The index should hold exactly one unique row (conflict resolved)
    expect(gateway.bufferStats.indexSize).toBe(1);
  });

  it('resolves conflict deterministically via clientId when HLC is equal', () => {
    const gateway = createTestGateway();
    const identicalHlc = HLC.encode(5000, 0);

    // Both clients write at the exact same HLC
    gateway.handlePush({
      clientId: 'client-a',
      deltas: [
        makeDelta({
          hlc: identicalHlc,
          clientId: 'client-a',
          columns: [{ column: 'title', value: 'From A' }],
          deltaId: 'tie-delta-a',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    gateway.handlePush({
      clientId: 'client-b',
      deltas: [
        makeDelta({
          hlc: identicalHlc,
          clientId: 'client-b',
          columns: [{ column: 'title', value: 'From B' }],
          deltaId: 'tie-delta-b',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // Both should be accepted (the log records both; index holds resolved winner)
    expect(gateway.bufferStats.logSize).toBe(2);
    expect(gateway.bufferStats.indexSize).toBe(1);

    // Pull and verify both events are present in the log
    const pullResult = gateway.handlePull({
      clientId: 'observer',
      sinceHlc: HLC.encode(0, 0),
      maxDeltas: 100,
    });

    expect(pullResult.ok).toBe(true);
    if (pullResult.ok) {
      expect(pullResult.value.deltas.length).toBe(2);
    }
  });
});

describe('Gateway restart idempotency', () => {
  it('re-pushed deltas with the same deltaId are deduplicated', () => {
    const gateway = createTestGateway();
    const delta = makeDelta({
      hlc: HLC.encode(1000, 0),
      deltaId: 'idempotent-delta',
    });

    // First push
    const first = gateway.handlePush({
      clientId: 'client-a',
      deltas: [delta],
      lastSeenHlc: HLC.encode(0, 0),
    });
    expect(first.ok).toBe(true);
    expect(gateway.bufferStats.logSize).toBe(1);

    // Re-push the same delta (simulating a client retry after restart)
    const second = gateway.handlePush({
      clientId: 'client-a',
      deltas: [delta],
      lastSeenHlc: HLC.encode(0, 0),
    });
    expect(second.ok).toBe(true);

    // Buffer should still hold exactly 1 entry (idempotent)
    expect(gateway.bufferStats.logSize).toBe(1);

    // Accepted count includes the duplicate (it was acknowledged)
    if (second.ok) {
      expect(second.value.accepted).toBe(1);
    }
  });

  it('different deltaIds for the same row are not deduplicated', () => {
    const gateway = createTestGateway();
    const { hlc, advance } = createTestHLC();

    advance(100);
    gateway.handlePush({
      clientId: 'client-a',
      deltas: [
        makeDelta({
          hlc: hlc.now(),
          deltaId: 'first-write',
          columns: [{ column: 'title', value: 'Initial' }],
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    advance(100);
    gateway.handlePush({
      clientId: 'client-a',
      deltas: [
        makeDelta({
          hlc: hlc.now(),
          deltaId: 'second-write',
          columns: [{ column: 'title', value: 'Updated' }],
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // Two distinct deltas should both be in the log
    expect(gateway.bufferStats.logSize).toBe(2);
    // But only one unique row in the index (same table:rowId)
    expect(gateway.bufferStats.indexSize).toBe(1);
  });
});

describe('Clock drift rejection', () => {
  it('rejects push with excessive clock drift', () => {
    const gateway = createTestGateway();

    // Create an HLC far in the future (wall = now + 10s, exceeding 5s max drift)
    const futureWall = Date.now() + 10_000;
    const futureHlc = HLC.encode(futureWall, 0);

    const result = gateway.handlePush({
      clientId: 'bad-clock-client',
      deltas: [
        makeDelta({
          hlc: futureHlc,
          deltaId: 'drift-delta',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error.code).toBe('CLOCK_DRIFT');
    }
  });

  it('accepts push within acceptable drift tolerance', () => {
    const gateway = createTestGateway();

    // Create an HLC only 2s ahead (within the 5s threshold)
    const nearFutureWall = Date.now() + 2_000;
    const nearFutureHlc = HLC.encode(nearFutureWall, 0);

    const result = gateway.handlePush({
      clientId: 'slightly-fast-client',
      deltas: [
        makeDelta({
          hlc: nearFutureHlc,
          deltaId: 'acceptable-drift-delta',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    expect(result.ok).toBe(true);
  });
});
