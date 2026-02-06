import { describe, it, expect } from 'vitest';
import { HLC } from '@lakesync/core';
import { MemoryQueue } from '@lakesync/client';
import { createTestGateway, makeDelta } from './helpers';

describe('Two clients non-conflicting', () => {
  it('merges different columns from two clients into the same row', () => {
    const gateway = createTestGateway();
    const hlcA = new HLC(() => 1_000_000);
    const hlcB = new HLC(() => 1_000_100);

    // Client A writes the "title" column
    gateway.handlePush({
      clientId: 'client-a',
      deltas: [
        makeDelta({
          hlc: hlcA.now(),
          clientId: 'client-a',
          columns: [{ column: 'title', value: 'Buy milk' }],
          deltaId: 'delta-a-1',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // Client B writes the "completed" column to the same row
    gateway.handlePush({
      clientId: 'client-b',
      deltas: [
        makeDelta({
          hlc: hlcB.now(),
          clientId: 'client-b',
          columns: [{ column: 'completed', value: true }],
          deltaId: 'delta-b-1',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // A third client pulling from the start should see both changes
    const pullResult = gateway.handlePull({
      clientId: 'client-c',
      sinceHlc: HLC.encode(0, 0),
      maxDeltas: 100,
    });

    expect(pullResult.ok).toBe(true);
    if (pullResult.ok) {
      expect(pullResult.value.deltas.length).toBe(2);
    }
  });

  it('index reflects merged state for a single row', () => {
    const gateway = createTestGateway();
    const hlcA = new HLC(() => 1_000_000);
    const hlcB = new HLC(() => 1_000_100);

    gateway.handlePush({
      clientId: 'client-a',
      deltas: [
        makeDelta({
          hlc: hlcA.now(),
          clientId: 'client-a',
          columns: [{ column: 'title', value: 'Buy milk' }],
          deltaId: 'delta-idx-a',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    gateway.handlePush({
      clientId: 'client-b',
      deltas: [
        makeDelta({
          hlc: hlcB.now(),
          clientId: 'client-b',
          columns: [{ column: 'completed', value: true }],
          deltaId: 'delta-idx-b',
        }),
      ],
      lastSeenHlc: HLC.encode(0, 0),
    });

    // Both writes target the same row, so the index should hold exactly one entry
    expect(gateway.bufferStats.indexSize).toBe(1);
  });
});

describe('Offline queue drain', () => {
  it('queues 50 deltas offline then pushes all at once', async () => {
    const queue = new MemoryQueue();
    const hlc = new HLC(() => Date.now());

    // Queue 50 deltas while "offline"
    for (let i = 0; i < 50; i++) {
      await queue.push(
        makeDelta({
          rowId: `row-${i}`,
          hlc: hlc.now(),
          deltaId: `offline-${i}`,
          op: 'INSERT',
          columns: [{ column: 'title', value: `Todo ${i}` }],
        }),
      );
    }

    // Verify queue depth
    const depthResult = await queue.depth();
    expect(depthResult.ok).toBe(true);
    if (depthResult.ok) {
      expect(depthResult.value).toBe(50);
    }

    // "Come online" — drain the queue and push to the gateway
    const gateway = createTestGateway();
    const peekResult = await queue.peek(50);
    expect(peekResult.ok).toBe(true);
    if (!peekResult.ok) return;

    const ids = peekResult.value.map((e) => e.id);
    await queue.markSending(ids);

    const pushResult = gateway.handlePush({
      clientId: 'offline-client',
      deltas: peekResult.value.map((e) => e.delta),
      lastSeenHlc: HLC.encode(0, 0),
    });

    expect(pushResult.ok).toBe(true);
    if (pushResult.ok) {
      expect(pushResult.value.accepted).toBe(50);
    }

    // Acknowledge delivery
    await queue.ack(ids);
    const finalDepth = await queue.depth();
    expect(finalDepth.ok).toBe(true);
    if (finalDepth.ok) {
      expect(finalDepth.value).toBe(0);
    }
  });

  it('nack re-enqueues entries with incremented retry count', async () => {
    const queue = new MemoryQueue();
    const hlc = new HLC(() => Date.now());

    await queue.push(
      makeDelta({
        hlc: hlc.now(),
        deltaId: 'nack-delta',
        op: 'INSERT',
        columns: [{ column: 'title', value: 'Will fail' }],
      }),
    );

    const peek1 = await queue.peek(10);
    expect(peek1.ok).toBe(true);
    if (!peek1.ok) return;

    const ids = peek1.value.map((e) => e.id);
    await queue.markSending(ids);

    // Simulate a network failure — nack the entries
    await queue.nack(ids);

    // Queue depth should still be 1 (entry re-enqueued as pending)
    const depth = await queue.depth();
    expect(depth.ok).toBe(true);
    if (depth.ok) {
      expect(depth.value).toBe(1);
    }

    // Retry count should have incremented
    const peek2 = await queue.peek(10);
    expect(peek2.ok).toBe(true);
    if (peek2.ok) {
      expect(peek2.value[0]?.retryCount).toBe(1);
    }
  });
});
