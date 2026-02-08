import { MemoryQueue } from "@lakesync/client";
import type { RowDelta } from "@lakesync/core";
import { HLC, unwrapOrThrow } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function makeQueueDelta(index: number): RowDelta {
	const hlc = new HLC(() => 1_000_000 + index);
	return {
		op: "UPDATE",
		table: "todos",
		rowId: `row-${index}`,
		clientId: "queue-client",
		columns: [{ column: "title", value: `Item ${index}` }],
		hlc: hlc.now(),
		deltaId: `queue-delta-${index}`,
	};
}

describe("Queue Resilience", () => {
	beforeEach(() => {
		vi.useFakeTimers();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	it("10K pending entries — depth is correct", async () => {
		const queue = new MemoryQueue();
		const N = 10_000;

		for (let i = 0; i < N; i++) {
			unwrapOrThrow(await queue.push(makeQueueDelta(i)));
		}

		const depth = unwrapOrThrow(await queue.depth());
		expect(depth).toBe(N);

		// Peek should return entries
		const peeked = unwrapOrThrow(await queue.peek(100));
		expect(peeked).toHaveLength(100);
	});

	it("nack/backoff cycle — exponential backoff formula verified", async () => {
		const queue = new MemoryQueue();
		vi.setSystemTime(10_000);

		const entry = unwrapOrThrow(await queue.push(makeQueueDelta(0)));

		// Nack 1: backoff = min(1000 * 2^1, 30000) = 2000ms
		unwrapOrThrow(await queue.nack([entry.id]));
		let peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(0); // not yet peekable

		vi.advanceTimersByTime(2001);
		peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(1);

		// Nack 2: backoff = min(1000 * 2^2, 30000) = 4000ms
		unwrapOrThrow(await queue.nack([entry.id]));
		peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(0);

		vi.advanceTimersByTime(4001);
		peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(1);

		// Nack 3: backoff = min(1000 * 2^3, 30000) = 8000ms
		unwrapOrThrow(await queue.nack([entry.id]));
		peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(0);

		vi.advanceTimersByTime(8001);
		peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(1);
	});

	it("backoff caps at 30 seconds", async () => {
		const queue = new MemoryQueue();
		vi.setSystemTime(10_000);

		const entry = unwrapOrThrow(await queue.push(makeQueueDelta(0)));

		// Nack many times to exceed cap
		// backoff at retryCount 5 = min(1000 * 2^5, 30000) = 32000 → capped to 30000
		for (let i = 0; i < 5; i++) {
			unwrapOrThrow(await queue.nack([entry.id]));
			vi.advanceTimersByTime(31_000); // advance past any backoff
		}

		// After 5 nacks, retryCount = 5, backoff = min(1000 * 2^5, 30000) = 30000
		unwrapOrThrow(await queue.nack([entry.id]));

		// Should not be peekable before 30s
		vi.advanceTimersByTime(29_999);
		let peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(0);

		// Should be peekable after 30s
		vi.advanceTimersByTime(2);
		peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(1);
	});

	it("large batch ack — all 10K entries cleared in one pass", async () => {
		const queue = new MemoryQueue();
		const N = 10_000;
		const ids: string[] = [];

		for (let i = 0; i < N; i++) {
			const entry = unwrapOrThrow(await queue.push(makeQueueDelta(i)));
			ids.push(entry.id);
		}

		expect(unwrapOrThrow(await queue.depth())).toBe(N);

		// Ack all at once
		unwrapOrThrow(await queue.ack(ids));

		expect(unwrapOrThrow(await queue.depth())).toBe(0);
		const peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(0);
	});

	it("interleaved push/peek/ack — no lost entries", async () => {
		const queue = new MemoryQueue();
		let totalPushed = 0;
		let totalAcked = 0;

		for (let round = 0; round < 10; round++) {
			// Push 100
			for (let i = 0; i < 100; i++) {
				unwrapOrThrow(await queue.push(makeQueueDelta(totalPushed)));
				totalPushed++;
			}

			// Peek and ack 50
			const peeked = unwrapOrThrow(await queue.peek(50));
			expect(peeked.length).toBeLessThanOrEqual(50);
			unwrapOrThrow(await queue.ack(peeked.map((e) => e.id)));
			totalAcked += peeked.length;
		}

		const remaining = unwrapOrThrow(await queue.depth());
		expect(remaining).toBe(totalPushed - totalAcked);
	});

	it("nack does not duplicate entries", async () => {
		const queue = new MemoryQueue();
		vi.setSystemTime(10_000);

		const entry = unwrapOrThrow(await queue.push(makeQueueDelta(0)));

		// Nack multiple times
		for (let i = 0; i < 5; i++) {
			unwrapOrThrow(await queue.nack([entry.id]));
			vi.advanceTimersByTime(60_000); // advance past all backoff
		}

		// Depth should still be 1
		const depth = unwrapOrThrow(await queue.depth());
		expect(depth).toBe(1);

		// Peek should return exactly 1
		const peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(1);
		expect(peeked[0]!.id).toBe(entry.id);
		expect(peeked[0]!.retryCount).toBe(5);
	});

	it("markSending prevents peek from returning entries", async () => {
		const queue = new MemoryQueue();

		const e1 = unwrapOrThrow(await queue.push(makeQueueDelta(0)));
		const e2 = unwrapOrThrow(await queue.push(makeQueueDelta(1)));

		// Mark e1 as sending
		unwrapOrThrow(await queue.markSending([e1.id]));

		// Peek should only return e2
		const peeked = unwrapOrThrow(await queue.peek(10));
		expect(peeked).toHaveLength(1);
		expect(peeked[0]!.id).toBe(e2.id);

		// Depth should still count both
		const depth = unwrapOrThrow(await queue.depth());
		expect(depth).toBe(2);
	});
});
