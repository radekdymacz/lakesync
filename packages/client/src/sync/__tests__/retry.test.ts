import type { HLCTimestamp, Result, RowDelta, SyncResponse } from "@lakesync/core";
import { Err, HLC, LakeSyncError } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { LocalDB } from "../../db/local-db";
import { registerSchema } from "../../db/schema-registry";
import { MemoryQueue } from "../../queue/memory-queue";
import { SyncCoordinator } from "../coordinator";
import type { SyncTransport } from "../transport";

const todoSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" as const },
		{ name: "completed", type: "boolean" as const },
	],
};

function makeDelta(id: string): RowDelta {
	return {
		op: "UPDATE",
		table: "todos",
		rowId: id,
		clientId: "test-client",
		columns: [{ column: "title", value: "Test" }],
		hlc: HLC.encode(Date.now(), 0),
		deltaId: `delta-${id}`,
	};
}

/** Transport that always fails */
const failTransport: SyncTransport = {
	push: async (): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>> =>
		Err(new LakeSyncError("Network error", "TRANSPORT_ERROR")),
	pull: async (): Promise<Result<SyncResponse, LakeSyncError>> =>
		Err(new LakeSyncError("Network error", "TRANSPORT_ERROR")),
};

describe("Retry with exponential backoff", () => {
	beforeEach(() => {
		vi.useFakeTimers();
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	describe("MemoryQueue backoff", () => {
		it("nack sets retryAfter with exponential backoff", async () => {
			const queue = new MemoryQueue();
			const delta = makeDelta("1");
			const pushResult = await queue.push(delta);
			expect(pushResult.ok).toBe(true);
			if (!pushResult.ok) return;

			const entryId = pushResult.value.id;
			await queue.markSending([entryId]);
			await queue.nack([entryId]);

			// After first nack, retryCount = 1, backoff = min(1000 * 2^1, 30000) = 2000ms
			const peekResult = await queue.peek(10);
			expect(peekResult.ok).toBe(true);
			if (!peekResult.ok) return;
			// Entry should NOT be visible yet (retryAfter is in the future)
			expect(peekResult.value).toHaveLength(0);
		});

		it("peek skips entries with future retryAfter", async () => {
			const queue = new MemoryQueue();
			const delta = makeDelta("1");
			await queue.push(delta);

			const peekBefore = await queue.peek(10);
			expect(peekBefore.ok).toBe(true);
			if (!peekBefore.ok) return;
			const entryId = peekBefore.value[0]?.id;
			expect(entryId).toBeDefined();

			await queue.markSending([entryId!]);
			await queue.nack([entryId!]);

			// Immediately after nack, entry has retryAfter in the future
			const peekImmediate = await queue.peek(10);
			expect(peekImmediate.ok).toBe(true);
			if (!peekImmediate.ok) return;
			expect(peekImmediate.value).toHaveLength(0);
		});

		it("entry becomes peekable again after backoff period expires", async () => {
			const queue = new MemoryQueue();
			const delta = makeDelta("1");
			await queue.push(delta);

			const peekBefore = await queue.peek(10);
			expect(peekBefore.ok).toBe(true);
			if (!peekBefore.ok) return;
			const entryId = peekBefore.value[0]?.id;
			expect(entryId).toBeDefined();

			await queue.markSending([entryId!]);
			await queue.nack([entryId!]);

			// retryCount = 1, backoff = 2000ms
			// Advance time past the backoff period
			vi.advanceTimersByTime(2001);

			const peekAfter = await queue.peek(10);
			expect(peekAfter.ok).toBe(true);
			if (!peekAfter.ok) return;
			expect(peekAfter.value).toHaveLength(1);
			expect(peekAfter.value[0]?.id).toBe(entryId);
			expect(peekAfter.value[0]?.retryCount).toBe(1);
		});

		it("backoff increases exponentially with each nack", async () => {
			const queue = new MemoryQueue();
			const delta = makeDelta("1");
			await queue.push(delta);

			const peekFirst = await queue.peek(10);
			expect(peekFirst.ok).toBe(true);
			if (!peekFirst.ok) return;
			const entryId = peekFirst.value[0]?.id;
			expect(entryId).toBeDefined();

			// First nack: retryCount=1, backoff = 2000ms
			await queue.markSending([entryId!]);
			await queue.nack([entryId!]);
			vi.advanceTimersByTime(2001);

			let peek = await queue.peek(10);
			expect(peek.ok && peek.value.length).toBe(1);

			// Second nack: retryCount=2, backoff = 4000ms
			await queue.markSending([entryId!]);
			await queue.nack([entryId!]);

			// Not visible at 3999ms
			vi.advanceTimersByTime(3999);
			peek = await queue.peek(10);
			expect(peek.ok && peek.value.length).toBe(0);

			// Visible at 4001ms total
			vi.advanceTimersByTime(2);
			peek = await queue.peek(10);
			expect(peek.ok && peek.value.length).toBe(1);
		});

		it("backoff caps at 30 seconds", async () => {
			const queue = new MemoryQueue();
			const delta = makeDelta("1");
			await queue.push(delta);

			const peekFirst = await queue.peek(10);
			expect(peekFirst.ok).toBe(true);
			if (!peekFirst.ok) return;
			const entryId = peekFirst.value[0]?.id;
			expect(entryId).toBeDefined();

			// Nack many times to reach the cap
			for (let i = 0; i < 10; i++) {
				await queue.markSending([entryId!]);
				await queue.nack([entryId!]);
				// Advance enough to make entry peekable again
				vi.advanceTimersByTime(31_000);
			}

			// retryCount=10, backoff = min(1000 * 2^10, 30000) = min(1024000, 30000) = 30000
			await queue.markSending([entryId!]);
			await queue.nack([entryId!]);

			// Not visible at 29999ms
			vi.advanceTimersByTime(29_999);
			let peek = await queue.peek(10);
			expect(peek.ok && peek.value.length).toBe(0);

			// Visible at 30001ms
			vi.advanceTimersByTime(2);
			peek = await queue.peek(10);
			expect(peek.ok && peek.value.length).toBe(1);
		});
	});

	describe("SyncCoordinator dead-lettering", () => {
		it("entries with retryCount >= maxRetries are dead-lettered", async () => {
			const dbResult = await LocalDB.open({ name: "test-retry", backend: "memory" });
			expect(dbResult.ok).toBe(true);
			if (!dbResult.ok) return;
			const db = dbResult.value;
			await registerSchema(db, todoSchema);

			const queue = new MemoryQueue();
			const hlc = new HLC();

			const maxRetries = 3;
			const coordinator = new SyncCoordinator(db, failTransport, {
				queue,
				hlc,
				clientId: "test-client",
				maxRetries,
			});

			// Push a delta via tracker
			await coordinator.tracker.insert("todos", "row-1", {
				title: "Test",
				completed: 0,
			});

			const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

			// Repeatedly push and advance time to exhaust retries
			for (let i = 0; i < maxRetries; i++) {
				// Advance past any backoff
				vi.advanceTimersByTime(31_000);
				await coordinator.pushToGateway();
			}

			// At this point, each push failed and called nack, incrementing retryCount.
			// After maxRetries nacks, the entry should have retryCount = maxRetries.
			// Next push should dead-letter it.
			vi.advanceTimersByTime(31_000);
			await coordinator.pushToGateway();

			// Queue should be empty now (entry dead-lettered)
			const depth = await queue.depth();
			expect(depth.ok).toBe(true);
			if (!depth.ok) return;
			expect(depth.value).toBe(0);

			// console.warn should have been called
			expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("Dead-lettering"));

			warnSpy.mockRestore();
			await db.close();
		});

		it("entries below maxRetries are retried normally", async () => {
			const dbResult = await LocalDB.open({ name: "test-retry-normal", backend: "memory" });
			expect(dbResult.ok).toBe(true);
			if (!dbResult.ok) return;
			const db = dbResult.value;
			await registerSchema(db, todoSchema);

			const queue = new MemoryQueue();
			const hlc = new HLC();

			const coordinator = new SyncCoordinator(db, failTransport, {
				queue,
				hlc,
				clientId: "test-client",
				maxRetries: 10,
			});

			// Push a delta
			await coordinator.tracker.insert("todos", "row-1", {
				title: "Test",
				completed: 0,
			});

			// Push once (will fail and nack)
			await coordinator.pushToGateway();

			// Entry should still be in queue (retryCount = 1, below maxRetries)
			const depth = await queue.depth();
			expect(depth.ok).toBe(true);
			if (!depth.ok) return;
			expect(depth.value).toBe(1);

			await db.close();
		});
	});
});
