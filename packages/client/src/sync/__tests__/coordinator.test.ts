import type { RowDelta } from "@lakesync/core";
import { Err, HLC, LakeSyncError as LakeSyncErrorClass, Ok } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LocalDB } from "../../db/local-db";
import type { QueueEntry, SyncQueue } from "../../queue/types";
import { SyncCoordinator } from "../coordinator";
import type { SyncTransport } from "../transport";

// ────────────────────────────────────────────────────────────
// Helpers — builds mock objects used throughout the suite
// ────────────────────────────────────────────────────────────

/** Create a minimal RowDelta for testing purposes */
function makeDelta(overrides?: Partial<RowDelta>): RowDelta {
	return {
		op: "INSERT",
		table: "todos",
		rowId: "row-1",
		clientId: "test-client",
		columns: [{ column: "title", value: "Buy milk" }],
		hlc: HLC.encode(1_000_000, 0),
		deltaId: "delta-1",
		...overrides,
	};
}

/** Create a QueueEntry wrapping a RowDelta */
function makeEntry(id: string, delta: RowDelta, retryCount = 0): QueueEntry {
	return {
		id,
		delta,
		status: "pending",
		createdAt: Date.now(),
		retryCount,
	};
}

/** Build a fully-mocked SyncQueue using vi.fn() */
function mockQueue(): SyncQueue {
	return {
		push: vi.fn<SyncQueue["push"]>().mockResolvedValue(Ok(makeEntry("q-1", makeDelta()))),
		peek: vi.fn<SyncQueue["peek"]>().mockResolvedValue(Ok([])),
		markSending: vi.fn<SyncQueue["markSending"]>().mockResolvedValue(Ok(undefined)),
		ack: vi.fn<SyncQueue["ack"]>().mockResolvedValue(Ok(undefined)),
		nack: vi.fn<SyncQueue["nack"]>().mockResolvedValue(Ok(undefined)),
		depth: vi.fn<SyncQueue["depth"]>().mockResolvedValue(Ok(0)),
		clear: vi.fn<SyncQueue["clear"]>().mockResolvedValue(Ok(undefined)),
	};
}

/** Build a fully-mocked SyncTransport using vi.fn() */
function mockTransport(): SyncTransport {
	return {
		push: vi
			.fn<SyncTransport["push"]>()
			.mockResolvedValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 1 })),
		pull: vi
			.fn<SyncTransport["pull"]>()
			.mockResolvedValue(Ok({ deltas: [], serverHlc: HLC.encode(2_000_000, 0), hasMore: false })),
	};
}

/**
 * Build a mock LocalDB that returns Ok for exec/query.
 * The applier uses exec (for SQL writes) and query is unused in push/pull,
 * but we provide both for completeness.
 */
function mockLocalDB(): LocalDB {
	return {
		exec: vi.fn().mockResolvedValue(Ok(undefined)),
		query: vi.fn().mockResolvedValue(Ok([])),
		name: "test-coordinator",
		backend: "memory",
		close: vi.fn().mockResolvedValue(undefined),
		save: vi.fn().mockResolvedValue(Ok(undefined)),
		transaction: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as LocalDB;
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

describe("SyncCoordinator", () => {
	let db: LocalDB;
	let transport: SyncTransport;
	let queue: SyncQueue;
	let hlc: HLC;
	let coordinator: SyncCoordinator;

	const CLIENT_ID = "test-client";
	const MAX_RETRIES = 3;

	beforeEach(() => {
		db = mockLocalDB();
		transport = mockTransport();
		queue = mockQueue();
		hlc = new HLC(() => 1_000_000);

		coordinator = new SyncCoordinator(db, transport, {
			queue,
			hlc,
			clientId: CLIENT_ID,
			maxRetries: MAX_RETRIES,
		});
	});

	// ── pushToGateway ──────────────────────────────────────

	describe("pushToGateway", () => {
		it("happy path — marks entries as sending, pushes, then acks", async () => {
			const entry1 = makeEntry("e-1", makeDelta({ deltaId: "d-1" }));
			const entry2 = makeEntry("e-2", makeDelta({ deltaId: "d-2", rowId: "row-2" }));

			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry1, entry2]));

			await coordinator.pushToGateway();

			// Should mark both entries as sending
			expect(queue.markSending).toHaveBeenCalledWith(["e-1", "e-2"]);

			// Should call transport.push with the deltas
			expect(transport.push).toHaveBeenCalledOnce();
			const pushArg = (transport.push as ReturnType<typeof vi.fn>).mock.calls[0]?.[0];
			expect(pushArg.clientId).toBe(CLIENT_ID);
			expect(pushArg.deltas).toHaveLength(2);

			// Should ack after successful push
			expect(queue.ack).toHaveBeenCalledWith(["e-1", "e-2"]);
			expect(queue.nack).not.toHaveBeenCalled();
		});

		it("transport failure nacks entries", async () => {
			const entry = makeEntry("e-1", makeDelta());
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

			const transportErr = new LakeSyncErrorClass("Network failure", "TRANSPORT_ERROR");
			(transport.push as ReturnType<typeof vi.fn>).mockResolvedValue(Err(transportErr));

			await coordinator.pushToGateway();

			// Should still mark as sending before the attempt
			expect(queue.markSending).toHaveBeenCalledWith(["e-1"]);

			// Should nack on failure
			expect(queue.nack).toHaveBeenCalledWith(["e-1"]);
			expect(queue.ack).not.toHaveBeenCalled();
		});

		it("empty queue is a no-op", async () => {
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([]));

			await coordinator.pushToGateway();

			expect(transport.push).not.toHaveBeenCalled();
			expect(queue.markSending).not.toHaveBeenCalled();
			expect(queue.ack).not.toHaveBeenCalled();
		});

		it("dead-letters entries that have reached maxRetries", async () => {
			const good = makeEntry("e-1", makeDelta({ deltaId: "d-good" }), 0);
			const deadLettered = makeEntry(
				"e-2",
				makeDelta({ deltaId: "d-dead", rowId: "row-2" }),
				MAX_RETRIES,
			);

			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([good, deadLettered]));

			await coordinator.pushToGateway();

			// Dead-lettered entry should be acked first (removed from queue)
			const ackCalls = (queue.ack as ReturnType<typeof vi.fn>).mock.calls;
			// First ack call: dead-lettered entries
			expect(ackCalls[0]).toEqual([["e-2"]]);

			// Remaining entry should be pushed normally
			expect(queue.markSending).toHaveBeenCalledWith(["e-1"]);
			expect(transport.push).toHaveBeenCalledOnce();

			// Second ack call: successful entries
			expect(ackCalls[1]).toEqual([["e-1"]]);
		});

		it("all entries dead-lettered skips push entirely", async () => {
			const dl1 = makeEntry("e-1", makeDelta({ deltaId: "d-1" }), MAX_RETRIES);
			const dl2 = makeEntry("e-2", makeDelta({ deltaId: "d-2", rowId: "row-2" }), MAX_RETRIES + 1);

			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([dl1, dl2]));

			await coordinator.pushToGateway();

			// Dead-lettered entries should be acked (removed)
			expect(queue.ack).toHaveBeenCalledWith(["e-1", "e-2"]);

			// No push should occur since all entries were dead-lettered
			expect(transport.push).not.toHaveBeenCalled();
			expect(queue.markSending).not.toHaveBeenCalled();
		});
	});

	// ── pullFromGateway ────────────────────────────────────

	describe("pullFromGateway", () => {
		it("happy path — applies remote deltas and advances cursor", async () => {
			const serverHlc = HLC.encode(5_000_000, 0);
			const remoteDelta = makeDelta({
				clientId: "remote-client",
				hlc: HLC.encode(4_000_000, 0),
				deltaId: "remote-d-1",
				op: "INSERT",
			});

			(transport.pull as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok({ deltas: [remoteDelta], serverHlc, hasMore: false }),
			);

			// The applier will call db.exec for CREATE TABLE, BEGIN, the INSERT, cursor update, COMMIT
			// Our mock db.exec returns Ok(undefined) for all calls, so applier succeeds.

			const applied = await coordinator.pullFromGateway();
			expect(applied).toBe(1);

			// Verify transport.pull was called
			expect(transport.pull).toHaveBeenCalledOnce();
			const pullArg = (transport.pull as ReturnType<typeof vi.fn>).mock.calls[0]?.[0];
			expect(pullArg.clientId).toBe(CLIENT_ID);
		});

		it("transport failure returns 0", async () => {
			const transportErr = new LakeSyncErrorClass("Network timeout", "TRANSPORT_ERROR");
			(transport.pull as ReturnType<typeof vi.fn>).mockResolvedValue(Err(transportErr));

			const applied = await coordinator.pullFromGateway();
			expect(applied).toBe(0);
		});

		it("empty deltas returns 0", async () => {
			(transport.pull as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok({ deltas: [], serverHlc: HLC.encode(1_000_000, 0), hasMore: false }),
			);

			const applied = await coordinator.pullFromGateway();
			expect(applied).toBe(0);
		});
	});

	// ── queueDepth ─────────────────────────────────────────

	describe("queueDepth", () => {
		it("returns depth from queue on success", async () => {
			(queue.depth as ReturnType<typeof vi.fn>).mockResolvedValue(Ok(42));

			const depth = await coordinator.queueDepth();
			expect(depth).toBe(42);
		});

		it("returns 0 when queue.depth returns an error", async () => {
			const depthErr = new LakeSyncErrorClass("Queue corrupted", "QUEUE_ERROR");
			(queue.depth as ReturnType<typeof vi.fn>).mockResolvedValue(Err(depthErr));

			const depth = await coordinator.queueDepth();
			expect(depth).toBe(0);
		});
	});

	// ── lastSyncTime ───────────────────────────────────────

	describe("lastSyncTime", () => {
		it("is null before any sync", () => {
			expect(coordinator.engine.lastSyncTime).toBeNull();
		});

		it("updates on successful push", async () => {
			const entry = makeEntry("e-1", makeDelta());
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

			const before = Date.now();
			await coordinator.pushToGateway();
			const after = Date.now();

			expect(coordinator.engine.lastSyncTime).not.toBeNull();
			const ts = coordinator.engine.lastSyncTime as Date;
			expect(ts.getTime()).toBeGreaterThanOrEqual(before);
			expect(ts.getTime()).toBeLessThanOrEqual(after);
		});

		it("does not update on failed push", async () => {
			const entry = makeEntry("e-1", makeDelta());
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

			const transportErr = new LakeSyncErrorClass("Fail", "TRANSPORT_ERROR");
			(transport.push as ReturnType<typeof vi.fn>).mockResolvedValue(Err(transportErr));

			await coordinator.pushToGateway();

			expect(coordinator.engine.lastSyncTime).toBeNull();
		});

		it("updates on successful pull", async () => {
			const remoteDelta = makeDelta({
				clientId: "remote-client",
				hlc: HLC.encode(4_000_000, 0),
				deltaId: "remote-d-1",
			});

			(transport.pull as ReturnType<typeof vi.fn>).mockResolvedValue(
				Ok({ deltas: [remoteDelta], serverHlc: HLC.encode(5_000_000, 0), hasMore: false }),
			);

			const before = Date.now();
			await coordinator.pullFromGateway();
			const after = Date.now();

			expect(coordinator.engine.lastSyncTime).not.toBeNull();
			const ts = coordinator.engine.lastSyncTime as Date;
			expect(ts.getTime()).toBeGreaterThanOrEqual(before);
			expect(ts.getTime()).toBeLessThanOrEqual(after);
		});
	});

	// ── clientId ────────────────────────────────────────────

	describe("clientId", () => {
		it("returns the configured client identifier", () => {
			expect(coordinator.engine.clientId).toBe(CLIENT_ID);
		});
	});

	// ── startAutoSync / stopAutoSync ───────────────────────

	describe("startAutoSync / stopAutoSync lifecycle", () => {
		beforeEach(() => {
			vi.useFakeTimers();
		});

		afterEach(() => {
			coordinator.stopAutoSync();
			vi.useRealTimers();
		});

		it("fires push and pull on each interval tick", async () => {
			// Provide entries so pushToGateway does work
			const entry = makeEntry("e-1", makeDelta());
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

			coordinator.startAutoSync();

			// Advance past one interval (10 seconds)
			await vi.advanceTimersByTimeAsync(10_000);

			expect(transport.push).toHaveBeenCalled();
			expect(transport.pull).toHaveBeenCalled();
		});

		it("stopAutoSync prevents further ticks", async () => {
			const entry = makeEntry("e-1", makeDelta());
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

			coordinator.startAutoSync();

			// Advance one tick so it fires once
			await vi.advanceTimersByTimeAsync(10_000);
			const pushCountAfterFirst = (transport.push as ReturnType<typeof vi.fn>).mock.calls.length;

			// Stop auto-sync
			coordinator.stopAutoSync();

			// Advance another tick — should NOT fire again
			await vi.advanceTimersByTimeAsync(10_000);
			expect((transport.push as ReturnType<typeof vi.fn>).mock.calls.length).toBe(
				pushCountAfterFirst,
			);
		});

		it("multiple ticks accumulate calls", async () => {
			const entry = makeEntry("e-1", makeDelta());
			(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

			coordinator.startAutoSync();

			// Advance 3 intervals
			await vi.advanceTimersByTimeAsync(30_000);

			expect((transport.push as ReturnType<typeof vi.fn>).mock.calls.length).toBe(3);
			expect((transport.pull as ReturnType<typeof vi.fn>).mock.calls.length).toBe(3);
		});
	});
});
