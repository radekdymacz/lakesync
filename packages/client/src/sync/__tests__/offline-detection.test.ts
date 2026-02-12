import type { RowDelta } from "@lakesync/core";
import { HLC, Ok } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LocalDB } from "../../db/local-db";
import type { QueueEntry, SyncQueue } from "../../queue/types";
import { SyncCoordinator } from "../coordinator";
import type { SyncTransport } from "../transport";

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

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

function makeEntry(id: string, delta: RowDelta, retryCount = 0): QueueEntry {
	return { id, delta, status: "pending", createdAt: Date.now(), retryCount };
}

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

function mockLocalDB(): LocalDB {
	return {
		exec: vi.fn().mockResolvedValue(Ok(undefined)),
		query: vi.fn().mockResolvedValue(Ok([])),
		name: "test-offline",
		backend: "memory",
		close: vi.fn().mockResolvedValue(undefined),
		save: vi.fn().mockResolvedValue(Ok(undefined)),
		transaction: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as LocalDB;
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

describe("SyncCoordinator offline detection", () => {
	let db: LocalDB;
	let transport: SyncTransport;
	let queue: SyncQueue;
	let hlc: HLC;
	let coordinator: SyncCoordinator;
	let windowListeners: Map<string, Set<EventListener>>;

	beforeEach(() => {
		vi.useFakeTimers();
		db = mockLocalDB();
		transport = mockTransport();
		queue = mockQueue();
		hlc = new HLC(() => 1_000_000);

		// Set up window event listener tracking
		windowListeners = new Map();

		vi.stubGlobal("window", {
			addEventListener: vi.fn((event: string, handler: EventListener) => {
				if (!windowListeners.has(event)) {
					windowListeners.set(event, new Set());
				}
				windowListeners.get(event)!.add(handler);
			}),
			removeEventListener: vi.fn((event: string, handler: EventListener) => {
				windowListeners.get(event)?.delete(handler);
			}),
		});

		vi.stubGlobal("navigator", { onLine: true });

		coordinator = new SyncCoordinator(db, transport, {
			queue,
			hlc,
			clientId: "test-client",
		});
	});

	afterEach(() => {
		coordinator.stopAutoSync();
		vi.useRealTimers();
		vi.unstubAllGlobals();
	});

	it("isOnline defaults to true", () => {
		expect(coordinator.isOnline).toBe(true);
	});

	it("reads navigator.onLine on startAutoSync", () => {
		vi.stubGlobal("navigator", { onLine: false });

		const coord = new SyncCoordinator(db, transport, {
			queue,
			hlc,
			clientId: "test-client-2",
		});
		coord.startAutoSync();

		expect(coord.isOnline).toBe(false);

		coord.stopAutoSync();
	});

	it("skips sync when offline", async () => {
		coordinator.startAutoSync();

		// Simulate offline
		const offlineHandlers = windowListeners.get("offline");
		expect(offlineHandlers).toBeDefined();
		for (const handler of offlineHandlers!) {
			handler(new Event("offline"));
		}

		expect(coordinator.isOnline).toBe(false);

		// Advance timer to trigger auto-sync
		await vi.advanceTimersByTimeAsync(10_000);

		// Transport should not have been called because we're offline
		expect(transport.push).not.toHaveBeenCalled();
		expect(transport.pull).not.toHaveBeenCalled();
	});

	it("triggers immediate sync on reconnect", async () => {
		const entry = makeEntry("e-1", makeDelta());
		(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

		coordinator.startAutoSync();

		// Go offline
		for (const handler of windowListeners.get("offline")!) {
			handler(new Event("offline"));
		}
		expect(coordinator.isOnline).toBe(false);

		// Come back online
		for (const handler of windowListeners.get("online")!) {
			handler(new Event("online"));
		}
		expect(coordinator.isOnline).toBe(true);

		// Allow the immediate sync triggered by online event to complete
		await vi.advanceTimersByTimeAsync(0);

		expect(transport.pull).toHaveBeenCalled();
	});

	it("resumes periodic sync after coming back online", async () => {
		const entry = makeEntry("e-1", makeDelta());
		(queue.peek as ReturnType<typeof vi.fn>).mockResolvedValue(Ok([entry]));

		coordinator.startAutoSync();

		// Go offline
		for (const handler of windowListeners.get("offline")!) {
			handler(new Event("offline"));
		}

		// Advance past several intervals while offline — should not sync
		await vi.advanceTimersByTimeAsync(30_000);
		expect(transport.push).not.toHaveBeenCalled();

		// Come back online
		for (const handler of windowListeners.get("online")!) {
			handler(new Event("online"));
		}

		// Allow the immediate sync triggered by online event to complete
		await vi.advanceTimersByTimeAsync(0);
		const pullCountAfterReconnect = (transport.pull as ReturnType<typeof vi.fn>).mock.calls.length;
		expect(pullCountAfterReconnect).toBeGreaterThan(0);

		// Next interval should sync normally
		await vi.advanceTimersByTimeAsync(10_000);
		expect((transport.pull as ReturnType<typeof vi.fn>).mock.calls.length).toBeGreaterThan(
			pullCountAfterReconnect,
		);
	});

	it("removes listeners on stopAutoSync", () => {
		coordinator.startAutoSync();

		expect(windowListeners.get("online")?.size).toBe(1);
		expect(windowListeners.get("offline")?.size).toBe(1);

		coordinator.stopAutoSync();

		expect(windowListeners.get("online")?.size).toBe(0);
		expect(windowListeners.get("offline")?.size).toBe(0);
	});

	it("works safely in Node/SSR environment without window", () => {
		vi.unstubAllGlobals();

		const coord = new SyncCoordinator(db, transport, {
			queue,
			hlc,
			clientId: "node-client",
		});

		// Should not throw
		coord.startAutoSync();
		expect(coord.isOnline).toBe(true);
		coord.stopAutoSync();
	});
});
