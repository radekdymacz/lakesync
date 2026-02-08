import { HLC, Ok } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LocalDB } from "../../db/local-db";
import type { SyncQueue } from "../../queue/types";
import { SyncCoordinator } from "../coordinator";
import type { SyncTransport } from "../transport";

function mockQueue(): SyncQueue {
	return {
		push: vi.fn().mockResolvedValue(Ok(undefined)),
		peek: vi.fn().mockResolvedValue(Ok([])),
		markSending: vi.fn().mockResolvedValue(Ok(undefined)),
		ack: vi.fn().mockResolvedValue(Ok(undefined)),
		nack: vi.fn().mockResolvedValue(Ok(undefined)),
		depth: vi.fn().mockResolvedValue(Ok(0)),
		clear: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as SyncQueue;
}

function mockTransport(): SyncTransport {
	return {
		push: vi.fn().mockResolvedValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 0 })),
		pull: vi
			.fn()
			.mockResolvedValue(Ok({ deltas: [], serverHlc: HLC.encode(2_000_000, 0), hasMore: false })),
	};
}

function mockLocalDB(): LocalDB {
	return {
		exec: vi.fn().mockResolvedValue(Ok(undefined)),
		query: vi.fn().mockResolvedValue(Ok([])),
		name: "test-sync-mode",
		backend: "memory",
		close: vi.fn().mockResolvedValue(undefined),
		save: vi.fn().mockResolvedValue(Ok(undefined)),
		transaction: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as LocalDB;
}

describe("SyncMode", () => {
	let db: LocalDB;
	let transport: SyncTransport;
	let queue: SyncQueue;
	let hlc: HLC;

	beforeEach(() => {
		vi.useFakeTimers();
		db = mockLocalDB();
		transport = mockTransport();
		queue = mockQueue();
		hlc = new HLC(() => 1_000_000);
	});

	afterEach(() => {
		vi.useRealTimers();
	});

	describe("full (default)", () => {
		it("calls both pull and attempts push", async () => {
			const coord = new SyncCoordinator(db, transport, {
				queue,
				hlc,
				clientId: "c1",
			});
			coord.startAutoSync();
			await vi.advanceTimersByTimeAsync(10_000);
			coord.stopAutoSync();

			expect(transport.pull).toHaveBeenCalled();
			// Push peeks the queue (empty → no transport.push call), so verify peek was called
			expect(queue.peek).toHaveBeenCalled();
		});
	});

	describe("pushOnly", () => {
		it("attempts push but not pull", async () => {
			const coord = new SyncCoordinator(db, transport, {
				queue,
				hlc,
				clientId: "c1",
				syncMode: "pushOnly",
			});
			coord.startAutoSync();
			await vi.advanceTimersByTimeAsync(10_000);
			coord.stopAutoSync();

			// Push peeks the queue to check for pending entries
			expect(queue.peek).toHaveBeenCalled();
			expect(transport.pull).not.toHaveBeenCalled();
		});

		it("skips initial sync even on first tick", async () => {
			const checkpoint = vi.fn().mockResolvedValue(Ok(null));
			const transportWithCheckpoint = { ...mockTransport(), checkpoint };

			const coord = new SyncCoordinator(db, transportWithCheckpoint, {
				queue,
				hlc,
				clientId: "c1",
				syncMode: "pushOnly",
			});
			coord.startAutoSync();
			await vi.advanceTimersByTimeAsync(10_000);
			coord.stopAutoSync();

			expect(checkpoint).not.toHaveBeenCalled();
			expect(transportWithCheckpoint.pull).not.toHaveBeenCalled();
		});
	});

	describe("pullOnly", () => {
		it("calls pull but not push", async () => {
			const coord = new SyncCoordinator(db, transport, {
				queue,
				hlc,
				clientId: "c1",
				syncMode: "pullOnly",
			});
			coord.startAutoSync();
			await vi.advanceTimersByTimeAsync(10_000);
			coord.stopAutoSync();

			expect(transport.pull).toHaveBeenCalled();
			expect(transport.push).not.toHaveBeenCalled();
		});

		it("performs initial sync on first tick when checkpoint available", async () => {
			const checkpoint = vi
				.fn()
				.mockResolvedValue(Ok({ deltas: [], snapshotHlc: HLC.encode(1_000_000, 0) }));
			const transportWithCheckpoint = { ...mockTransport(), checkpoint };

			const coord = new SyncCoordinator(db, transportWithCheckpoint, {
				queue,
				hlc,
				clientId: "c1",
				syncMode: "pullOnly",
			});
			coord.startAutoSync();
			await vi.advanceTimersByTimeAsync(10_000);
			coord.stopAutoSync();

			expect(checkpoint).toHaveBeenCalled();
			expect(transportWithCheckpoint.pull).toHaveBeenCalled();
			expect(transportWithCheckpoint.push).not.toHaveBeenCalled();
		});
	});

	describe("direct methods ignore syncMode", () => {
		it("pushToGateway works in pullOnly mode", async () => {
			const coord = new SyncCoordinator(db, transport, {
				queue,
				hlc,
				clientId: "c1",
				syncMode: "pullOnly",
			});

			// pushToGateway is callable directly regardless of mode
			await coord.pushToGateway();
			// No entries in queue so push is a no-op, but it shouldn't throw
		});

		it("pullFromGateway works in pushOnly mode", async () => {
			const coord = new SyncCoordinator(db, transport, {
				queue,
				hlc,
				clientId: "c1",
				syncMode: "pushOnly",
			});

			const applied = await coord.pullFromGateway();
			expect(applied).toBe(0);
			expect(transport.pull).toHaveBeenCalled();
		});
	});
});
