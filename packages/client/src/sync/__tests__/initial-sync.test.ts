import type { RowDelta } from "@lakesync/core";
import { Err, HLC, LakeSyncError as LSError, Ok } from "@lakesync/core";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { LocalDB } from "../../db/local-db";
import type { SyncQueue } from "../../queue/types";
import { SyncCoordinator } from "../coordinator";
import type { CheckpointTransport, TransportWithCapabilities } from "../transport";

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

function mockQueue(): SyncQueue {
	return {
		push: vi.fn().mockResolvedValue(
			Ok({
				id: "q-1",
				delta: makeDelta(),
				status: "pending",
				createdAt: Date.now(),
				retryCount: 0,
			}),
		),
		peek: vi.fn().mockResolvedValue(Ok([])),
		markSending: vi.fn().mockResolvedValue(Ok(undefined)),
		ack: vi.fn().mockResolvedValue(Ok(undefined)),
		nack: vi.fn().mockResolvedValue(Ok(undefined)),
		depth: vi.fn().mockResolvedValue(Ok(0)),
		clear: vi.fn().mockResolvedValue(Ok(undefined)),
	};
}

function mockTransportWithCheckpoint(
	checkpointResult: Awaited<ReturnType<CheckpointTransport["checkpoint"]>>,
): TransportWithCapabilities {
	return {
		push: vi.fn().mockResolvedValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 1 })),
		pull: vi
			.fn()
			.mockResolvedValue(Ok({ deltas: [], serverHlc: HLC.encode(2_000_000, 0), hasMore: false })),
		checkpoint: vi.fn().mockResolvedValue(checkpointResult),
	};
}

function mockTransportWithoutCheckpoint(): TransportWithCapabilities {
	return {
		push: vi.fn().mockResolvedValue(Ok({ serverHlc: HLC.encode(2_000_000, 0), accepted: 1 })),
		pull: vi
			.fn()
			.mockResolvedValue(Ok({ deltas: [], serverHlc: HLC.encode(2_000_000, 0), hasMore: false })),
	};
}

function mockLocalDB(): LocalDB {
	return {
		exec: vi.fn().mockResolvedValue(Ok(undefined)),
		query: vi.fn().mockResolvedValue(Ok([])),
		name: "test-initial-sync",
		backend: "memory",
		close: vi.fn().mockResolvedValue(undefined),
		save: vi.fn().mockResolvedValue(Ok(undefined)),
		transaction: vi.fn().mockResolvedValue(Ok(undefined)),
	} as unknown as LocalDB;
}

/** Trigger the private syncOnce() method */
async function triggerSyncOnce(coordinator: SyncCoordinator): Promise<void> {
	await (coordinator as unknown as { syncOnce(): Promise<void> }).syncOnce();
}

describe("SyncCoordinator initial sync", () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it("calls checkpoint on first syncOnce when lastSyncedHlc is zero", async () => {
		const snapshotHlc = HLC.encode(5_000_000, 0);
		const checkpointDeltas = [
			makeDelta({ rowId: "r1", hlc: HLC.encode(1_000_000, 0) }),
			makeDelta({ rowId: "r2", hlc: HLC.encode(2_000_000, 0) }),
		];

		const transport = mockTransportWithCheckpoint(Ok({ deltas: checkpointDeltas, snapshotHlc }));
		const queue = mockQueue();
		const db = mockLocalDB();

		const coordinator = new SyncCoordinator(db, transport, {
			queue,
			clientId: "test-client",
		});

		await triggerSyncOnce(coordinator);

		expect(transport.checkpoint).toHaveBeenCalledOnce();
		// Pull should also be called for delta catchup after checkpoint
		expect(transport.pull).toHaveBeenCalled();
	});

	it("falls back to incremental pull when checkpoint returns null", async () => {
		const transport = mockTransportWithCheckpoint(Ok(null));
		const queue = mockQueue();
		const db = mockLocalDB();

		const coordinator = new SyncCoordinator(db, transport, {
			queue,
			clientId: "test-client",
		});

		await triggerSyncOnce(coordinator);

		expect(transport.checkpoint).toHaveBeenCalledOnce();
		expect(transport.pull).toHaveBeenCalled();
	});

	it("falls back to incremental pull when checkpoint fails", async () => {
		const transport = mockTransportWithCheckpoint(
			Err(new LSError("Checkpoint failed", "TRANSPORT_ERROR")),
		);
		const queue = mockQueue();
		const db = mockLocalDB();

		const coordinator = new SyncCoordinator(db, transport, {
			queue,
			clientId: "test-client",
		});

		await triggerSyncOnce(coordinator);

		expect(transport.checkpoint).toHaveBeenCalledOnce();
		expect(transport.pull).toHaveBeenCalled();
	});

	it("skips checkpoint when transport does not support it", async () => {
		const transport = mockTransportWithoutCheckpoint();
		const queue = mockQueue();
		const db = mockLocalDB();

		const coordinator = new SyncCoordinator(db, transport, {
			queue,
			clientId: "test-client",
		});

		await triggerSyncOnce(coordinator);

		// No checkpoint method, so pull should handle everything
		expect(transport.pull).toHaveBeenCalled();
	});

	it("does not call checkpoint on subsequent syncs", async () => {
		const snapshotHlc = HLC.encode(5_000_000, 0);
		const transport = mockTransportWithCheckpoint(Ok({ deltas: [makeDelta()], snapshotHlc }));
		const queue = mockQueue();
		const db = mockLocalDB();

		const coordinator = new SyncCoordinator(db, transport, {
			queue,
			clientId: "test-client",
		});

		// First sync — checkpoint should be called
		await triggerSyncOnce(coordinator);
		expect(transport.checkpoint).toHaveBeenCalledOnce();

		// Reset mock counts
		(transport.checkpoint as ReturnType<typeof vi.fn>).mockClear();

		// Second sync — checkpoint should NOT be called (lastSyncedHlc advanced)
		await triggerSyncOnce(coordinator);
		expect(transport.checkpoint).not.toHaveBeenCalled();
	});

	it("handles empty checkpoint deltas", async () => {
		const snapshotHlc = HLC.encode(5_000_000, 0);
		const transport = mockTransportWithCheckpoint(Ok({ deltas: [], snapshotHlc }));
		const queue = mockQueue();
		const db = mockLocalDB();

		const coordinator = new SyncCoordinator(db, transport, {
			queue,
			clientId: "test-client",
		});

		await triggerSyncOnce(coordinator);

		expect(transport.checkpoint).toHaveBeenCalledOnce();
		// DB exec should not be called for empty deltas (no apply needed)
		expect(db.exec).not.toHaveBeenCalled();
	});
});
