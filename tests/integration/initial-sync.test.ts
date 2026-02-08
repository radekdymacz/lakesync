import {
	type CheckpointResponse,
	LocalDB,
	LocalTransport,
	MemoryQueue,
	registerSchema,
	SyncCoordinator,
} from "@lakesync/client";
import { CheckpointGenerator } from "@lakesync/compactor";
import type { HLCTimestamp, LakeSyncError, Result, RowDelta, TableSchema } from "@lakesync/core";
import { HLC, Ok, unwrapOrThrow } from "@lakesync/core";
import { decodeSyncResponse } from "@lakesync/proto";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createMockAdapter, createTestGateway } from "./helpers";

const TodoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/**
 * Creates a shared monotonic clock source for multi-client tests.
 */
function createSharedClock(startMs = 1_000_000, stepMs = 100) {
	let now = startMs;
	return {
		tick(): number {
			now += stepMs;
			return now;
		},
		clock(offset = 0): () => number {
			return () => now + offset;
		},
		get now() {
			return now;
		},
	};
}

/**
 * Transport that serves checkpoint data from a mock adapter's R2 storage.
 * Extends LocalTransport with a `checkpoint()` method that reads from
 * the adapter's stored checkpoint files, mimicking what the CF Worker does.
 */
function createCheckpointTransport(
	gateway: ReturnType<typeof createTestGateway>,
	adapter: ReturnType<typeof createMockAdapter>,
	gatewayId: string,
) {
	const base = new LocalTransport(gateway);
	return {
		push: base.push.bind(base),
		pull: base.pull.bind(base),
		async checkpoint(): Promise<Result<CheckpointResponse | null, LakeSyncError>> {
			const manifestKey = `checkpoints/${gatewayId}/manifest.json`;
			const manifestResult = await adapter.getObject(manifestKey);
			if (!manifestResult.ok) {
				return Ok(null);
			}

			const manifest = JSON.parse(new TextDecoder().decode(manifestResult.value)) as {
				snapshotHlc: string;
				chunks: string[];
			};

			const allDeltas: RowDelta[] = [];
			for (const chunkName of manifest.chunks) {
				const chunkKey = `checkpoints/${gatewayId}/${chunkName}`;
				const chunkResult = await adapter.getObject(chunkKey);
				if (!chunkResult.ok) continue;

				const decoded = decodeSyncResponse(chunkResult.value);
				if (decoded.ok) {
					allDeltas.push(...decoded.value.deltas);
				}
			}

			return Ok({
				deltas: allDeltas,
				snapshotHlc: BigInt(manifest.snapshotHlc) as HLCTimestamp,
			});
		},
	};
}

async function createClient(
	gateway: ReturnType<typeof createTestGateway>,
	adapter: ReturnType<typeof createMockAdapter>,
	gatewayId: string,
	opts: { clientId: string; wallClock: () => number; dbName: string },
) {
	const db = unwrapOrThrow(await LocalDB.open({ name: opts.dbName, backend: "memory" }));
	unwrapOrThrow(await registerSchema(db, TodoSchema));

	const hlc = new HLC(opts.wallClock);
	const transport = createCheckpointTransport(gateway, adapter, gatewayId);
	const queue = new MemoryQueue();
	const coordinator = new SyncCoordinator(db, transport, {
		hlc,
		queue,
		clientId: opts.clientId,
	});

	return { db, coordinator, tracker: coordinator.tracker };
}

/** Trigger the private syncOnce() method */
async function triggerSyncOnce(coordinator: SyncCoordinator): Promise<void> {
	await (coordinator as unknown as { syncOnce(): Promise<void> }).syncOnce();
}

describe("Initial sync via checkpoint", () => {
	let clock: ReturnType<typeof createSharedClock>;
	let adapter: ReturnType<typeof createMockAdapter>;
	let gateway: ReturnType<typeof createTestGateway>;
	let openDbs: LocalDB[];
	const gatewayId = "test-gateway";

	beforeEach(() => {
		clock = createSharedClock();
		adapter = createMockAdapter();
		gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: TodoSchema,
		});
		(gateway as unknown as { hlc: HLC }).hlc = new HLC(clock.clock(0));
		openDbs = [];
	});

	afterEach(async () => {
		for (const db of openDbs) {
			await db.close();
		}
	});

	it("push → flush → checkpoint → fresh client gets all data via initial sync", async () => {
		// Phase 1: Push deltas from client-1
		const c1 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "initial-sync-c1",
		});
		openDbs.push(c1.db);

		// Insert 5 rows
		for (let i = 0; i < 5; i++) {
			clock.tick();
			unwrapOrThrow(
				await c1.tracker.insert("todos", `row-${i}`, {
					title: `Todo ${i}`,
					completed: 0,
				}),
			);
		}
		clock.tick();
		await c1.coordinator.pushToGateway();

		// Phase 2: Flush to Parquet
		clock.tick();
		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

		// Phase 3: Generate checkpoint from the flushed Parquet file
		const listResult = await adapter.listObjects("deltas/");
		expect(listResult.ok).toBe(true);
		if (!listResult.ok) return;

		const baseFileKeys = listResult.value.map((o) => o.key);
		const snapshotHlc = HLC.encode(clock.now, 0);
		const checkpointGen = new CheckpointGenerator(adapter, TodoSchema, gatewayId);
		const cpResult = await checkpointGen.generate(baseFileKeys, snapshotHlc);
		expect(cpResult.ok).toBe(true);
		if (!cpResult.ok) return;
		expect(cpResult.value.chunksWritten).toBeGreaterThanOrEqual(1);

		// Phase 4: Fresh client does initial sync via checkpoint
		const c2 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "initial-sync-c2",
		});
		openDbs.push(c2.db);

		clock.tick();
		await triggerSyncOnce(c2.coordinator);

		// Verify client-2 has all 5 rows
		const rows = unwrapOrThrow(
			await c2.db.query<{ _rowId: string; title: string }>(
				"SELECT _rowId, title FROM todos ORDER BY _rowId",
			),
		);
		expect(rows).toHaveLength(5);
		for (let i = 0; i < 5; i++) {
			expect(rows[i]!.title).toBe(`Todo ${i}`);
		}
	});

	it("checkpoint + delta catchup: fresh client gets checkpoint data plus live deltas", async () => {
		// Push initial batch (will go into checkpoint)
		const c1 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "catchup-c1",
		});
		openDbs.push(c1.db);

		for (let i = 0; i < 3; i++) {
			clock.tick();
			unwrapOrThrow(
				await c1.tracker.insert("todos", `old-${i}`, {
					title: `Old Todo ${i}`,
					completed: 0,
				}),
			);
		}
		clock.tick();
		await c1.coordinator.pushToGateway();

		// Flush to Parquet and generate checkpoint
		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

		const listResult = await adapter.listObjects("deltas/");
		if (!listResult.ok) return;
		const baseFileKeys = listResult.value.map((o) => o.key);
		const snapshotHlc = HLC.encode(clock.now, 0);
		const checkpointGen = new CheckpointGenerator(adapter, TodoSchema, gatewayId);
		await checkpointGen.generate(baseFileKeys, snapshotHlc);

		// Push NEW deltas AFTER checkpoint (these are live in the gateway buffer)
		for (let i = 0; i < 2; i++) {
			clock.tick();
			unwrapOrThrow(
				await c1.tracker.insert("todos", `new-${i}`, {
					title: `New Todo ${i}`,
					completed: 0,
				}),
			);
		}
		clock.tick();
		await c1.coordinator.pushToGateway();

		// Fresh client-2 does initial sync: checkpoint + incremental pull
		const c2 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "catchup-c2",
		});
		openDbs.push(c2.db);

		clock.tick();
		await triggerSyncOnce(c2.coordinator);

		// Client-2 should have ALL 5 rows: 3 from checkpoint + 2 from live pull
		const rows = unwrapOrThrow(
			await c2.db.query<{ _rowId: string; title: string }>(
				"SELECT _rowId, title FROM todos ORDER BY _rowId",
			),
		);
		expect(rows).toHaveLength(5);

		const rowIds = rows.map((r) => r._rowId).sort();
		expect(rowIds).toEqual(["new-0", "new-1", "old-0", "old-1", "old-2"]);
	});

	it("no checkpoint available: fresh client falls back to incremental pull", async () => {
		// Push deltas without generating a checkpoint
		const c1 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "no-checkpoint-c1",
		});
		openDbs.push(c1.db);

		for (let i = 0; i < 3; i++) {
			clock.tick();
			unwrapOrThrow(
				await c1.tracker.insert("todos", `row-${i}`, {
					title: `Todo ${i}`,
					completed: 0,
				}),
			);
		}
		clock.tick();
		await c1.coordinator.pushToGateway();

		// Fresh client-2 — NO checkpoint exists, falls back to incremental pull
		const c2 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "no-checkpoint-c2",
		});
		openDbs.push(c2.db);

		clock.tick();
		await triggerSyncOnce(c2.coordinator);

		// Client-2 should still have all rows (from incremental pull)
		const rows = unwrapOrThrow(
			await c2.db.query<{ _rowId: string; title: string }>(
				"SELECT _rowId, title FROM todos ORDER BY _rowId",
			),
		);
		expect(rows).toHaveLength(3);
	});

	it("subsequent syncs after initial sync use incremental pull only", async () => {
		// Setup: push, flush, checkpoint
		const c1 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "subsequent-c1",
		});
		openDbs.push(c1.db);

		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-0", { title: "Initial", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();
		await gateway.flush();

		const listResult = await adapter.listObjects("deltas/");
		if (!listResult.ok) return;
		const snapshotHlc = HLC.encode(clock.now, 0);
		const checkpointGen = new CheckpointGenerator(adapter, TodoSchema, gatewayId);
		await checkpointGen.generate(
			listResult.value.map((o) => o.key),
			snapshotHlc,
		);

		// Client-2: first sync (uses checkpoint)
		const c2 = await createClient(gateway, adapter, gatewayId, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "subsequent-c2",
		});
		openDbs.push(c2.db);

		clock.tick();
		await triggerSyncOnce(c2.coordinator);

		// Verify initial data
		let rows = unwrapOrThrow(await c2.db.query<{ _rowId: string }>("SELECT _rowId FROM todos"));
		expect(rows).toHaveLength(1);

		// Client-1 pushes more data
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-1", { title: "Second", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// Client-2: second sync (incremental pull only, no checkpoint)
		clock.tick();
		await triggerSyncOnce(c2.coordinator);

		rows = unwrapOrThrow(await c2.db.query<{ _rowId: string }>("SELECT _rowId FROM todos"));
		expect(rows).toHaveLength(2);
	});
});
