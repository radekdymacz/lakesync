import {
	LocalDB,
	LocalTransport,
	MemoryQueue,
	registerSchema,
	SyncCoordinator,
} from "@lakesync/client";
import type { TableSchema } from "@lakesync/core";
import { HLC, unwrapOrThrow } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createTestGateway } from "./helpers";

const TodoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/**
 * Creates a shared monotonic clock source for multi-client tests.
 *
 * Each call to `tick()` advances the shared time by `stepMs`.
 * The `clock(offset)` method returns a wallClock function for HLC
 * construction that adds a fixed offset to the shared time.
 */
function createSharedClock(startMs = 1_000_000, stepMs = 100) {
	let now = startMs;
	return {
		/** Advance the shared time by one step and return the new value */
		tick(): number {
			now += stepMs;
			return now;
		},
		/** Return a wallClock function for HLC with a fixed offset */
		clock(offset = 0): () => number {
			return () => now + offset;
		},
		/** Get the current shared time */
		get now() {
			return now;
		},
	};
}

/**
 * Helper to open a LocalDB, register the TodoSchema, and build a
 * SyncCoordinator with an injectable HLC clock and MemoryQueue.
 */
async function createClient(
	gateway: ReturnType<typeof createTestGateway>,
	opts: { clientId: string; wallClock: () => number; dbName: string },
) {
	const db = unwrapOrThrow(await LocalDB.open({ name: opts.dbName, backend: "memory" }));
	unwrapOrThrow(await registerSchema(db, TodoSchema));

	const hlc = new HLC(opts.wallClock);
	const transport = new LocalTransport(gateway);
	const queue = new MemoryQueue();
	const coordinator = new SyncCoordinator(db, transport, {
		hlc,
		queue,
		clientId: opts.clientId,
	});

	return { db, coordinator, tracker: coordinator.tracker };
}

describe("Client <-> Gateway roundtrip", () => {
	let gateway: ReturnType<typeof createTestGateway>;
	let clock: ReturnType<typeof createSharedClock>;
	let openDbs: LocalDB[];

	beforeEach(() => {
		clock = createSharedClock();
		gateway = createTestGateway();
		// Override the gateway's private HLC so it uses the shared clock
		(gateway as unknown as { hlc: HLC }).hlc = new HLC(clock.clock(0));
		openDbs = [];
	});

	afterEach(async () => {
		for (const db of openDbs) {
			await db.close();
		}
	});

	it("insert on client-1 -> push -> pull on client-2 -> row appears", async () => {
		const c1 = await createClient(gateway, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "roundtrip-insert-c1",
		});
		const c2 = await createClient(gateway, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "roundtrip-insert-c2",
		});
		openDbs.push(c1.db, c2.db);

		// client-1: insert a row and push
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-1", { title: "Buy milk", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: pull from gateway
		clock.tick();
		const pulled = await c2.coordinator.pullFromGateway();
		expect(pulled).toBe(1);

		// Verify the row landed in client-2's local DB
		const rows = unwrapOrThrow(
			await c2.db.query<{ _rowId: string; title: string; completed: number }>(
				"SELECT * FROM todos WHERE _rowId = 'row-1'",
			),
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]?.title).toBe("Buy milk");
	});

	it("update on client-1 -> push -> pull on client-2 -> row updated", async () => {
		const c1 = await createClient(gateway, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "roundtrip-update-c1",
		});
		const c2 = await createClient(gateway, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "roundtrip-update-c2",
		});
		openDbs.push(c1.db, c2.db);

		// client-1: insert + push
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-1", { title: "Buy milk", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: pull the insert
		clock.tick();
		await c2.coordinator.pullFromGateway();

		// client-1: update the row + push
		clock.tick();
		unwrapOrThrow(await c1.tracker.update("todos", "row-1", { title: "Buy eggs" }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: pull the update
		clock.tick();
		const pulled = await c2.coordinator.pullFromGateway();
		expect(pulled).toBe(1);

		// Verify update is reflected
		const rows = unwrapOrThrow(
			await c2.db.query<{ title: string }>("SELECT title FROM todos WHERE _rowId = 'row-1'"),
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]?.title).toBe("Buy eggs");
	});

	it("delete on client-1 -> push -> pull on client-2 -> row gone", async () => {
		const c1 = await createClient(gateway, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "roundtrip-delete-c1",
		});
		const c2 = await createClient(gateway, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "roundtrip-delete-c2",
		});
		openDbs.push(c1.db, c2.db);

		// client-1: insert + push
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-1", { title: "Buy milk", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: pull the insert
		clock.tick();
		await c2.coordinator.pullFromGateway();

		// client-1: delete the row + push
		clock.tick();
		unwrapOrThrow(await c1.tracker.delete("todos", "row-1"));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: pull the delete
		clock.tick();
		const pulled = await c2.coordinator.pullFromGateway();
		expect(pulled).toBe(1);

		// Verify the row is gone
		const rows = unwrapOrThrow(
			await c2.db.query<{ _rowId: string }>("SELECT * FROM todos WHERE _rowId = 'row-1'"),
		);
		expect(rows).toHaveLength(0);
	});

	it("conflicting updates — higher HLC wins", async () => {
		// client-2 uses a positive clock offset so its HLCs are deterministically higher
		const c1 = await createClient(gateway, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "roundtrip-conflict-c1",
		});
		const c2 = await createClient(gateway, {
			clientId: "client-2",
			wallClock: clock.clock(1_000),
			dbName: "roundtrip-conflict-c2",
		});
		openDbs.push(c1.db, c2.db);

		// client-1: insert row-1 + push
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-1", { title: "Original", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: pull the insert so it has the row locally
		clock.tick();
		await c2.coordinator.pullFromGateway();

		// client-1: update title + push (lower HLC)
		clock.tick();
		unwrapOrThrow(await c1.tracker.update("todos", "row-1", { title: "Client 1 edit" }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: update title + push (higher HLC — wins)
		clock.tick();
		unwrapOrThrow(await c2.tracker.update("todos", "row-1", { title: "Client 2 edit" }));
		clock.tick();
		await c2.coordinator.pushToGateway();

		// client-1: pull — should receive the resolved delta with client-2's edit
		clock.tick();
		const pulled = await c1.coordinator.pullFromGateway();
		expect(pulled).toBeGreaterThanOrEqual(1);

		// Verify client-1 now has client-2's edit (higher HLC wins)
		const rows = unwrapOrThrow(
			await c1.db.query<{ title: string }>("SELECT title FROM todos WHERE _rowId = 'row-1'"),
		);
		expect(rows).toHaveLength(1);
		expect(rows[0]?.title).toBe("Client 2 edit");
	});

	it("bidirectional sync — both clients converge", async () => {
		const c1 = await createClient(gateway, {
			clientId: "client-1",
			wallClock: clock.clock(0),
			dbName: "roundtrip-bidi-c1",
		});
		const c2 = await createClient(gateway, {
			clientId: "client-2",
			wallClock: clock.clock(0),
			dbName: "roundtrip-bidi-c2",
		});
		openDbs.push(c1.db, c2.db);

		// client-1: insert row-1 + push
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "row-1", { title: "A", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		// client-2: insert row-2 locally (not pushed yet)
		clock.tick();
		unwrapOrThrow(await c2.tracker.insert("todos", "row-2", { title: "B", completed: 0 }));

		// client-2: pull first (gets row-1 while cursor is still at 0), then push row-2
		clock.tick();
		const pulled2 = await c2.coordinator.pullFromGateway();
		expect(pulled2).toBeGreaterThanOrEqual(1);
		clock.tick();
		await c2.coordinator.pushToGateway();

		// client-1: pull -> gets row-2
		clock.tick();
		const pulled1 = await c1.coordinator.pullFromGateway();
		expect(pulled1).toBeGreaterThanOrEqual(1);

		// Both DBs should have both rows
		const rows1 = unwrapOrThrow(
			await c1.db.query<{ _rowId: string; title: string }>(
				"SELECT _rowId, title FROM todos ORDER BY _rowId",
			),
		);
		const rows2 = unwrapOrThrow(
			await c2.db.query<{ _rowId: string; title: string }>(
				"SELECT _rowId, title FROM todos ORDER BY _rowId",
			),
		);

		expect(rows1).toHaveLength(2);
		expect(rows2).toHaveLength(2);

		expect(rows1[0]?.title).toBe("A");
		expect(rows1[1]?.title).toBe("B");
		expect(rows2[0]?.title).toBe("A");
		expect(rows2[1]?.title).toBe("B");
	});
});
