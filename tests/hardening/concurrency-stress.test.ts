import type { TableSchema } from "@lakesync/core";
import { HLC, unwrapOrThrow } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createClient, createSharedClock, createTestGateway, TodoSchema } from "./helpers";

describe("Concurrency Stress", () => {
	let gateway: ReturnType<typeof createTestGateway>;
	let clock: ReturnType<typeof createSharedClock>;
	let openDbs: Array<{ close(): Promise<void> }>;

	beforeEach(() => {
		clock = createSharedClock();
		gateway = createTestGateway();
		(gateway as unknown as { hlc: HLC }).hlc = new HLC(clock.clock(0));
		openDbs = [];
	});

	afterEach(async () => {
		for (const db of openDbs) {
			await db.close();
		}
	});

	it("10 clients pushing simultaneously — all deltas round-trip", async () => {
		const N = 10;
		const clients = await Promise.all(
			Array.from({ length: N }, (_, i) =>
				createClient(gateway, {
					clientId: `client-${i}`,
					wallClock: clock.clock(i),
					dbName: `conc-10-${i}`,
				}),
			),
		);
		openDbs.push(...clients.map((c) => c.db));

		// Each client inserts a unique row and pushes
		for (let i = 0; i < N; i++) {
			clock.tick();
			unwrapOrThrow(
				await clients[i]!.tracker.insert("todos", `row-${i}`, {
					title: `Task ${i}`,
					completed: 0,
				}),
			);
		}

		clock.tick();
		await Promise.all(clients.map((c) => c.coordinator.pushToGateway()));

		// New reader client pulls all
		clock.tick();
		const reader = await createClient(gateway, {
			clientId: "reader",
			wallClock: clock.clock(0),
			dbName: "conc-10-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		await reader.coordinator.pullFromGateway();

		const rows = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM todos ORDER BY _rowId"),
		);
		expect(rows).toHaveLength(N);
	});

	it("50 clients pushing simultaneously — all deltas round-trip", async () => {
		const N = 50;
		const clients = await Promise.all(
			Array.from({ length: N }, (_, i) =>
				createClient(gateway, {
					clientId: `client-${i}`,
					wallClock: clock.clock(i),
					dbName: `conc-50-${i}`,
				}),
			),
		);
		openDbs.push(...clients.map((c) => c.db));

		for (let i = 0; i < N; i++) {
			clock.tick();
			unwrapOrThrow(
				await clients[i]!.tracker.insert("todos", `row-${i}`, {
					title: `Task ${i}`,
					completed: 0,
				}),
			);
		}

		clock.tick();
		await Promise.all(clients.map((c) => c.coordinator.pushToGateway()));

		clock.tick();
		const reader = await createClient(gateway, {
			clientId: "reader",
			wallClock: clock.clock(0),
			dbName: "conc-50-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		await reader.coordinator.pullFromGateway();

		const rows = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM todos ORDER BY _rowId"),
		);
		expect(rows).toHaveLength(N);
	});

	it("conflict storm — 10 clients writing same row, LWW resolves correctly", async () => {
		const N = 10;

		// Insert the row first from client-0
		const inserter = await createClient(gateway, {
			clientId: "client-0",
			wallClock: clock.clock(0),
			dbName: "conc-storm-inserter",
		});
		openDbs.push(inserter.db);

		clock.tick();
		unwrapOrThrow(
			await inserter.tracker.insert("todos", "contested-row", {
				title: "Original",
				completed: 0,
			}),
		);
		clock.tick();
		await inserter.coordinator.pushToGateway();

		// N clients all update the same row — highest HLC wins
		const updaters = await Promise.all(
			Array.from({ length: N }, (_, i) =>
				createClient(gateway, {
					clientId: `updater-${i}`,
					wallClock: clock.clock(i * 10), // spread clocks so there's a clear winner
					dbName: `conc-storm-${i}`,
				}),
			),
		);
		openDbs.push(...updaters.map((c) => c.db));

		// Pull the row to each updater
		for (let i = 0; i < N; i++) {
			clock.tick();
			await updaters[i]!.coordinator.pullFromGateway();
		}

		// Each updater writes to the same row
		for (let i = 0; i < N; i++) {
			clock.tick();
			unwrapOrThrow(
				await updaters[i]!.tracker.update("todos", "contested-row", {
					title: `Edit-${i}`,
				}),
			);
		}

		// Push all — last client (highest offset) should win
		clock.tick();
		for (let i = 0; i < N; i++) {
			clock.tick();
			await updaters[i]!.coordinator.pushToGateway();
		}

		// Reader pulls final state
		clock.tick();
		const reader = await createClient(gateway, {
			clientId: "reader",
			wallClock: clock.clock(0),
			dbName: "conc-storm-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		await reader.coordinator.pullFromGateway();

		const rows = unwrapOrThrow(
			await reader.db.query<{ title: string }>(
				"SELECT title FROM todos WHERE _rowId = 'contested-row'",
			),
		);
		expect(rows).toHaveLength(1);
		// The last updater (highest clock offset) should win via LWW
		expect(rows[0]!.title).toBe(`Edit-${N - 1}`);
	});

	it("tiebreak — identical HLC wall, lexicographically highest clientId wins", async () => {
		// First, insert the row via a setup client so both c1 and c2 UPDATE
		const inserter = await createClient(gateway, {
			clientId: "inserter",
			wallClock: clock.clock(0),
			dbName: "tiebreak-inserter",
		});
		openDbs.push(inserter.db);

		clock.tick();
		unwrapOrThrow(
			await inserter.tracker.insert("todos", "tie-row", {
				title: "Original",
				completed: 0,
			}),
		);
		clock.tick();
		await inserter.coordinator.pushToGateway();

		// c1 and c2 with same clock offset but different clientIds
		const c1 = await createClient(gateway, {
			clientId: "aaa-client",
			wallClock: clock.clock(0),
			dbName: "tiebreak-c1",
		});
		const c2 = await createClient(gateway, {
			clientId: "zzz-client",
			wallClock: clock.clock(0),
			dbName: "tiebreak-c2",
		});
		openDbs.push(c1.db, c2.db);

		// Both pull the original row
		clock.tick();
		await c1.coordinator.pullFromGateway();
		clock.tick();
		await c2.coordinator.pullFromGateway();

		// Both update the same row with same clock → HLC tiebreak on clientId
		clock.tick();
		unwrapOrThrow(await c1.tracker.update("todos", "tie-row", { title: "From aaa" }));
		unwrapOrThrow(await c2.tracker.update("todos", "tie-row", { title: "From zzz" }));

		clock.tick();
		await c1.coordinator.pushToGateway();
		clock.tick();
		await c2.coordinator.pushToGateway();

		// Reader sees the winner
		const reader = await createClient(gateway, {
			clientId: "reader",
			wallClock: clock.clock(0),
			dbName: "tiebreak-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		await reader.coordinator.pullFromGateway();

		const rows = unwrapOrThrow(
			await reader.db.query<{ title: string }>("SELECT title FROM todos WHERE _rowId = 'tie-row'"),
		);
		expect(rows).toHaveLength(1);
		// zzz-client > aaa-client lexicographically
		expect(rows[0]!.title).toBe("From zzz");
	});

	it("rapid fire — 1,000 deltas in one push", async () => {
		const c1 = await createClient(gateway, {
			clientId: "rapid-client",
			wallClock: clock.clock(0),
			dbName: "rapid-fire",
		});
		openDbs.push(c1.db);

		const COUNT = 1_000;
		for (let i = 0; i < COUNT; i++) {
			clock.tick();
			unwrapOrThrow(
				await c1.tracker.insert("todos", `rapid-${i}`, {
					title: `Item ${i}`,
					completed: 0,
				}),
			);
		}

		// Push in a loop — pushToGateway peeks 100 entries at a time
		let queueDepth = await c1.coordinator.queueDepth();
		while (queueDepth > 0) {
			clock.tick();
			await c1.coordinator.pushToGateway();
			queueDepth = await c1.coordinator.queueDepth();
		}

		// Pull from another client
		const reader = await createClient(gateway, {
			clientId: "reader",
			wallClock: clock.clock(0),
			dbName: "rapid-fire-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		// Pull in a loop until no more
		// Pull until complete
		let pulled: number;
		do {
			clock.tick();
			pulled = await reader.coordinator.pullFromGateway();
			total += pulled;
		} while (pulled > 0);

		const rows = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM todos"),
		);
		expect(rows).toHaveLength(COUNT);
	});

	it("interleaved push/pull from multiple clients", async () => {
		const N = 5;
		const clients = await Promise.all(
			Array.from({ length: N }, (_, i) =>
				createClient(gateway, {
					clientId: `interleave-${i}`,
					wallClock: clock.clock(i),
					dbName: `interleave-${i}`,
				}),
			),
		);
		openDbs.push(...clients.map((c) => c.db));

		// 3 rounds: each round, all clients insert and push sequentially
		for (let round = 0; round < 3; round++) {
			for (let i = 0; i < N; i++) {
				clock.tick();
				unwrapOrThrow(
					await clients[i]!.tracker.insert("todos", `row-r${round}-c${i}`, {
						title: `R${round}C${i}`,
						completed: 0,
					}),
				);
				clock.tick();
				await clients[i]!.coordinator.pushToGateway();
			}
		}

		// All 15 deltas are in the buffer. Use a fresh reader to verify.
		clock.tick();
		const reader = await createClient(gateway, {
			clientId: "interleave-reader",
			wallClock: clock.clock(0),
			dbName: "interleave-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		let pulled: number;
		do {
			pulled = await reader.coordinator.pullFromGateway();
			clock.tick();
		} while (pulled > 0);

		const rows = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM todos"),
		);
		expect(rows).toHaveLength(15);
	});

	it("multi-table concurrent writes", async () => {
		const NotesSchema: TableSchema = {
			table: "notes",
			columns: [
				{ name: "body", type: "string" },
				{ name: "priority", type: "integer" },
			],
		};

		const schemas = [TodoSchema, NotesSchema];

		const c1 = await createClient(gateway, {
			clientId: "mt-client-1",
			wallClock: clock.clock(0),
			dbName: "multi-table-1",
			schemas,
		});
		const c2 = await createClient(gateway, {
			clientId: "mt-client-2",
			wallClock: clock.clock(1),
			dbName: "multi-table-2",
			schemas,
		});
		openDbs.push(c1.db, c2.db);

		// c1 writes to todos, c2 writes to notes
		clock.tick();
		unwrapOrThrow(await c1.tracker.insert("todos", "todo-1", { title: "Todo 1", completed: 0 }));
		clock.tick();
		await c1.coordinator.pushToGateway();

		clock.tick();
		unwrapOrThrow(await c2.tracker.insert("notes", "note-1", { body: "Note 1", priority: 1 }));
		clock.tick();
		await c2.coordinator.pushToGateway();

		// Use a fresh reader with both schemas to verify both tables have data
		clock.tick();
		const reader = await createClient(gateway, {
			clientId: "mt-reader",
			wallClock: clock.clock(0),
			dbName: "multi-table-reader",
			schemas,
		});
		openDbs.push(reader.db);

		clock.tick();
		await reader.coordinator.pullFromGateway();

		const rTodos = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM todos"),
		);
		const rNotes = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM notes"),
		);

		expect(rTodos).toHaveLength(1);
		expect(rNotes).toHaveLength(1);
	});

	it("100 clients pushing simultaneously — all deltas round-trip", async () => {
		const N = 100;
		const clients = await Promise.all(
			Array.from({ length: N }, (_, i) =>
				createClient(gateway, {
					clientId: `client-${i}`,
					wallClock: clock.clock(i),
					dbName: `conc-100-${i}`,
				}),
			),
		);
		openDbs.push(...clients.map((c) => c.db));

		for (let i = 0; i < N; i++) {
			clock.tick();
			unwrapOrThrow(
				await clients[i]!.tracker.insert("todos", `row-${i}`, {
					title: `Task ${i}`,
					completed: 0,
				}),
			);
		}

		clock.tick();
		await Promise.all(clients.map((c) => c.coordinator.pushToGateway()));

		clock.tick();
		const reader = await createClient(gateway, {
			clientId: "reader",
			wallClock: clock.clock(0),
			dbName: "conc-100-reader",
		});
		openDbs.push(reader.db);

		clock.tick();
		// Pull until complete
		let pulled: number;
		do {
			pulled = await reader.coordinator.pullFromGateway();
			total += pulled;
		} while (pulled > 0);

		const rows = unwrapOrThrow(
			await reader.db.query<{ _rowId: string }>("SELECT _rowId FROM todos"),
		);
		expect(rows).toHaveLength(N);
	});
});
