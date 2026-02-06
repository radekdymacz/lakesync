import type { RowDelta, TableSchema } from "@lakesync/core";
import { HLC, LWWResolver, unwrapOrThrow } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { LocalDB } from "../../db/local-db";
import { registerSchema } from "../../db/schema-registry";
import { MemoryQueue } from "../../queue/memory-queue";
import { applyRemoteDeltas } from "../applier";
import { SyncTracker } from "../tracker";

const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

describe("applyRemoteDeltas", () => {
	let db: LocalDB;
	let queue: MemoryQueue;
	let resolver: LWWResolver;

	beforeEach(async () => {
		db = unwrapOrThrow(await LocalDB.open({ name: "test-applier", backend: "memory" }));
		unwrapOrThrow(await registerSchema(db, todoSchema));
		queue = new MemoryQueue();
		resolver = new LWWResolver();
	});

	afterEach(async () => {
		await db.close();
	});

	it("apply INSERT — row appears in SQLite", async () => {
		const remoteDelta: RowDelta = {
			op: "INSERT",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [
				{ column: "title", value: "Remote Todo" },
				{ column: "completed", value: 0 },
			],
			hlc: HLC.encode(2_000_000, 0),
			deltaId: "remote-delta-1",
		};

		const result = await applyRemoteDeltas(db, [remoteDelta], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(1);

		// Verify the row exists in SQLite
		const queryResult = await db.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]?.title).toBe("Remote Todo");
		expect(queryResult.value[0]?.completed).toBe(0);
	});

	it("apply UPDATE — row updated in SQLite", async () => {
		// First insert a row directly
		unwrapOrThrow(
			await db.exec("INSERT INTO todos (_rowId, title, completed) VALUES (?, ?, ?)", [
				"row-1",
				"Original Title",
				0,
			]),
		);

		const remoteDelta: RowDelta = {
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [{ column: "title", value: "Updated Title" }],
			hlc: HLC.encode(3_000_000, 0),
			deltaId: "remote-delta-2",
		};

		const result = await applyRemoteDeltas(db, [remoteDelta], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(1);

		// Verify the row was updated
		const queryResult = await db.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]?.title).toBe("Updated Title");
		expect(queryResult.value[0]?.completed).toBe(0);
	});

	it("apply DELETE — row removed from SQLite", async () => {
		// First insert a row directly
		unwrapOrThrow(
			await db.exec("INSERT INTO todos (_rowId, title, completed) VALUES (?, ?, ?)", [
				"row-1",
				"To Be Deleted",
				1,
			]),
		);

		const remoteDelta: RowDelta = {
			op: "DELETE",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [],
			hlc: HLC.encode(4_000_000, 0),
			deltaId: "remote-delta-3",
		};

		const result = await applyRemoteDeltas(db, [remoteDelta], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(1);

		// Verify the row is gone
		const queryResult = await db.query<Record<string, unknown>>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(0);
	});

	it("conflict: remote wins when it has a higher HLC", async () => {
		// Use SyncTracker to create a local delta in the queue
		const hlc = new HLC(() => 1_000_000);
		const tracker = new SyncTracker(db, queue, hlc, "local-client");

		// Insert a row locally (this pushes an INSERT delta to the queue)
		unwrapOrThrow(await tracker.insert("todos", "row-1", { title: "Local Title", completed: 0 }));

		// Verify local delta is in the queue
		const depthBefore = unwrapOrThrow(await queue.depth());
		expect(depthBefore).toBe(1);

		// Now apply a remote UPDATE for the same row with a HIGHER HLC
		const remoteDelta: RowDelta = {
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [
				{ column: "title", value: "Remote Wins" },
				{ column: "completed", value: 1 },
			],
			hlc: HLC.encode(5_000_000, 0),
			deltaId: "remote-delta-conflict",
		};

		const result = await applyRemoteDeltas(db, [remoteDelta], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(1);

		// The row should have the remote values
		const queryResult = await db.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]?.title).toBe("Remote Wins");
		expect(queryResult.value[0]?.completed).toBe(1);

		// The local queue entry should have been acked (removed)
		const depthAfter = unwrapOrThrow(await queue.depth());
		expect(depthAfter).toBe(0);
	});

	it("conflict: local wins when it has a higher HLC", async () => {
		// Use SyncTracker to create a local delta with a HIGH HLC
		const hlc = new HLC(() => 10_000_000);
		const tracker = new SyncTracker(db, queue, hlc, "local-client");

		// Insert a row locally
		unwrapOrThrow(await tracker.insert("todos", "row-1", { title: "Local Wins", completed: 1 }));

		// Verify local delta is in the queue
		const depthBefore = unwrapOrThrow(await queue.depth());
		expect(depthBefore).toBe(1);

		// Apply a remote UPDATE with a LOWER HLC
		const remoteDelta: RowDelta = {
			op: "UPDATE",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [
				{ column: "title", value: "Remote Loses" },
				{ column: "completed", value: 0 },
			],
			hlc: HLC.encode(1_000_000, 0),
			deltaId: "remote-delta-lose",
		};

		const result = await applyRemoteDeltas(db, [remoteDelta], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		// Remote was skipped, so 0 applied
		expect(result.value).toBe(0);

		// The row should still have the local values
		const queryResult = await db.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]?.title).toBe("Local Wins");
		expect(queryResult.value[0]?.completed).toBe(1);

		// The local queue entry should still be there
		const depthAfter = unwrapOrThrow(await queue.depth());
		expect(depthAfter).toBe(1);
	});

	it("cursor advances after batch", async () => {
		const delta1: RowDelta = {
			op: "INSERT",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [
				{ column: "title", value: "First" },
				{ column: "completed", value: 0 },
			],
			hlc: HLC.encode(1_000_000, 0),
			deltaId: "delta-1",
		};

		const delta2: RowDelta = {
			op: "INSERT",
			table: "todos",
			rowId: "row-2",
			clientId: "remote-client",
			columns: [
				{ column: "title", value: "Second" },
				{ column: "completed", value: 1 },
			],
			hlc: HLC.encode(3_000_000, 0),
			deltaId: "delta-2",
		};

		const result = await applyRemoteDeltas(db, [delta1, delta2], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(2);

		// Check that the cursor was updated to the max HLC
		const cursorResult = await db.query<{ table_name: string; last_synced_hlc: string }>(
			"SELECT * FROM _sync_cursor WHERE table_name = ?",
			["todos"],
		);
		expect(cursorResult.ok).toBe(true);
		if (!cursorResult.ok) return;
		expect(cursorResult.value).toHaveLength(1);

		const expectedHlc = HLC.encode(3_000_000, 0);
		expect(cursorResult.value[0]?.last_synced_hlc).toBe(expectedHlc.toString());
	});

	it("empty batch returns 0", async () => {
		const result = await applyRemoteDeltas(db, [], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(0);
	});

	it("multiple deltas across different tables", async () => {
		// Register a second table
		const notesSchema: TableSchema = {
			table: "notes",
			columns: [{ name: "body", type: "string" }],
		};
		unwrapOrThrow(await registerSchema(db, notesSchema));

		const todoDelta: RowDelta = {
			op: "INSERT",
			table: "todos",
			rowId: "row-1",
			clientId: "remote-client",
			columns: [
				{ column: "title", value: "A todo" },
				{ column: "completed", value: 0 },
			],
			hlc: HLC.encode(1_000_000, 0),
			deltaId: "delta-todo",
		};

		const noteDelta: RowDelta = {
			op: "INSERT",
			table: "notes",
			rowId: "note-1",
			clientId: "remote-client",
			columns: [{ column: "body", value: "A note" }],
			hlc: HLC.encode(2_000_000, 0),
			deltaId: "delta-note",
		};

		const result = await applyRemoteDeltas(db, [todoDelta, noteDelta], resolver, queue);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toBe(2);

		// Verify cursors for both tables
		const todoCursor = await db.query<{ last_synced_hlc: string }>(
			"SELECT last_synced_hlc FROM _sync_cursor WHERE table_name = ?",
			["todos"],
		);
		expect(todoCursor.ok).toBe(true);
		if (!todoCursor.ok) return;
		expect(todoCursor.value[0]?.last_synced_hlc).toBe(HLC.encode(1_000_000, 0).toString());

		const noteCursor = await db.query<{ last_synced_hlc: string }>(
			"SELECT last_synced_hlc FROM _sync_cursor WHERE table_name = ?",
			["notes"],
		);
		expect(noteCursor.ok).toBe(true);
		if (!noteCursor.ok) return;
		expect(noteCursor.value[0]?.last_synced_hlc).toBe(HLC.encode(2_000_000, 0).toString());
	});
});
