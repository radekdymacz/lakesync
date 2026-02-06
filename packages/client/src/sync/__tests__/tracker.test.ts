import type { TableSchema } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { LocalDB } from "../../db/local-db";
import { registerSchema } from "../../db/schema-registry";
import { MemoryQueue } from "../../queue/memory-queue";
import { SyncTracker } from "../tracker";

const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

describe("SyncTracker", () => {
	let db: LocalDB;
	let queue: MemoryQueue;
	let hlc: HLC;
	let tracker: SyncTracker;

	beforeEach(async () => {
		const dbResult = await LocalDB.open({ name: "test-tracker", backend: "memory" });
		expect(dbResult.ok).toBe(true);
		if (!dbResult.ok) throw new Error("Failed to open DB");
		db = dbResult.value;

		const regResult = await registerSchema(db, todoSchema);
		expect(regResult.ok).toBe(true);

		queue = new MemoryQueue();
		hlc = new HLC();
		tracker = new SyncTracker(db, queue, hlc, "test-client");
	});

	afterEach(async () => {
		await db.close();
	});

	it("insert() writes row to SQLite and pushes INSERT delta to queue", async () => {
		const result = await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
		});
		expect(result.ok).toBe(true);

		// Verify the row exists in SQLite
		const queryResult = await db.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(1);
		expect(queryResult.value[0]?.title).toBe("Buy milk");
		expect(queryResult.value[0]?.completed).toBe(0);

		// Verify the delta was queued
		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(1);

		// Verify the delta content
		const peekResult = await queue.peek(1);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(1);
		const delta = peekResult.value[0]?.delta;
		expect(delta?.op).toBe("INSERT");
		expect(delta?.table).toBe("todos");
		expect(delta?.rowId).toBe("row-1");
		expect(delta?.clientId).toBe("test-client");
		expect(delta?.columns).toHaveLength(2);
	});

	it("update() with partial data pushes delta with only changed columns", async () => {
		// First insert a row
		const insertResult = await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
		});
		expect(insertResult.ok).toBe(true);

		// Update only the title
		const updateResult = await tracker.update("todos", "row-1", {
			title: "Buy cheese",
		});
		expect(updateResult.ok).toBe(true);

		// Verify the row was updated in SQLite
		const queryResult = await db.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value[0]?.title).toBe("Buy cheese");
		expect(queryResult.value[0]?.completed).toBe(0);

		// Queue should have 2 entries: INSERT + UPDATE
		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(2);

		// The UPDATE delta should only have the changed column
		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		const updateDelta = peekResult.value[1]?.delta;
		expect(updateDelta?.op).toBe("UPDATE");
		expect(updateDelta?.columns).toHaveLength(1);
		expect(updateDelta?.columns[0]?.column).toBe("title");
		expect(updateDelta?.columns[0]?.value).toBe("Buy cheese");
	});

	it("update() with no change does not push a delta to the queue", async () => {
		// Insert a row
		const insertResult = await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
		});
		expect(insertResult.ok).toBe(true);

		// Update with the same values — no actual change
		const updateResult = await tracker.update("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
		});
		expect(updateResult.ok).toBe(true);

		// Queue should still only have the INSERT delta
		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(1);
	});

	it("delete() removes row from SQLite and pushes DELETE delta to queue", async () => {
		// Insert a row
		const insertResult = await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
		});
		expect(insertResult.ok).toBe(true);

		// Delete the row
		const deleteResult = await tracker.delete("todos", "row-1");
		expect(deleteResult.ok).toBe(true);

		// Verify the row is gone from SQLite
		const queryResult = await db.query<Record<string, unknown>>(
			"SELECT * FROM todos WHERE _rowId = ?",
			["row-1"],
		);
		expect(queryResult.ok).toBe(true);
		if (!queryResult.ok) return;
		expect(queryResult.value).toHaveLength(0);

		// Queue should have 2 entries: INSERT + DELETE
		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(2);

		// The DELETE delta
		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		const deleteDelta = peekResult.value[1]?.delta;
		expect(deleteDelta?.op).toBe("DELETE");
		expect(deleteDelta?.table).toBe("todos");
		expect(deleteDelta?.rowId).toBe("row-1");
		expect(deleteDelta?.columns).toHaveLength(0);
	});

	it("queue depth matches number of tracked changes", async () => {
		// Perform a sequence of operations
		await tracker.insert("todos", "row-1", { title: "Task A", completed: 0 });
		await tracker.insert("todos", "row-2", { title: "Task B", completed: 0 });
		await tracker.update("todos", "row-1", { title: "Task A updated" });
		await tracker.delete("todos", "row-2");

		// 2 inserts + 1 update + 1 delete = 4 deltas
		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(4);

		// Verify the operations in order
		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(4);
		expect(peekResult.value[0]?.delta.op).toBe("INSERT");
		expect(peekResult.value[1]?.delta.op).toBe("INSERT");
		expect(peekResult.value[2]?.delta.op).toBe("UPDATE");
		expect(peekResult.value[3]?.delta.op).toBe("DELETE");
	});

	it("query() passes through to LocalDB", async () => {
		// Insert via tracker
		await tracker.insert("todos", "row-1", { title: "Test", completed: 1 });

		// Query via tracker
		const result = await tracker.query<{ _rowId: string; title: string; completed: number }>(
			"SELECT * FROM todos",
		);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toHaveLength(1);
		expect(result.value[0]?.title).toBe("Test");
		expect(result.value[0]?.completed).toBe(1);
	});

	it("update() returns error for non-existent row", async () => {
		const result = await tracker.update("todos", "nonexistent", { title: "Nope" });
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.code).toBe("ROW_NOT_FOUND");
	});

	it("delete() returns error for non-existent row", async () => {
		const result = await tracker.delete("todos", "nonexistent");
		expect(result.ok).toBe(false);
		if (result.ok) return;
		expect(result.error.code).toBe("ROW_NOT_FOUND");
	});
});
