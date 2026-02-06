import { HLC } from "@lakesync/core";
import type { TableSchema } from "@lakesync/core";
import { readParquetToDeltas } from "@lakesync/parquet";
import { describe, expect, it } from "vitest";
import {
	createMockAdapter,
	createTestGateway,
	createTestHLC,
	makeDelta,
} from "./helpers";

/** Shared schema used across all Parquet flush tests. */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
		{ name: "priority", type: "number" },
	],
};

describe("Parquet flush pipeline", () => {
	it("pushes 50 deltas, flushes to Parquet, and stores a .parquet file", async () => {
		const adapter = createMockAdapter();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
		});
		const { hlc, advance } = createTestHLC();

		// Push 50 deltas with varied title values
		const deltas = [];
		for (let i = 0; i < 50; i++) {
			advance(100);
			deltas.push(
				makeDelta({
					table: "todos",
					rowId: `row-${i}`,
					clientId: "client-a",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Todo ${i}` }],
					deltaId: `parquet-delta-${i}`,
				}),
			);
		}

		const pushResult = gateway.handlePush({
			clientId: "client-a",
			deltas,
			lastSeenHlc: HLC.encode(0, 0),
		});
		expect(pushResult.ok).toBe(true);

		// Flush to the adapter
		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

		// Verify exactly one object was stored
		const listResult = await adapter.listObjects("deltas/");
		expect(listResult.ok).toBe(true);
		if (!listResult.ok) return;

		expect(listResult.value).toHaveLength(1);

		const storedKey = listResult.value[0]!.key;
		expect(storedKey).toMatch(/\.parquet$/);

		// Verify key follows the expected pattern: deltas/{date}/{gatewayId}/{hlcRange}.parquet
		const keyPattern =
			/^deltas\/\d{4}-\d{2}-\d{2}\/test-gateway\/\d+-\d+\.parquet$/;
		expect(storedKey).toMatch(keyPattern);
	});

	it("pushes 50 deltas, flushes, reads back, and verifies deep equality", async () => {
		const adapter = createMockAdapter();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
		});
		const { hlc, advance } = createTestHLC();

		// Build and push 50 deltas
		const originalDeltas = [];
		for (let i = 0; i < 50; i++) {
			advance(100);
			originalDeltas.push(
				makeDelta({
					table: "todos",
					rowId: `row-${i}`,
					clientId: "client-a",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Todo ${i}` }],
					deltaId: `roundtrip-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-a",
			deltas: originalDeltas,
			lastSeenHlc: HLC.encode(0, 0),
		});

		await gateway.flush();

		// Retrieve the stored Parquet bytes from the adapter
		const keys = [...adapter.stored.keys()];
		expect(keys).toHaveLength(1);

		const storedData = adapter.stored.get(keys[0]!);
		expect(storedData).toBeDefined();
		if (!storedData) return;

		// Read back using readParquetToDeltas
		const readResult = await readParquetToDeltas(storedData);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value;
		expect(restored).toHaveLength(50);

		// Verify each delta matches the original
		for (let i = 0; i < originalDeltas.length; i++) {
			const original = originalDeltas[i]!;
			const roundtripped = restored[i]!;

			expect(roundtripped.deltaId).toBe(original.deltaId);
			expect(roundtripped.table).toBe(original.table);
			expect(roundtripped.rowId).toBe(original.rowId);
			expect(roundtripped.clientId).toBe(original.clientId);
			expect(roundtripped.op).toBe(original.op);

			// HLC timestamps are branded bigints — compare directly
			expect(roundtripped.hlc).toBe(original.hlc);
		}
	});

	it("produces separate files for multiple flushes", async () => {
		const adapter = createMockAdapter();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
		});
		const { hlc, advance } = createTestHLC();

		// First batch: push 10 deltas and flush
		const batch1 = [];
		for (let i = 0; i < 10; i++) {
			advance(100);
			batch1.push(
				makeDelta({
					table: "todos",
					rowId: `batch1-row-${i}`,
					clientId: "client-a",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Batch 1 Todo ${i}` }],
					deltaId: `batch1-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-a",
			deltas: batch1,
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flush1 = await gateway.flush();
		expect(flush1.ok).toBe(true);

		// Second batch: push 10 more deltas and flush
		const batch2 = [];
		for (let i = 0; i < 10; i++) {
			advance(100);
			batch2.push(
				makeDelta({
					table: "todos",
					rowId: `batch2-row-${i}`,
					clientId: "client-a",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Batch 2 Todo ${i}` }],
					deltaId: `batch2-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-a",
			deltas: batch2,
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flush2 = await gateway.flush();
		expect(flush2.ok).toBe(true);

		// Verify the adapter holds exactly 2 objects, both .parquet
		expect(adapter.stored.size).toBe(2);

		const keys = [...adapter.stored.keys()];
		expect(keys[0]).toMatch(/\.parquet$/);
		expect(keys[1]).toMatch(/\.parquet$/);

		// Keys should differ (different HLC ranges)
		expect(keys[0]).not.toBe(keys[1]);
	});

	it("roundtrips mixed delta types (INSERT, UPDATE, DELETE) through Parquet", async () => {
		const adapter = createMockAdapter();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
		});
		const { hlc, advance } = createTestHLC();

		// INSERT with multiple columns
		advance(100);
		const insertDelta = makeDelta({
			table: "todos",
			rowId: "row-mixed-1",
			clientId: "client-a",
			hlc: hlc.now(),
			op: "INSERT",
			columns: [
				{ column: "title", value: "Buy milk" },
				{ column: "completed", value: false },
				{ column: "priority", value: 3 },
			],
			deltaId: "mixed-insert",
		});

		// UPDATE with partial columns (only title and priority)
		advance(100);
		const updateDelta = makeDelta({
			table: "todos",
			rowId: "row-mixed-2",
			clientId: "client-b",
			hlc: hlc.now(),
			op: "UPDATE",
			columns: [
				{ column: "title", value: "Updated title" },
				{ column: "priority", value: 1 },
			],
			deltaId: "mixed-update",
		});

		// DELETE with empty columns
		advance(100);
		const deleteDelta = makeDelta({
			table: "todos",
			rowId: "row-mixed-3",
			clientId: "client-c",
			hlc: hlc.now(),
			op: "DELETE",
			columns: [],
			deltaId: "mixed-delete",
		});

		gateway.handlePush({
			clientId: "client-a",
			deltas: [insertDelta, updateDelta, deleteDelta],
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

		// Retrieve and deserialise
		const keys = [...adapter.stored.keys()];
		expect(keys).toHaveLength(1);

		const storedData = adapter.stored.get(keys[0]!);
		expect(storedData).toBeDefined();
		if (!storedData) return;

		const readResult = await readParquetToDeltas(storedData);
		expect(readResult.ok).toBe(true);
		if (!readResult.ok) return;

		const restored = readResult.value;
		expect(restored).toHaveLength(3);

		// Verify INSERT delta
		const restoredInsert = restored.find((d) => d.deltaId === "mixed-insert");
		expect(restoredInsert).toBeDefined();
		expect(restoredInsert!.op).toBe("INSERT");
		expect(restoredInsert!.rowId).toBe("row-mixed-1");
		expect(restoredInsert!.clientId).toBe("client-a");
		expect(restoredInsert!.columns).toHaveLength(3);

		const titleCol = restoredInsert!.columns.find(
			(c) => c.column === "title",
		);
		expect(titleCol?.value).toBe("Buy milk");

		const completedCol = restoredInsert!.columns.find(
			(c) => c.column === "completed",
		);
		expect(completedCol?.value).toBe(false);

		const priorityCol = restoredInsert!.columns.find(
			(c) => c.column === "priority",
		);
		expect(priorityCol?.value).toBe(3);

		// Verify UPDATE delta
		const restoredUpdate = restored.find((d) => d.deltaId === "mixed-update");
		expect(restoredUpdate).toBeDefined();
		expect(restoredUpdate!.op).toBe("UPDATE");
		expect(restoredUpdate!.rowId).toBe("row-mixed-2");
		expect(restoredUpdate!.clientId).toBe("client-b");
		expect(restoredUpdate!.columns).toHaveLength(2);

		// Verify DELETE delta
		const restoredDelete = restored.find((d) => d.deltaId === "mixed-delete");
		expect(restoredDelete).toBeDefined();
		expect(restoredDelete!.op).toBe("DELETE");
		expect(restoredDelete!.rowId).toBe("row-mixed-3");
		expect(restoredDelete!.clientId).toBe("client-c");
		expect(restoredDelete!.columns).toHaveLength(0);

		// Verify HLC timestamps are preserved as bigints
		expect(typeof restoredInsert!.hlc).toBe("bigint");
		expect(restoredInsert!.hlc).toBe(insertDelta.hlc);
		expect(restoredUpdate!.hlc).toBe(updateDelta.hlc);
		expect(restoredDelete!.hlc).toBe(deleteDelta.hlc);
	});
});
