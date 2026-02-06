import { HLC, Ok } from "@lakesync/core";
import type { TableSchema } from "@lakesync/core";
import type { NessieCatalogueClient } from "@lakesync/catalogue";
import { readParquetToDeltas } from "@lakesync/parquet";
import { describe, expect, it, vi } from "vitest";
import {
	createMockAdapter,
	createTestGateway,
	createTestHLC,
	makeDelta,
} from "./helpers";

/** Shared schema used across all catalogue integration tests. */
const todoSchema: TableSchema = {
	table: "todos",
	columns: [{ name: "title", type: "string" }],
};

/**
 * Create a mock catalogue client that records calls without
 * requiring a live Nessie instance.
 */
function createMockCatalogue(): NessieCatalogueClient & {
	createNamespace: ReturnType<typeof vi.fn>;
	createTable: ReturnType<typeof vi.fn>;
	loadTable: ReturnType<typeof vi.fn>;
	appendFiles: ReturnType<typeof vi.fn>;
	currentSnapshot: ReturnType<typeof vi.fn>;
	listNamespaces: ReturnType<typeof vi.fn>;
} {
	return {
		createNamespace: vi.fn().mockResolvedValue(Ok(undefined)),
		createTable: vi.fn().mockResolvedValue(Ok(undefined)),
		loadTable: vi.fn().mockResolvedValue(
			Ok({
				metadata: {
					"format-version": 2,
					"table-uuid": "test",
					location: "s3://test",
					schemas: [],
					"current-schema-id": 0,
				},
			}),
		),
		appendFiles: vi.fn().mockResolvedValue(Ok(undefined)),
		currentSnapshot: vi.fn().mockResolvedValue(Ok(null)),
		listNamespaces: vi.fn().mockResolvedValue(Ok([])),
	} as unknown as NessieCatalogueClient & {
		createNamespace: ReturnType<typeof vi.fn>;
		createTable: ReturnType<typeof vi.fn>;
		loadTable: ReturnType<typeof vi.fn>;
		appendFiles: ReturnType<typeof vi.fn>;
		currentSnapshot: ReturnType<typeof vi.fn>;
		listNamespaces: ReturnType<typeof vi.fn>;
	};
}

// ---------------------------------------------------------------------------
// Mock-based tests — always run, no Docker required
// ---------------------------------------------------------------------------
describe("Iceberg catalogue integration (mock)", () => {
	it("push -> Parquet flush -> catalogue commit flow", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue: catalogue as unknown as NessieCatalogueClient,
		});
		const { hlc, advance } = createTestHLC();

		// Push 20 deltas
		const deltas = [];
		for (let i = 0; i < 20; i++) {
			advance(100);
			deltas.push(
				makeDelta({
					table: "todos",
					rowId: `row-${i}`,
					clientId: "client-a",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Todo ${i}` }],
					deltaId: `catalogue-delta-${i}`,
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

		// Verify adapter has a .parquet file
		const keys = [...adapter.stored.keys()];
		expect(keys).toHaveLength(1);
		expect(keys[0]).toMatch(/\.parquet$/);

		// Verify catalogue createNamespace was called
		expect(catalogue.createNamespace).toHaveBeenCalledTimes(1);
		expect(catalogue.createNamespace).toHaveBeenCalledWith(["lakesync"]);

		// Verify catalogue createTable was called
		expect(catalogue.createTable).toHaveBeenCalledTimes(1);
		const createTableArgs = catalogue.createTable.mock.calls[0];
		expect(createTableArgs[0]).toEqual(["lakesync"]);
		expect(createTableArgs[1]).toBe("todos");

		// Verify catalogue appendFiles was called with correct record count
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
		const appendArgs = catalogue.appendFiles.mock.calls[0];
		expect(appendArgs[0]).toEqual(["lakesync"]);
		expect(appendArgs[1]).toBe("todos");

		const dataFiles = appendArgs[2] as Array<{
			"record-count": number;
			"file-format": string;
		}>;
		expect(dataFiles).toHaveLength(1);
		expect(dataFiles[0]!["record-count"]).toBe(20);
		expect(dataFiles[0]!["file-format"]).toBe("PARQUET");
	});

	it("multiple flushes -> multiple appendFiles calls", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue: catalogue as unknown as NessieCatalogueClient,
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

		// Verify appendFiles was called twice
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(2);

		// Verify adapter has 2 .parquet objects
		expect(adapter.stored.size).toBe(2);
		const keys = [...adapter.stored.keys()];
		expect(keys[0]).toMatch(/\.parquet$/);
		expect(keys[1]).toMatch(/\.parquet$/);
		expect(keys[0]).not.toBe(keys[1]);

		// Verify record counts in each appendFiles call
		const firstAppendFiles = catalogue.appendFiles.mock.calls[0][2] as Array<{
			"record-count": number;
		}>;
		expect(firstAppendFiles[0]!["record-count"]).toBe(10);

		const secondAppendFiles = catalogue.appendFiles.mock.calls[1][2] as Array<{
			"record-count": number;
		}>;
		expect(secondAppendFiles[0]!["record-count"]).toBe(10);
	});

	it("Parquet roundtrip still works with catalogue enabled", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue: catalogue as unknown as NessieCatalogueClient,
		});
		const { hlc, advance } = createTestHLC();

		// Push 15 deltas
		const originalDeltas = [];
		for (let i = 0; i < 15; i++) {
			advance(100);
			originalDeltas.push(
				makeDelta({
					table: "todos",
					rowId: `row-${i}`,
					clientId: "client-a",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Roundtrip Todo ${i}` }],
					deltaId: `roundtrip-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-a",
			deltas: originalDeltas,
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

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
		expect(restored).toHaveLength(15);

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

		// Confirm catalogue was still invoked despite the roundtrip focus
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
	});
});

// ---------------------------------------------------------------------------
// Docker-dependent tests — skipped without NESSIE_URI
// ---------------------------------------------------------------------------
const NESSIE_URI = process.env.NESSIE_URI;

describe.skipIf(!NESSIE_URI)("Iceberg catalogue integration (Docker)", () => {
	it("push -> flush -> Nessie snapshot exists", async () => {
		const { NessieCatalogueClient: RealNessieCatalogueClient } = await import(
			"@lakesync/catalogue"
		);

		const adapter = createMockAdapter();
		const catalogue = new RealNessieCatalogueClient({
			nessieUri: NESSIE_URI!,
			warehouseUri: "s3://lakesync-test",
		});
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue,
		});
		const { hlc, advance } = createTestHLC();

		// Push deltas
		const deltas = [];
		for (let i = 0; i < 10; i++) {
			advance(100);
			deltas.push(
				makeDelta({
					table: "todos",
					rowId: `docker-row-${i}`,
					clientId: "client-docker",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Docker Todo ${i}` }],
					deltaId: `docker-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-docker",
			deltas,
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flushResult = await gateway.flush();
		expect(flushResult.ok).toBe(true);

		// Verify a snapshot was created via the catalogue
		const snapshotResult = await catalogue.currentSnapshot(
			["lakesync"],
			"todos",
		);
		expect(snapshotResult.ok).toBe(true);
		if (!snapshotResult.ok) return;

		expect(snapshotResult.value).not.toBeNull();
	});

	it("multiple flushes -> snapshot chain grows", async () => {
		const { NessieCatalogueClient: RealNessieCatalogueClient } = await import(
			"@lakesync/catalogue"
		);

		const tableName = `todos_chain_${Date.now()}`;
		const chainSchema: TableSchema = {
			table: tableName,
			columns: [{ name: "title", type: "string" }],
		};

		const adapter = createMockAdapter();
		const catalogue = new RealNessieCatalogueClient({
			nessieUri: NESSIE_URI!,
			warehouseUri: "s3://lakesync-test",
		});
		const gateway = createTestGateway(adapter, {
			flushFormat: "parquet",
			tableSchema: chainSchema,
			catalogue,
		});
		const { hlc, advance } = createTestHLC();

		// First flush
		const batch1 = [];
		for (let i = 0; i < 5; i++) {
			advance(100);
			batch1.push(
				makeDelta({
					table: tableName,
					rowId: `chain-row-1-${i}`,
					clientId: "client-chain",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Chain 1 Todo ${i}` }],
					deltaId: `chain1-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-chain",
			deltas: batch1,
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flush1 = await gateway.flush();
		expect(flush1.ok).toBe(true);

		// Capture first snapshot
		const snap1Result = await catalogue.currentSnapshot(
			["lakesync"],
			tableName,
		);
		expect(snap1Result.ok).toBe(true);
		if (!snap1Result.ok) return;

		const snap1Id = snap1Result.value?.["snapshot-id"];
		expect(snap1Id).toBeDefined();

		// Second flush
		const batch2 = [];
		for (let i = 0; i < 5; i++) {
			advance(100);
			batch2.push(
				makeDelta({
					table: tableName,
					rowId: `chain-row-2-${i}`,
					clientId: "client-chain",
					hlc: hlc.now(),
					op: "INSERT",
					columns: [{ column: "title", value: `Chain 2 Todo ${i}` }],
					deltaId: `chain2-delta-${i}`,
				}),
			);
		}

		gateway.handlePush({
			clientId: "client-chain",
			deltas: batch2,
			lastSeenHlc: HLC.encode(0, 0),
		});

		const flush2 = await gateway.flush();
		expect(flush2.ok).toBe(true);

		// Capture second snapshot
		const snap2Result = await catalogue.currentSnapshot(
			["lakesync"],
			tableName,
		);
		expect(snap2Result.ok).toBe(true);
		if (!snap2Result.ok) return;

		const snap2Id = snap2Result.value?.["snapshot-id"];
		expect(snap2Id).toBeDefined();

		// Snapshot IDs should differ — the chain has grown
		expect(snap2Id).not.toBe(snap1Id);
	});
});
