import type { LakeAdapter } from "@lakesync/adapter";
import { CatalogueError } from "@lakesync/catalogue";
import type { NessieCatalogueClient } from "@lakesync/catalogue";
import { AdapterError, Err, HLC, Ok } from "@lakesync/core";
import type { DeltaOp, HLCTimestamp, Result, RowDelta, TableSchema } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { SyncGateway } from "../gateway";
import type { GatewayConfig } from "../types";

/** Helper to build a RowDelta with sensible defaults */
function makeDelta(opts: Partial<RowDelta> & { hlc: HLCTimestamp }): RowDelta {
	return {
		op: (opts.op ?? "UPDATE") as DeltaOp,
		table: opts.table ?? "todos",
		rowId: opts.rowId ?? "row-1",
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [{ column: "title", value: "Test" }],
		hlc: opts.hlc,
		deltaId: opts.deltaId ?? `delta-${Math.random().toString(36).slice(2)}`,
	};
}

/** Simple in-memory mock adapter */
function createMockAdapter(): LakeAdapter & {
	stored: Map<string, Uint8Array>;
} {
	const stored = new Map<string, Uint8Array>();
	return {
		stored,
		async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			return Ok(undefined);
		},
		async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
			const data = stored.get(path);
			return data ? Ok(data) : Err(new AdapterError("Not found"));
		},
		async headObject(
			path: string,
		): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			const data = stored.get(path);
			return data
				? Ok({ size: data.length, lastModified: new Date() })
				: Err(new AdapterError("Not found"));
		},
		async listObjects(
			prefix: string,
		): Promise<Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>> {
			const results = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({
					key,
					size: data.length,
					lastModified: new Date(),
				}));
			return Ok(results);
		},
		async deleteObject(path: string): Promise<Result<void, AdapterError>> {
			stored.delete(path);
			return Ok(undefined);
		},
		async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
			for (const p of paths) stored.delete(p);
			return Ok(undefined);
		},
	};
}

/**
 * Creates a mock NessieCatalogueClient with spies on all public methods.
 * All methods return Ok by default.
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
					"table-uuid": "mock-uuid",
					location: "s3://test/lakesync/todos",
					"current-schema-id": 0,
					schemas: [],
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

const todoSchema: TableSchema = {
	table: "todos",
	columns: [{ name: "title", type: "string" }],
};

describe("SyncGateway catalogue flush", () => {
	const hlcLow = HLC.encode(1_000_000, 0);

	it("flush with catalogue calls createNamespace, createTable, and appendFiles", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();
		const config: GatewayConfig = {
			gatewayId: "gw-cat-1",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue,
		};
		const gw = new SyncGateway(config, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-cat-1" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		expect(result.ok).toBe(true);
		expect(catalogue.createNamespace).toHaveBeenCalledTimes(1);
		expect(catalogue.createNamespace).toHaveBeenCalledWith(["lakesync"]);
		expect(catalogue.createTable).toHaveBeenCalledTimes(1);
		expect(catalogue.createTable).toHaveBeenCalledWith(
			["lakesync"],
			"todos",
			expect.objectContaining({ type: "struct" }),
			expect.objectContaining({ "spec-id": 0 }),
		);
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
		expect(catalogue.appendFiles).toHaveBeenCalledWith(["lakesync"], "todos", [
			expect.objectContaining({
				content: "data",
				"file-format": "PARQUET",
				"record-count": 1,
			}),
		]);
	});

	it("flush without catalogue does not call any catalogue methods", async () => {
		const adapter = createMockAdapter();
		const config: GatewayConfig = {
			gatewayId: "gw-no-cat",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "json",
		};
		const gw = new SyncGateway(config, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-nocat-1" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		expect(result.ok).toBe(true);
		// No catalogue configured, so adapter stored the JSON but no catalogue calls
		expect(adapter.stored.size).toBe(1);
	});

	it("catalogue error on appendFiles still returns Ok (best-effort)", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();
		catalogue.appendFiles.mockResolvedValue(Err(new CatalogueError("Server error", 500)));

		const config: GatewayConfig = {
			gatewayId: "gw-cat-err",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue,
		};
		const gw = new SyncGateway(config, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-cat-err" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		// Flush succeeds even though catalogue append failed
		expect(result.ok).toBe(true);
		// Data was written to the adapter
		expect(adapter.stored.size).toBe(1);
		// appendFiles was called
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
	});

	it("409 conflict on appendFiles triggers a single retry", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();

		// First call returns 409, second call succeeds
		catalogue.appendFiles
			.mockResolvedValueOnce(Err(new CatalogueError("Conflict", 409)))
			.mockResolvedValueOnce(Ok(undefined));

		const config: GatewayConfig = {
			gatewayId: "gw-cat-409",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue,
		};
		const gw = new SyncGateway(config, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-cat-409" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		expect(result.ok).toBe(true);
		// appendFiles called twice: initial attempt + one retry
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(2);
	});

	it("createTable 409 is treated as success and appendFiles proceeds", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();

		// createTable returns 409 (table already exists)
		catalogue.createTable.mockResolvedValue(Err(new CatalogueError("Table already exists", 409)));

		const config: GatewayConfig = {
			gatewayId: "gw-cat-exists",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue,
		};
		const gw = new SyncGateway(config, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-cat-exists" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		expect(result.ok).toBe(true);
		// appendFiles should still be called despite createTable returning 409
		expect(catalogue.appendFiles).toHaveBeenCalledTimes(1);
	});

	it("createTable non-409 error aborts catalogue commit but flush still succeeds", async () => {
		const adapter = createMockAdapter();
		const catalogue = createMockCatalogue();

		// createTable returns a non-409 error
		catalogue.createTable.mockResolvedValue(Err(new CatalogueError("Internal server error", 500)));

		const config: GatewayConfig = {
			gatewayId: "gw-cat-500",
			maxBufferBytes: 1_048_576,
			maxBufferAgeMs: 30_000,
			flushFormat: "parquet",
			tableSchema: todoSchema,
			catalogue,
		};
		const gw = new SyncGateway(config, adapter);

		const delta = makeDelta({ hlc: hlcLow, deltaId: "delta-cat-500" });
		gw.handlePush({
			clientId: "client-a",
			deltas: [delta],
			lastSeenHlc: hlcLow,
		});

		const result = await gw.flush();

		// Flush still succeeds — catalogue is best-effort
		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(1);
		// appendFiles should NOT have been called since createTable failed with non-409
		expect(catalogue.appendFiles).not.toHaveBeenCalled();
	});
});
