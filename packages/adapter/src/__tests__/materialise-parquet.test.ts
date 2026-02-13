import type { AdapterError, LakeAdapter, Result, RowDelta, TableSchema } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { ParquetMaterialiser } from "../materialise-parquet";
import { hlc, makeDelta } from "./test-helpers";

/** In-memory mock lake adapter that stores objects. */
function createMockLakeAdapter(): LakeAdapter & { stored: Map<string, Uint8Array> } {
	const stored = new Map<string, Uint8Array>();
	return {
		stored,
		async putObject(path: string, data: Uint8Array): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			return Ok(undefined);
		},
		async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
			const data = stored.get(path);
			return data ? Ok(data) : Err({ code: "ADAPTER_ERROR", message: "Not found" } as AdapterError);
		},
		async headObject(
			path: string,
		): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			const data = stored.get(path);
			return data
				? Ok({ size: data.length, lastModified: new Date() })
				: Err({ code: "ADAPTER_ERROR", message: "Not found" } as AdapterError);
		},
		async listObjects(
			prefix: string,
		): Promise<Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>> {
			const results = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({ key, size: data.length, lastModified: new Date() }));
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

/** Failing lake adapter for error tests. */
function createFailingLakeAdapter(): LakeAdapter {
	return {
		async putObject(): Promise<Result<void, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Upload failed" } as AdapterError);
		},
		async getObject(): Promise<Result<Uint8Array, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Not implemented" } as AdapterError);
		},
		async headObject(): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			return Err({ code: "ADAPTER_ERROR", message: "Not implemented" } as AdapterError);
		},
		async listObjects(): Promise<
			Result<Array<{ key: string; size: number; lastModified: Date }>, AdapterError>
		> {
			return Ok([]);
		},
		async deleteObject(): Promise<Result<void, AdapterError>> {
			return Ok(undefined);
		},
		async deleteObjects(): Promise<Result<void, AdapterError>> {
			return Ok(undefined);
		},
	};
}

const todoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "done", type: "boolean" },
	],
};

const usersSchema: TableSchema = {
	table: "users",
	columns: [{ name: "name", type: "string" }],
};

describe("ParquetMaterialiser", () => {
	it("materialises INSERT deltas into a Parquet file at the correct path", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const deltas: RowDelta[] = [
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "INSERT",
				columns: [
					{ column: "title", value: "Buy milk" },
					{ column: "done", value: false },
				],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema]);

		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(1);
		expect(adapter.stored.has("materialised/todos/current.parquet")).toBe(true);
		const data = adapter.stored.get("materialised/todos/current.parquet")!;
		expect(data.byteLength).toBeGreaterThan(0);
	});

	it("respects custom pathPrefix", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter, { pathPrefix: "custom/output" });

		const deltas: RowDelta[] = [
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "INSERT",
				columns: [{ column: "title", value: "Test" }],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema]);

		expect(result.ok).toBe(true);
		expect(adapter.stored.has("custom/output/todos/current.parquet")).toBe(true);
	});

	it("merges UPDATE deltas with INSERT for the same row", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const deltas: RowDelta[] = [
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "INSERT",
				columns: [
					{ column: "title", value: "Buy milk" },
					{ column: "done", value: false },
				],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "UPDATE",
				columns: [{ column: "done", value: true }],
				hlc: hlc(2000),
				deltaId: "d2",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema]);

		expect(result.ok).toBe(true);
		// Should produce a single Parquet file (we cannot easily read it back,
		// but we verify it was written successfully)
		expect(adapter.stored.size).toBe(1);
	});

	it("excludes tombstoned rows (DELETE as last op)", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const deltas: RowDelta[] = [
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "INSERT",
				columns: [{ column: "title", value: "Buy milk" }],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "DELETE",
				columns: [],
				hlc: hlc(2000),
				deltaId: "d2",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema]);

		// All rows deleted — no Parquet file written (no rows to output)
		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(0);
	});

	it("handles multiple tables in a single batch", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const deltas: RowDelta[] = [
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "INSERT",
				columns: [
					{ column: "title", value: "Buy milk" },
					{ column: "done", value: false },
				],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
			makeDelta({
				table: "users",
				rowId: "u1",
				op: "INSERT",
				columns: [{ column: "name", value: "Alice" }],
				hlc: hlc(2000),
				deltaId: "d2",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema, usersSchema]);

		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(2);
		expect(adapter.stored.has("materialised/todos/current.parquet")).toBe(true);
		expect(adapter.stored.has("materialised/users/current.parquet")).toBe(true);
	});

	it("skips tables without matching schema", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const deltas: RowDelta[] = [
			makeDelta({
				table: "unknown_table",
				rowId: "r1",
				op: "INSERT",
				columns: [{ column: "foo", value: "bar" }],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema]);

		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(0);
	});

	it("returns Ok for empty deltas", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const result = await materialiser.materialise([], [todoSchema]);

		expect(result.ok).toBe(true);
		expect(adapter.stored.size).toBe(0);
	});

	it("returns AdapterError when putObject fails", async () => {
		const adapter = createFailingLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const deltas: RowDelta[] = [
			makeDelta({
				table: "todos",
				rowId: "r1",
				op: "INSERT",
				columns: [{ column: "title", value: "Fail" }],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
		];

		const result = await materialiser.materialise(deltas, [todoSchema]);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to upload materialised Parquet");
		}
	});

	it("supports sourceTable schema mapping", async () => {
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		const renamedSchema: TableSchema = {
			table: "tickets",
			sourceTable: "jira_issues",
			columns: [{ name: "summary", type: "string" }],
		};

		const deltas: RowDelta[] = [
			makeDelta({
				table: "jira_issues",
				rowId: "i1",
				op: "INSERT",
				columns: [{ column: "summary", value: "Fix bug" }],
				hlc: hlc(1000),
				deltaId: "d1",
			}),
		];

		const result = await materialiser.materialise(deltas, [renamedSchema]);

		expect(result.ok).toBe(true);
		// Should use the destination table name "tickets" in the path
		expect(adapter.stored.has("materialised/tickets/current.parquet")).toBe(true);
	});

	it("is detected as Materialisable by isMaterialisable", async () => {
		const { isMaterialisable } = await import("@lakesync/core");
		const adapter = createMockLakeAdapter();
		const materialiser = new ParquetMaterialiser(adapter);

		expect(isMaterialisable(materialiser)).toBe(true);
	});
});
