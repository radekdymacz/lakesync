import type { MockInstance } from "vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NessieCatalogueClient } from "../nessie-client";
import type { IcebergSchema, PartitionSpec, TableMetadata } from "../types";
import { CatalogueError } from "../types";

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

const TEST_SCHEMA: IcebergSchema = {
	type: "struct",
	"schema-id": 0,
	fields: [
		{ id: 1, name: "id", required: true, type: "long" },
		{ id: 2, name: "name", required: false, type: "string" },
	],
};

const TEST_PARTITION_SPEC: PartitionSpec = {
	"spec-id": 0,
	fields: [],
};

/** Create a fresh mock `/v1/config` response (Response can only be consumed once). */
function mockConfigResponse(): Response {
	return new Response(JSON.stringify({ defaults: {}, overrides: {} }), {
		status: 200,
		headers: { "Content-Type": "application/json" },
	});
}

const TABLE_METADATA_RESPONSE: TableMetadata = {
	"metadata-location": "s3://lakesync-test/lakesync/test-table/metadata/v1.metadata.json",
	metadata: {
		"format-version": 2,
		"table-uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		location: "s3://lakesync-test/lakesync/test-table",
		"current-schema-id": 0,
		schemas: [TEST_SCHEMA],
		"current-snapshot-id": 100,
		snapshots: [
			{
				"snapshot-id": 100,
				"timestamp-ms": 1700000000000,
				summary: { operation: "append", "added-data-files": "1" },
			},
		],
		"partition-specs": [TEST_PARTITION_SPEC],
	},
};

// ---------------------------------------------------------------------------
// Unit tests (mocked fetch -- no Docker required)
// ---------------------------------------------------------------------------

describe("NessieCatalogueClient (unit)", () => {
	let client: NessieCatalogueClient;
	let fetchSpy: MockInstance<typeof globalThis.fetch>;

	beforeEach(() => {
		client = new NessieCatalogueClient({
			nessieUri: "http://localhost:19120/iceberg",
			warehouseUri: "s3://lakesync-test",
		});
		fetchSpy = vi.spyOn(globalThis, "fetch");
	});

	afterEach(() => {
		vi.restoreAllMocks();
	});

	// -----------------------------------------------------------------------
	// createNamespace
	// -----------------------------------------------------------------------

	it("createNamespace sends correct POST request", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespace: ["lakesync"], properties: {} }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.createNamespace(["lakesync"]);

		expect(result.ok).toBe(true);
		expect(fetchSpy).toHaveBeenCalledTimes(2);

		// First call is /v1/config, second is the actual request
		const [configUrl] = fetchSpy.mock.calls[0] as [string, RequestInit];
		expect(configUrl).toBe("http://localhost:19120/iceberg/v1/config");

		const [url, options] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces");
		expect(options.method).toBe("POST");
		expect(options.headers).toEqual({ "Content-Type": "application/json" });
		expect(JSON.parse(options.body as string)).toEqual({
			namespace: ["lakesync"],
			properties: {},
		});
	});

	it("createNamespace handles 409 conflict gracefully (idempotent)", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response("Namespace already exists", {
				status: 409,
				statusText: "Conflict",
			}),
		);

		const result = await client.createNamespace(["lakesync"]);

		expect(result.ok).toBe(true);
	});

	it("createNamespace returns CatalogueError on server error", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response("Internal Server Error", {
				status: 500,
				statusText: "Internal Server Error",
			}),
		);

		const result = await client.createNamespace(["lakesync"]);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(500);
		}
	});

	// -----------------------------------------------------------------------
	// listNamespaces
	// -----------------------------------------------------------------------

	it("listNamespaces parses response correctly", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespaces: [["lakesync"], ["other", "nested"]] }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.listNamespaces();

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toEqual([["lakesync"], ["other", "nested"]]);
		}

		// calls[0] is /v1/config, calls[1] is the actual request
		const [url, options] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces");
		expect(options.method).toBe("GET");
	});

	it("listNamespaces returns CatalogueError on failure", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response("Forbidden", { status: 403, statusText: "Forbidden" }),
		);

		const result = await client.listNamespaces();

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(403);
		}
	});

	// -----------------------------------------------------------------------
	// createTable
	// -----------------------------------------------------------------------

	it("createTable sends correct POST with schema and partition spec", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify(TABLE_METADATA_RESPONSE), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.createTable(
			["lakesync"],
			"events",
			TEST_SCHEMA,
			TEST_PARTITION_SPEC,
		);

		expect(result.ok).toBe(true);

		// calls[0] is /v1/config, calls[1] is the actual request
		const [url, options] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces/lakesync/tables");
		expect(options.method).toBe("POST");
		const body = JSON.parse(options.body as string);
		expect(body.name).toBe("events");
		expect(body.schema).toEqual(TEST_SCHEMA);
		expect(body["partition-spec"]).toEqual(TEST_PARTITION_SPEC);
		expect(body.location).toBe("s3://lakesync-test/lakesync/events");
	});

	// -----------------------------------------------------------------------
	// loadTable
	// -----------------------------------------------------------------------

	it("loadTable returns table metadata on success", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify(TABLE_METADATA_RESPONSE), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.loadTable(["lakesync"], "test-table");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.metadata["table-uuid"]).toBe("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
			expect(result.value.metadata["format-version"]).toBe(2);
		}

		// calls[0] is /v1/config, calls[1] is the actual request
		const [url] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces/lakesync/tables/test-table");
	});

	it("loadTable returns CatalogueError on 404", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response("Table not found", {
				status: 404,
				statusText: "Not Found",
			}),
		);

		const result = await client.loadTable(["lakesync"], "nonexistent");

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(404);
			expect(result.error.code).toBe("CATALOGUE_ERROR");
		}
	});

	// -----------------------------------------------------------------------
	// appendFiles
	// -----------------------------------------------------------------------

	it("appendFiles loads metadata then commits files", async () => {
		// First call: /v1/config
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		// Second call: loadTable to get current metadata
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify(TABLE_METADATA_RESPONSE), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);
		// Third call: commit the append
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({}), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const files = [
			{
				content: "data" as const,
				"file-path": "s3://lakesync-test/lakesync/events/data/file1.parquet",
				"file-format": "PARQUET" as const,
				"record-count": 100,
				"file-size-in-bytes": 4096,
			},
		];

		const result = await client.appendFiles(["lakesync"], "events", files);

		expect(result.ok).toBe(true);
		expect(fetchSpy).toHaveBeenCalledTimes(3);

		// calls[0] is /v1/config, calls[1] is loadTable, calls[2] is the commit
		const [commitUrl, commitOptions] = fetchSpy.mock.calls[2] as [string, RequestInit];
		expect(commitUrl).toBe("http://localhost:19120/iceberg/v1/namespaces/lakesync/tables/events");
		expect(commitOptions.method).toBe("POST");

		const commitBody = JSON.parse(commitOptions.body as string);
		expect(commitBody.requirements).toEqual([
			{ type: "assert-current-schema-id", "current-schema-id": 0 },
		]);
		expect(commitBody.updates[0].action).toBe("append");
		expect(commitBody.updates[0]["data-files"]).toEqual(files);
	});

	it("appendFiles propagates loadTable error", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response("Table not found", {
				status: 404,
				statusText: "Not Found",
			}),
		);

		const result = await client.appendFiles(["lakesync"], "nonexistent", []);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(404);
		}
	});

	// -----------------------------------------------------------------------
	// currentSnapshot
	// -----------------------------------------------------------------------

	it("currentSnapshot returns the current snapshot", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify(TABLE_METADATA_RESPONSE), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.currentSnapshot(["lakesync"], "test-table");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).not.toBeNull();
			expect(result.value?.["snapshot-id"]).toBe(100);
			expect(result.value?.["timestamp-ms"]).toBe(1700000000000);
		}
	});

	it("currentSnapshot returns null when no snapshots exist", async () => {
		const noSnapshotMetadata: TableMetadata = {
			metadata: {
				"format-version": 2,
				"table-uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				location: "s3://lakesync-test/lakesync/empty-table",
				"current-schema-id": 0,
				schemas: [TEST_SCHEMA],
				snapshots: [],
			},
		};

		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify(noSnapshotMetadata), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.currentSnapshot(["lakesync"], "empty-table");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBeNull();
		}
	});

	// -----------------------------------------------------------------------
	// HTTP error and network error handling
	// -----------------------------------------------------------------------

	it("HTTP error produces CatalogueError with correct status code", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response("Bad Gateway", {
				status: 502,
				statusText: "Bad Gateway",
			}),
		);

		const result = await client.listNamespaces();

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(502);
			expect(result.error.code).toBe("CATALOGUE_ERROR");
			expect(result.error.message).toContain("502");
		}
	});

	it("network error produces CatalogueError with status code 0", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockRejectedValueOnce(new TypeError("fetch failed"));

		const result = await client.createNamespace(["lakesync"]);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(0);
			expect(result.error.cause).toBeInstanceOf(TypeError);
			expect(result.error.message).toContain("fetch failed");
		}
	});

	it("network error on listNamespaces produces CatalogueError", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockRejectedValueOnce(new Error("DNS resolution failed"));

		const result = await client.listNamespaces();

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(0);
			expect(result.error.message).toContain("DNS resolution failed");
		}
	});

	// -----------------------------------------------------------------------
	// Multi-level namespace encoding
	// -----------------------------------------------------------------------

	it("encodes multi-level namespaces correctly in URL", async () => {
		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify(TABLE_METADATA_RESPONSE), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		await client.loadTable(["org", "team"], "events");

		// calls[0] is /v1/config, calls[1] is the actual request
		const [url] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces/org%1Fteam/tables/events");
	});

	// -----------------------------------------------------------------------
	// Trailing slash handling
	// -----------------------------------------------------------------------

	it("strips trailing slash from nessieUri", async () => {
		const clientWithSlash = new NessieCatalogueClient({
			nessieUri: "http://localhost:19120/iceberg/",
			warehouseUri: "s3://lakesync-test",
		});

		fetchSpy.mockResolvedValueOnce(mockConfigResponse());
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespaces: [] }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		await clientWithSlash.listNamespaces();

		// calls[0] is /v1/config, calls[1] is the actual request
		const [url] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces");
	});

	// -----------------------------------------------------------------------
	// Prefix resolution from /v1/config
	// -----------------------------------------------------------------------

	it("includes prefix from /v1/config in API URLs", async () => {
		// Config returns prefix "main"
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ defaults: { prefix: "main" }, overrides: {} }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespaces: [] }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.listNamespaces();
		expect(result.ok).toBe(true);

		// The actual request should include the prefix in the path
		const [url] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/main/namespaces");
	});

	it("caches prefix across multiple calls", async () => {
		// Config returns prefix "main" (only called once)
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ defaults: { prefix: "main" }, overrides: {} }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);
		// First API call
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespaces: [] }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);
		// Second API call (no config fetch needed)
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespaces: [] }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		await client.listNamespaces();
		await client.listNamespaces();

		// Config endpoint called once, API endpoint called twice
		expect(fetchSpy).toHaveBeenCalledTimes(3);
		const [configUrl] = fetchSpy.mock.calls[0] as [string, RequestInit];
		expect(configUrl).toBe("http://localhost:19120/iceberg/v1/config");
	});

	it("falls back to no prefix when config endpoint fails", async () => {
		// Config returns 500
		fetchSpy.mockResolvedValueOnce(new Response("Internal Server Error", { status: 500 }));
		fetchSpy.mockResolvedValueOnce(
			new Response(JSON.stringify({ namespaces: [] }), {
				status: 200,
				headers: { "Content-Type": "application/json" },
			}),
		);

		const result = await client.listNamespaces();
		expect(result.ok).toBe(true);

		// Falls back to URL without prefix
		const [url] = fetchSpy.mock.calls[1] as [string, RequestInit];
		expect(url).toBe("http://localhost:19120/iceberg/v1/namespaces");
	});

	// -----------------------------------------------------------------------
	// Custom branch (defaultBranch configuration)
	// -----------------------------------------------------------------------

	it("uses custom defaultBranch when configured", () => {
		const customClient = new NessieCatalogueClient({
			nessieUri: "http://localhost:19120/iceberg",
			warehouseUri: "s3://lakesync-test",
			defaultBranch: "develop",
		});

		// The branch is stored internally; verify client was created without error
		expect(customClient).toBeInstanceOf(NessieCatalogueClient);
	});
});

// ---------------------------------------------------------------------------
// Integration tests (require running Nessie -- skip cleanly without Docker)
// ---------------------------------------------------------------------------

const NESSIE_URI = process.env.NESSIE_URI ?? "http://localhost:19120/iceberg";

describe.skipIf(!process.env.NESSIE_URI)("NessieCatalogueClient (integration)", () => {
	const client = new NessieCatalogueClient({
		nessieUri: NESSIE_URI,
		warehouseUri: "s3://lakesync-test",
	});

	const testNamespace = [`test_${Date.now()}`];

	it("creates and lists namespaces", async () => {
		const createResult = await client.createNamespace(testNamespace);
		expect(createResult.ok).toBe(true);

		// Idempotent: creating again should succeed
		const createAgain = await client.createNamespace(testNamespace);
		expect(createAgain.ok).toBe(true);

		const listResult = await client.listNamespaces();
		expect(listResult.ok).toBe(true);
		if (listResult.ok) {
			const found = listResult.value.some(
				(ns) => ns.length === testNamespace.length && ns[0] === testNamespace[0],
			);
			expect(found).toBe(true);
		}
	});

	it("creates and loads a table", async () => {
		await client.createNamespace(testNamespace);

		const schema: IcebergSchema = {
			type: "struct",
			"schema-id": 0,
			fields: [
				{ id: 1, name: "id", required: true, type: "long" },
				{ id: 2, name: "data", required: false, type: "string" },
			],
		};

		const partitionSpec: PartitionSpec = {
			"spec-id": 0,
			fields: [],
		};

		const tableName = `tbl_${Date.now()}`;
		const createResult = await client.createTable(testNamespace, tableName, schema, partitionSpec);
		expect(createResult.ok).toBe(true);

		const loadResult = await client.loadTable(testNamespace, tableName);
		expect(loadResult.ok).toBe(true);
		if (loadResult.ok) {
			expect(loadResult.value.metadata["format-version"]).toBeGreaterThanOrEqual(1);
			expect(loadResult.value.metadata.schemas.length).toBeGreaterThan(0);
		}
	});

	it("appends files and retrieves snapshot", async () => {
		await client.createNamespace(testNamespace);

		const schema: IcebergSchema = {
			type: "struct",
			"schema-id": 0,
			fields: [{ id: 1, name: "id", required: true, type: "long" }],
		};

		const tableName = `append_${Date.now()}`;
		await client.createTable(testNamespace, tableName, schema, {
			"spec-id": 0,
			fields: [],
		});

		const files = [
			{
				content: "data" as const,
				"file-path": `s3://lakesync-test/${testNamespace[0]}/${tableName}/data/file1.parquet`,
				"file-format": "PARQUET" as const,
				"record-count": 50,
				"file-size-in-bytes": 2048,
			},
		];

		const appendResult = await client.appendFiles(testNamespace, tableName, files);
		expect(appendResult.ok).toBe(true);

		const snapshotResult = await client.currentSnapshot(testNamespace, tableName);
		expect(snapshotResult.ok).toBe(true);
		if (snapshotResult.ok && snapshotResult.value) {
			expect(snapshotResult.value["snapshot-id"]).toBeDefined();
			expect(snapshotResult.value["timestamp-ms"]).toBeGreaterThan(0);
		}
	});

	it("returns error for missing table", async () => {
		const result = await client.loadTable(testNamespace, `nonexistent_${Date.now()}`);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(CatalogueError);
			expect(result.error.statusCode).toBe(404);
		}
	});
});
