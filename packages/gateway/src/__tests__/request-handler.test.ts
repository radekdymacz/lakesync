import type {
	Action,
	ActionHandler,
	HLCTimestamp,
	RowDelta,
	SyncRulesConfig,
} from "@lakesync/core";
import { bigintReplacer, HLC, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { MemoryConfigStore } from "../config-store";
import { SyncGateway } from "../gateway";
import {
	handleActionRequest,
	handleFlushRequest,
	handleListConnectors,
	handleMetrics,
	handlePullRequest,
	handlePushRequest,
	handleRegisterConnector,
	handleSaveSchema,
	handleSaveSyncRules,
	handleUnregisterConnector,
} from "../request-handler";
import type { GatewayConfig } from "../types";
import { createMockLakeAdapter, makeDelta } from "./helpers";

const defaultConfig: GatewayConfig = {
	gatewayId: "gw-test",
	maxBufferBytes: 1_048_576,
	maxBufferAgeMs: 30_000,
	flushFormat: "json" as const,
};

/** Serialise a push body with bigint-aware replacer. */
function pushJson(body: {
	clientId: string;
	deltas: RowDelta[];
	lastSeenHlc?: HLCTimestamp;
}): string {
	return JSON.stringify({ lastSeenHlc: HLC.encode(0, 0), ...body }, bigintReplacer);
}

/** Serialise an action body with proper Action structure. */
function actionJson(body: { clientId: string; actions: Action[] }): string {
	return JSON.stringify(body, bigintReplacer);
}

/** Create a minimal valid Action. */
function makeAction(opts: Partial<Action> & { connector: string; actionType: string }): Action {
	return {
		actionId: opts.actionId ?? `act-${Math.random().toString(36).slice(2)}`,
		clientId: opts.clientId ?? "client-a",
		hlc: opts.hlc ?? HLC.encode(1_000_000, 0),
		connector: opts.connector,
		actionType: opts.actionType,
		params: opts.params ?? {},
	};
}

// ---------------------------------------------------------------------------
// handlePushRequest
// ---------------------------------------------------------------------------

describe("handlePushRequest", () => {
	it("valid push returns 200 with serverHlc and accepted count", () => {
		const gw = new SyncGateway(defaultConfig);
		const hlc = HLC.encode(1_000_000, 0);
		const delta = makeDelta({ hlc });
		const raw = pushJson({ clientId: "client-a", deltas: [delta] });

		const result = handlePushRequest(gw, raw);

		expect(result.status).toBe(200);
		const body = result.body as { serverHlc: HLCTimestamp; accepted: number; deltas: RowDelta[] };
		expect(body.accepted).toBe(1);
		expect(body.serverHlc).toBeDefined();
		expect(body.deltas).toHaveLength(1);
	});

	it("invalid JSON returns 400", () => {
		const gw = new SyncGateway(defaultConfig);
		const result = handlePushRequest(gw, "not json{{{");

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Invalid JSON");
	});

	it("missing fields returns 400", () => {
		const gw = new SyncGateway(defaultConfig);
		const result = handlePushRequest(gw, JSON.stringify({ clientId: "c1" }));

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Missing required fields");
	});

	it("client ID mismatch returns 403", () => {
		const gw = new SyncGateway(defaultConfig);
		const hlc = HLC.encode(1_000_000, 0);
		const delta = makeDelta({ hlc });
		const raw = pushJson({ clientId: "client-a", deltas: [delta] });

		const result = handlePushRequest(gw, raw, "different-client");

		expect(result.status).toBe(403);
		expect((result.body as { error: string }).error).toContain("mismatch");
	});

	it("clock drift returns 409", () => {
		const gw = new SyncGateway(defaultConfig);
		// Create a delta with HLC very far in the future
		const futureHlc = HLC.encode(Date.now() + 120_000, 0);
		const delta = makeDelta({ hlc: futureHlc });
		const raw = pushJson({ clientId: "client-a", deltas: [delta] });

		const result = handlePushRequest(gw, raw);

		expect(result.status).toBe(409);
	});

	it("backpressure returns 503", () => {
		const gw = new SyncGateway({
			...defaultConfig,
			maxBackpressureBytes: 1, // Tiny limit to trigger backpressure
		});
		// Fill the buffer to exceed the tiny backpressure limit
		const hlc1 = HLC.encode(1_000_000, 0);
		const delta1 = makeDelta({ hlc: hlc1, deltaId: "d1" });
		gw.handlePush({ clientId: "client-a", deltas: [delta1], lastSeenHlc: hlc1 });

		const hlc2 = HLC.encode(1_000_001, 0);
		const delta2 = makeDelta({ hlc: hlc2, deltaId: "d2" });
		const raw = pushJson({ clientId: "client-a", deltas: [delta2] });

		const result = handlePushRequest(gw, raw);

		expect(result.status).toBe(503);
	});

	it("calls persistBatch and clearPersistence on success", () => {
		const gw = new SyncGateway(defaultConfig);
		const hlc = HLC.encode(1_000_000, 0);
		const delta = makeDelta({ hlc });
		const raw = pushJson({ clientId: "client-a", deltas: [delta] });

		const persistBatch = vi.fn();
		const clearPersistence = vi.fn();

		handlePushRequest(gw, raw, null, { persistBatch, clearPersistence });

		expect(persistBatch).toHaveBeenCalledTimes(1);
		expect(persistBatch).toHaveBeenCalledWith(
			expect.arrayContaining([expect.objectContaining({ deltaId: delta.deltaId })]),
		);
		expect(clearPersistence).toHaveBeenCalledTimes(1);
	});

	it("calls broadcastFn with ingested deltas", () => {
		const gw = new SyncGateway(defaultConfig);
		const hlc = HLC.encode(1_000_000, 0);
		const delta = makeDelta({ hlc });
		const raw = pushJson({ clientId: "client-a", deltas: [delta] });

		const broadcastFn = vi.fn();

		handlePushRequest(gw, raw, null, { broadcastFn });

		expect(broadcastFn).toHaveBeenCalledTimes(1);
		expect(broadcastFn).toHaveBeenCalledWith(
			expect.arrayContaining([expect.objectContaining({ deltaId: delta.deltaId })]),
			expect.anything(),
			"client-a",
		);
	});
});

// ---------------------------------------------------------------------------
// handlePullRequest
// ---------------------------------------------------------------------------

describe("handlePullRequest", () => {
	it("valid pull returns 200 with deltas", async () => {
		const gw = new SyncGateway(defaultConfig);
		const hlc = HLC.encode(1_000_000, 0);
		const delta = makeDelta({ hlc });
		gw.handlePush({ clientId: "client-a", deltas: [delta], lastSeenHlc: hlc });

		const result = await handlePullRequest(gw, {
			since: "0",
			clientId: "client-b",
			limit: "100",
			source: null,
		});

		expect(result.status).toBe(200);
		const body = result.body as { deltas: RowDelta[]; serverHlc: HLCTimestamp; hasMore: boolean };
		expect(body.deltas.length).toBeGreaterThanOrEqual(1);
		expect(body.hasMore).toBe(false);
	});

	it("missing params returns 400", async () => {
		const gw = new SyncGateway(defaultConfig);

		const result = await handlePullRequest(gw, {
			since: null,
			clientId: null,
			limit: null,
			source: null,
		});

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Missing required query params");
	});

	it("invalid since returns 400", async () => {
		const gw = new SyncGateway(defaultConfig);

		const result = await handlePullRequest(gw, {
			since: "not-a-number",
			clientId: "client-b",
			limit: null,
			source: null,
		});

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Invalid 'since'");
	});

	it("source adapter not found returns 404", async () => {
		const gw = new SyncGateway(defaultConfig);

		const result = await handlePullRequest(gw, {
			since: "0",
			clientId: "client-b",
			limit: "100",
			source: "nonexistent",
		});

		expect(result.status).toBe(404);
	});

	it("with sync rules returns filtered results", async () => {
		const gw = new SyncGateway(defaultConfig);
		const hlc1 = HLC.encode(1_000_000, 0);
		const hlc2 = HLC.encode(1_000_001, 0);
		const delta1 = makeDelta({
			hlc: hlc1,
			deltaId: "d1",
			columns: [{ column: "tenant", value: "t1" }],
		});
		const delta2 = makeDelta({
			hlc: hlc2,
			deltaId: "d2",
			columns: [{ column: "tenant", value: "t2" }],
		});
		gw.handlePush({ clientId: "client-a", deltas: [delta1, delta2], lastSeenHlc: hlc1 });

		const syncRules: SyncRulesConfig = {
			version: 1,
			buckets: [
				{
					name: "tenant-bucket",
					tables: [],
					filters: [{ column: "tenant", op: "eq", value: "jwt:tenant" }],
				},
			],
		};

		const result = await handlePullRequest(
			gw,
			{ since: "0", clientId: "client-b", limit: "100", source: null },
			{ tenant: "t1" },
			syncRules,
		);

		expect(result.status).toBe(200);
		const body = result.body as { deltas: RowDelta[] };
		// Only delta with tenant=t1 should pass the filter
		expect(
			body.deltas.every((d) => d.columns.some((c) => c.column === "tenant" && c.value === "t1")),
		).toBe(true);
	});

	it("with registered source adapter returns 200", async () => {
		const gw = new SyncGateway(defaultConfig);

		// Register a mock source adapter that returns some deltas
		const hlc = HLC.encode(1_000_000, 0);
		const mockAdapter = {
			async insertDeltas() {
				return { ok: true as const, value: undefined };
			},
			async queryDeltasSince() {
				return {
					ok: true as const,
					value: [makeDelta({ hlc, deltaId: "source-d1" })],
				};
			},
			async getLatestState() {
				return { ok: true as const, value: null };
			},
			async ensureSchema() {
				return { ok: true as const, value: undefined };
			},
			async close() {},
		};

		gw.registerSource("my-source", mockAdapter);

		const result = await handlePullRequest(gw, {
			since: "0",
			clientId: "client-b",
			limit: "100",
			source: "my-source",
		});

		expect(result.status).toBe(200);
		const body = result.body as { deltas: RowDelta[] };
		expect(body.deltas).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// handleActionRequest
// ---------------------------------------------------------------------------

describe("handleActionRequest", () => {
	it("valid action returns 200", async () => {
		const gw = new SyncGateway(defaultConfig);

		const handler: ActionHandler = {
			supportedActions: [{ actionType: "greet", description: "Says hello" }],
			executeAction: async (action) =>
				Ok({
					actionId: action.actionId,
					data: { message: `Hello ${action.params?.name ?? "world"}` },
					serverHlc: HLC.encode(1_000_000, 0),
				}),
		};
		gw.registerActionHandler("greeter", handler);

		const raw = actionJson({
			clientId: "client-a",
			actions: [
				makeAction({ connector: "greeter", actionType: "greet", params: { name: "test" } }),
			],
		});

		const result = await handleActionRequest(gw, raw);

		expect(result.status).toBe(200);
		const body = result.body as { results: unknown[] };
		expect(body.results).toHaveLength(1);
	});

	it("invalid body returns 400", async () => {
		const gw = new SyncGateway(defaultConfig);
		const result = await handleActionRequest(gw, "not-json");

		expect(result.status).toBe(400);
	});

	it("client ID mismatch returns 403", async () => {
		const gw = new SyncGateway(defaultConfig);
		const raw = actionJson({
			clientId: "client-a",
			actions: [makeAction({ connector: "test", actionType: "action" })],
		});

		const result = await handleActionRequest(gw, raw, "different-client");

		expect(result.status).toBe(403);
	});

	it("unregistered handler returns ACTION_NOT_SUPPORTED in results", async () => {
		const gw = new SyncGateway(defaultConfig);
		const raw = actionJson({
			clientId: "client-a",
			actions: [makeAction({ connector: "missing", actionType: "action" })],
		});

		const result = await handleActionRequest(gw, raw);

		expect(result.status).toBe(200);
		const body = result.body as { results: Array<{ code?: string }> };
		expect(body.results[0]!.code).toBe("ACTION_NOT_SUPPORTED");
	});
});

// ---------------------------------------------------------------------------
// handleFlushRequest
// ---------------------------------------------------------------------------

describe("handleFlushRequest", () => {
	it("success returns 200 with flushed: true", async () => {
		const adapter = createMockLakeAdapter();
		const gw = new SyncGateway({ ...defaultConfig, adapter });
		// Push a delta so there is something to flush
		const hlc = HLC.encode(1_000_000, 0);
		gw.handlePush({
			clientId: "client-a",
			deltas: [makeDelta({ hlc })],
			lastSeenHlc: hlc,
		});

		const result = await handleFlushRequest(gw);

		expect(result.status).toBe(200);
		expect((result.body as { flushed: boolean }).flushed).toBe(true);
	});

	it("calls clearPersistence on success", async () => {
		const adapter = createMockLakeAdapter();
		const gw = new SyncGateway({ ...defaultConfig, adapter });
		const hlc = HLC.encode(1_000_000, 0);
		gw.handlePush({
			clientId: "client-a",
			deltas: [makeDelta({ hlc })],
			lastSeenHlc: hlc,
		});

		const clearPersistence = vi.fn();
		await handleFlushRequest(gw, { clearPersistence });

		expect(clearPersistence).toHaveBeenCalledTimes(1);
	});

	it("error returns 500", async () => {
		const gw = new SyncGateway(defaultConfig); // No adapter configured
		const hlc = HLC.encode(1_000_000, 0);
		gw.handlePush({
			clientId: "client-a",
			deltas: [makeDelta({ hlc })],
			lastSeenHlc: hlc,
		});

		const result = await handleFlushRequest(gw);

		expect(result.status).toBe(500);
		expect((result.body as { error: string }).error).toBeDefined();
	});
});

// ---------------------------------------------------------------------------
// handleSaveSchema
// ---------------------------------------------------------------------------

describe("handleSaveSchema", () => {
	it("valid schema returns 200 + saved: true", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({
			table: "todos",
			columns: [
				{ name: "title", type: "string" },
				{ name: "done", type: "boolean" },
			],
		});

		const result = await handleSaveSchema(raw, store, "gw-1");

		expect(result.status).toBe(200);
		expect((result.body as { saved: boolean }).saved).toBe(true);

		const stored = await store.getSchema("gw-1");
		expect(stored?.table).toBe("todos");
	});

	it("invalid JSON returns 400", async () => {
		const store = new MemoryConfigStore();
		const result = await handleSaveSchema("not json", store, "gw-1");

		expect(result.status).toBe(400);
	});

	it("missing table returns 400", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({ columns: [{ name: "x", type: "string" }] });

		const result = await handleSaveSchema(raw, store, "gw-1");

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Missing required fields");
	});

	it("invalid column type returns 400", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({
			table: "todos",
			columns: [{ name: "x", type: "invalid-type" }],
		});

		const result = await handleSaveSchema(raw, store, "gw-1");

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Invalid column type");
	});
});

// ---------------------------------------------------------------------------
// handleSaveSyncRules
// ---------------------------------------------------------------------------

describe("handleSaveSyncRules", () => {
	it("valid rules returns 200 + saved: true", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({
			version: 1,
			buckets: [
				{
					name: "user-bucket",
					tables: [],
					filters: [{ column: "userId", op: "eq", value: "jwt:sub" }],
				},
			],
		});

		const result = await handleSaveSyncRules(raw, store, "gw-1");

		expect(result.status).toBe(200);
		expect((result.body as { saved: boolean }).saved).toBe(true);

		const stored = await store.getSyncRules("gw-1");
		expect(stored?.buckets).toHaveLength(1);
	});

	it("invalid JSON returns 400", async () => {
		const store = new MemoryConfigStore();
		const result = await handleSaveSyncRules("bad json!!!", store, "gw-1");

		expect(result.status).toBe(400);
		expect((result.body as { error: string }).error).toContain("Invalid JSON");
	});

	it("invalid rules returns 400", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({ version: 1 }); // Missing buckets

		const result = await handleSaveSyncRules(raw, store, "gw-1");

		expect(result.status).toBe(400);
	});
});

// ---------------------------------------------------------------------------
// handleRegisterConnector
// ---------------------------------------------------------------------------

describe("handleRegisterConnector", () => {
	it("valid connector returns 200 + registered: true", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({
			name: "my-pg",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/db" },
		});

		const result = await handleRegisterConnector(raw, store);

		expect(result.status).toBe(200);
		const body = result.body as { registered: boolean; name: string };
		expect(body.registered).toBe(true);
		expect(body.name).toBe("my-pg");

		const connectors = await store.getConnectors();
		expect(connectors["my-pg"]).toBeDefined();
	});

	it("invalid JSON returns 400", async () => {
		const store = new MemoryConfigStore();
		const result = await handleRegisterConnector("bad json", store);

		expect(result.status).toBe(400);
	});

	it("duplicate name returns 409", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({
			name: "my-pg",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/db" },
		});

		await handleRegisterConnector(raw, store);
		const result = await handleRegisterConnector(raw, store);

		expect(result.status).toBe(409);
		expect((result.body as { error: string }).error).toContain("already exists");
	});

	it("invalid config returns 400", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({ name: "", type: "postgres" });

		const result = await handleRegisterConnector(raw, store);

		expect(result.status).toBe(400);
	});
});

// ---------------------------------------------------------------------------
// handleUnregisterConnector
// ---------------------------------------------------------------------------

describe("handleUnregisterConnector", () => {
	it("existing connector returns 200 + unregistered: true", async () => {
		const store = new MemoryConfigStore();
		// First register
		const raw = JSON.stringify({
			name: "my-pg",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/db" },
		});
		await handleRegisterConnector(raw, store);

		const result = await handleUnregisterConnector("my-pg", store);

		expect(result.status).toBe(200);
		const body = result.body as { unregistered: boolean; name: string };
		expect(body.unregistered).toBe(true);
		expect(body.name).toBe("my-pg");

		const connectors = await store.getConnectors();
		expect(connectors["my-pg"]).toBeUndefined();
	});

	it("not found returns 404", async () => {
		const store = new MemoryConfigStore();
		const result = await handleUnregisterConnector("nonexistent", store);

		expect(result.status).toBe(404);
		expect((result.body as { error: string }).error).toContain("not found");
	});
});

// ---------------------------------------------------------------------------
// handleListConnectors
// ---------------------------------------------------------------------------

describe("handleListConnectors", () => {
	it("empty store returns 200 + empty array", async () => {
		const store = new MemoryConfigStore();

		const result = await handleListConnectors(store);

		expect(result.status).toBe(200);
		expect(result.body).toEqual([]);
	});

	it("with connectors returns sanitised list", async () => {
		const store = new MemoryConfigStore();
		const raw = JSON.stringify({
			name: "my-pg",
			type: "postgres",
			postgres: { connectionString: "postgres://localhost/db" },
			ingest: {
				tables: [
					{
						table: "users",
						query: "SELECT * FROM users",
						rowIdColumn: "id",
						strategy: { type: "diff" },
					},
				],
				intervalMs: 5000,
			},
		});
		await handleRegisterConnector(raw, store);

		const result = await handleListConnectors(store);

		expect(result.status).toBe(200);
		const list = result.body as Array<{ name: string; type: string; hasIngest: boolean }>;
		expect(list).toHaveLength(1);
		expect(list[0]!.name).toBe("my-pg");
		expect(list[0]!.type).toBe("postgres");
		expect(list[0]!.hasIngest).toBe(true);
	});
});

// ---------------------------------------------------------------------------
// handleMetrics
// ---------------------------------------------------------------------------

describe("handleMetrics", () => {
	it("returns buffer stats", () => {
		const gw = new SyncGateway(defaultConfig);

		const result = handleMetrics(gw);

		expect(result.status).toBe(200);
		const body = result.body as { logSize: number; indexSize: number; byteSize: number };
		expect(body.logSize).toBe(0);
		expect(body.indexSize).toBe(0);
		expect(body.byteSize).toBe(0);
	});

	it("includes extra fields when provided", () => {
		const gw = new SyncGateway(defaultConfig);

		const result = handleMetrics(gw, { uptime: 12345 });

		expect(result.status).toBe(200);
		const body = result.body as { logSize: number; uptime: number };
		expect(body.logSize).toBe(0);
		expect(body.uptime).toBe(12345);
	});
});
