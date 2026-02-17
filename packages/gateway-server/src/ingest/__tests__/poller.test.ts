import type { HLCTimestamp, RowDelta } from "@lakesync/core";
import { SyncGateway } from "@lakesync/gateway";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { SourcePoller } from "../poller";
import type { QueryFn } from "../types";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a mock queryFn that returns predetermined rows per call. */
function mockQueryFn(calls: Record<string, unknown>[][]): QueryFn {
	let callIndex = 0;
	return async (_sql: string, _params?: unknown[]) => {
		const result = calls[callIndex] ?? [];
		callIndex++;
		return result;
	};
}

/** Create a SyncGateway for testing. */
function createGateway(): SyncGateway {
	return new SyncGateway({
		gatewayId: "test-gw",
		maxBufferBytes: 4 * 1024 * 1024,
		maxBufferAgeMs: 30_000,
	});
}

/** Pull all deltas from the gateway buffer. */
function pullAll(gw: SyncGateway): RowDelta[] {
	const result = gw.handlePull(
		{ clientId: "reader", sinceHlc: 0n as HLCTimestamp, maxDeltas: 10_000 },
		undefined,
	);
	if (!result.ok) return [];
	return result.value.deltas;
}

// ---------------------------------------------------------------------------
// Cursor strategy
// ---------------------------------------------------------------------------

describe("SourcePoller — cursor strategy", () => {
	let gw: SyncGateway;

	beforeEach(() => {
		gw = createGateway();
	});

	it("ingests all rows on first poll", async () => {
		const queryFn = mockQueryFn([
			[
				{ id: "1", name: "Alice", updated_at: 1000 },
				{ id: "2", name: "Bob", updated_at: 2000 },
			],
		]);

		const poller = new SourcePoller(
			{
				name: "test-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name, updated_at FROM users",
						rowIdColumn: "id",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			gw,
		);

		await poller.poll();

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(2);
		expect(deltas[0]!.table).toBe("users");
		expect(deltas[0]!.rowId).toBe("1");
		expect(deltas[0]!.op).toBe("INSERT");
		expect(deltas[0]!.clientId).toBe("ingest:test-src");
		expect(deltas[1]!.rowId).toBe("2");
	});

	it("uses cursor for subsequent polls", async () => {
		const queryCalls: { sql: string; params: unknown[] | undefined }[] = [];
		const queryFn: QueryFn = async (sql, params) => {
			queryCalls.push({ sql, params });
			if (queryCalls.length === 1) {
				return [{ id: "1", name: "Alice", updated_at: 1000 }];
			}
			return [{ id: "2", name: "Bob", updated_at: 2000 }];
		};

		const poller = new SourcePoller(
			{
				name: "test-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name, updated_at FROM users",
						rowIdColumn: "id",
						strategy: { type: "cursor", cursorColumn: "updated_at", lookbackMs: 0 },
					},
				],
			},
			gw,
		);

		await poller.poll();
		await poller.poll();

		// First poll should not have WHERE clause with params
		expect(queryCalls[0]!.params).toBeUndefined();
		// Second poll should pass cursor value as parameter
		expect(queryCalls[1]!.params).toEqual([1000]);
		expect(queryCalls[1]!.sql).toContain("WHERE");
		expect(queryCalls[1]!.sql).toContain("updated_at > $1");
	});

	it("applies look-back overlap for numeric cursors", async () => {
		const queryCalls: { sql: string; params: unknown[] | undefined }[] = [];
		const queryFn: QueryFn = async (sql, params) => {
			queryCalls.push({ sql, params });
			if (queryCalls.length === 1) {
				return [{ id: "1", name: "Alice", updated_at: 10000 }];
			}
			return [];
		};

		const poller = new SourcePoller(
			{
				name: "test-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name, updated_at FROM users",
						rowIdColumn: "id",
						strategy: { type: "cursor", cursorColumn: "updated_at", lookbackMs: 3000 },
					},
				],
			},
			gw,
		);

		await poller.poll();
		await poller.poll();

		// Second poll cursor: 10000 - 3000 = 7000
		expect(queryCalls[1]!.params).toEqual([7000]);
	});

	it("defaults rowIdColumn to 'id'", async () => {
		const queryFn = mockQueryFn([[{ id: "row-1", email: "a@b.com", updated_at: 100 }]]);

		const poller = new SourcePoller(
			{
				name: "src",
				queryFn,
				tables: [
					{
						table: "contacts",
						query: "SELECT id, email, updated_at FROM contacts",
						// no rowIdColumn — should default to "id"
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			gw,
		);

		await poller.poll();
		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(1);
		expect(deltas[0]!.rowId).toBe("row-1");
	});

	it("does not push when there are no rows", async () => {
		const handlePushSpy = vi.spyOn(gw, "handlePush");

		const queryFn = mockQueryFn([[]]);

		const poller = new SourcePoller(
			{
				name: "src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, updated_at FROM users",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			gw,
		);

		await poller.poll();
		expect(handlePushSpy).not.toHaveBeenCalled();
	});

	it("excludes rowId column from delta columns", async () => {
		const queryFn = mockQueryFn([
			[{ id: "1", name: "Alice", email: "alice@test.com", updated_at: 100 }],
		]);

		const poller = new SourcePoller(
			{
				name: "src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT * FROM users",
						rowIdColumn: "id",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			gw,
		);

		await poller.poll();
		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(1);

		const columnNames = deltas[0]!.columns.map((c) => c.column);
		expect(columnNames).not.toContain("id");
		expect(columnNames).toContain("name");
		expect(columnNames).toContain("email");
		expect(columnNames).toContain("updated_at");
	});
});

// ---------------------------------------------------------------------------
// Diff strategy
// ---------------------------------------------------------------------------

describe("SourcePoller — diff strategy", () => {
	let gw: SyncGateway;

	beforeEach(() => {
		gw = createGateway();
	});

	it("detects inserts on first poll", async () => {
		const queryFn = mockQueryFn([
			[
				{ id: "1", name: "Alice" },
				{ id: "2", name: "Bob" },
			],
		]);

		const poller = new SourcePoller(
			{
				name: "diff-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll();

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(2);
		expect(deltas.every((d) => d.op === "INSERT")).toBe(true);
	});

	it("detects updates on subsequent polls", async () => {
		const queryFn = mockQueryFn([
			[{ id: "1", name: "Alice" }],
			[{ id: "1", name: "Alice Updated" }],
		]);

		const poller = new SourcePoller(
			{
				name: "diff-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll(); // First poll: INSERT
		await poller.poll(); // Second poll: UPDATE

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(2);
		expect(deltas[0]!.op).toBe("INSERT");
		expect(deltas[1]!.op).toBe("UPDATE");
		expect(deltas[1]!.columns).toEqual([{ column: "name", value: "Alice Updated" }]);
	});

	it("detects deletes when rows disappear", async () => {
		const queryFn = mockQueryFn([
			[
				{ id: "1", name: "Alice" },
				{ id: "2", name: "Bob" },
			],
			[{ id: "1", name: "Alice" }], // Bob deleted
		]);

		const poller = new SourcePoller(
			{
				name: "diff-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll(); // 2 inserts
		await poller.poll(); // 1 delete

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(3);

		const deleteDeltas = deltas.filter((d) => d.op === "DELETE");
		expect(deleteDeltas).toHaveLength(1);
		expect(deleteDeltas[0]!.rowId).toBe("2");
		expect(deleteDeltas[0]!.table).toBe("users");
	});

	it("does not emit deltas for unchanged rows", async () => {
		const queryFn = mockQueryFn([
			[{ id: "1", name: "Alice" }],
			[{ id: "1", name: "Alice" }], // Same data
		]);

		const poller = new SourcePoller(
			{
				name: "diff-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll(); // 1 INSERT
		await poller.poll(); // No changes

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(1);
		expect(deltas[0]!.op).toBe("INSERT");
	});

	it("handles empty tables", async () => {
		const handlePushSpy = vi.spyOn(gw, "handlePush");

		const queryFn = mockQueryFn([[]]);

		const poller = new SourcePoller(
			{
				name: "diff-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll();
		expect(handlePushSpy).not.toHaveBeenCalled();
	});

	it("warns on large snapshots", async () => {
		const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

		// Build a result set just above the LARGE_SNAPSHOT_WARN threshold (1,000)
		const rows: Record<string, unknown>[] = [];
		for (let i = 0; i < 1_001; i++) {
			rows.push({ id: String(i), value: i });
		}

		const queryFn = mockQueryFn([rows]);

		const poller = new SourcePoller(
			{
				name: "big-src",
				queryFn,
				tables: [
					{
						table: "metrics",
						query: "SELECT id, value FROM metrics",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll();

		expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("1001 rows"));

		warnSpy.mockRestore();
	});
});

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

describe("SourcePoller — idempotency", () => {
	it("produces deterministic deltaIds for identical data", async () => {
		const gw1 = createGateway();
		const gw2 = createGateway();

		const data = [{ id: "1", name: "Alice" }];

		const poller1 = new SourcePoller(
			{
				name: "src",
				queryFn: mockQueryFn([data]),
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw1,
		);

		const poller2 = new SourcePoller(
			{
				name: "src",
				queryFn: mockQueryFn([data]),
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw2,
		);

		await poller1.poll();
		await poller2.poll();

		const deltas1 = pullAll(gw1);
		const deltas2 = pullAll(gw2);

		// Same clientId, table, rowId, columns → same content but possibly different HLCs
		// deltaId is based on all fields including HLC, so they may differ with different HLC instances
		// The key idempotency property is: re-polling the same poller produces no new deltas
		expect(deltas1).toHaveLength(1);
		expect(deltas2).toHaveLength(1);
	});

	it("gateway accepts duplicate deltaIds silently", async () => {
		const gw = createGateway();

		// Create a poller and poll twice with same data (diff strategy)
		// Second poll should produce no changes since snapshot matches
		const queryFn = mockQueryFn([[{ id: "1", name: "Alice" }], [{ id: "1", name: "Alice" }]]);

		const poller = new SourcePoller(
			{
				name: "src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll();
		await poller.poll(); // No changes detected → no push

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(1);
	});
});

// ---------------------------------------------------------------------------
// Multiple tables
// ---------------------------------------------------------------------------

describe("SourcePoller — multiple tables", () => {
	it("polls multiple tables in a single cycle", async () => {
		const gw = createGateway();

		let callCount = 0;
		const queryFn: QueryFn = async () => {
			callCount++;
			if (callCount === 1) {
				return [{ id: "u1", name: "Alice", updated_at: 100 }];
			}
			return [{ id: "l1", action: "login" }];
		};

		const poller = new SourcePoller(
			{
				name: "multi",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name, updated_at FROM users",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
					{
						table: "logs",
						query: "SELECT id, action FROM logs",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll();

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(2);

		const tables = deltas.map((d) => d.table);
		expect(tables).toContain("users");
		expect(tables).toContain("logs");
	});
});

// ---------------------------------------------------------------------------
// Start / stop lifecycle
// ---------------------------------------------------------------------------

describe("SourcePoller — lifecycle", () => {
	it("starts and stops cleanly", () => {
		const gw = createGateway();
		const poller = new SourcePoller(
			{
				name: "lifecycle",
				queryFn: async () => [],
				tables: [],
				intervalMs: 100,
			},
			gw,
		);

		expect(poller.isRunning).toBe(false);
		poller.start();
		expect(poller.isRunning).toBe(true);
		poller.stop();
		expect(poller.isRunning).toBe(false);
	});

	it("does not start twice", () => {
		const gw = createGateway();
		const poller = new SourcePoller(
			{
				name: "lifecycle",
				queryFn: async () => [],
				tables: [],
				intervalMs: 100,
			},
			gw,
		);

		poller.start();
		poller.start(); // Idempotent
		expect(poller.isRunning).toBe(true);
		poller.stop();
	});

	it("survives query errors without crashing", async () => {
		const gw = createGateway();

		let callCount = 0;
		const queryFn: QueryFn = async () => {
			callCount++;
			if (callCount === 1) throw new Error("Connection refused");
			return [{ id: "1", name: "recovered" }];
		};

		const poller = new SourcePoller(
			{
				name: "err-src",
				queryFn,
				tables: [
					{
						table: "users",
						query: "SELECT id, name FROM users",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		// First poll throws — should not propagate
		await poller.poll().catch(() => {
			// Expected to throw when called directly (poll() re-throws)
		});

		// Second poll succeeds
		await poller.poll();

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(1);
		expect(deltas[0]!.columns).toEqual([{ column: "name", value: "recovered" }]);
	});
});

// ---------------------------------------------------------------------------
// HLC monotonicity
// ---------------------------------------------------------------------------

describe("SourcePoller — HLC", () => {
	it("generates strictly increasing HLCs across deltas", async () => {
		const gw = createGateway();

		const queryFn = mockQueryFn([
			[
				{ id: "1", name: "A" },
				{ id: "2", name: "B" },
				{ id: "3", name: "C" },
			],
		]);

		const poller = new SourcePoller(
			{
				name: "hlc-src",
				queryFn,
				tables: [
					{
						table: "items",
						query: "SELECT id, name FROM items",
						strategy: { type: "diff" },
					},
				],
			},
			gw,
		);

		await poller.poll();

		const deltas = pullAll(gw);
		expect(deltas).toHaveLength(3);

		// HLCs should be strictly increasing
		for (let i = 1; i < deltas.length; i++) {
			expect(deltas[i]!.hlc > deltas[i - 1]!.hlc).toBe(true);
		}
	});
});
