import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SyncGateway } from "@lakesync/gateway";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SourcePoller } from "../ingest/poller";
import { MemoryPersistence, SqlitePersistence } from "../persistence";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Generate a unique temp DB path. */
function tempDbPath(): string {
	return join(
		tmpdir(),
		`lakesync-cursor-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`,
	);
}

/** Safely remove a file, ignoring errors if it doesn't exist. */
function safeUnlink(path: string): void {
	try {
		unlinkSync(path);
	} catch {
		// Ignore cleanup errors
	}
	try {
		unlinkSync(`${path}-wal`);
	} catch {
		// Ignore
	}
	try {
		unlinkSync(`${path}-shm`);
	} catch {
		// Ignore
	}
}

/** Minimal SyncGateway mock for testing pollers. */
function mockGateway(): SyncGateway {
	return {
		handlePush: vi.fn(),
	} as unknown as SyncGateway;
}

// ---------------------------------------------------------------------------
// MemoryPersistence — cursor methods
// ---------------------------------------------------------------------------

describe("MemoryPersistence cursor methods", () => {
	let persistence: MemoryPersistence;

	beforeEach(() => {
		persistence = new MemoryPersistence();
	});

	it("loadCursor returns null when no cursor exists", () => {
		expect(persistence.loadCursor("unknown")).toBeNull();
	});

	it("saveCursor + loadCursor roundtrip", () => {
		const cursor = JSON.stringify({ cursorStates: { orders: "2025-01-01" } });
		persistence.saveCursor("my-connector", cursor);
		expect(persistence.loadCursor("my-connector")).toBe(cursor);
	});

	it("saveCursor overwrites previous value", () => {
		persistence.saveCursor("conn", "first");
		persistence.saveCursor("conn", "second");
		expect(persistence.loadCursor("conn")).toBe("second");
	});

	it("different connectors have independent cursors", () => {
		persistence.saveCursor("conn-a", "a");
		persistence.saveCursor("conn-b", "b");
		expect(persistence.loadCursor("conn-a")).toBe("a");
		expect(persistence.loadCursor("conn-b")).toBe("b");
	});

	it("close() clears cursors", () => {
		persistence.saveCursor("conn", "value");
		persistence.close();
		expect(persistence.loadCursor("conn")).toBeNull();
	});
});

// ---------------------------------------------------------------------------
// SqlitePersistence — cursor methods
// ---------------------------------------------------------------------------

describe("SqlitePersistence cursor methods", () => {
	let dbPath: string;
	let persistence: SqlitePersistence;

	beforeEach(() => {
		dbPath = tempDbPath();
		persistence = new SqlitePersistence(dbPath);
	});

	afterEach(() => {
		persistence.close();
		safeUnlink(dbPath);
	});

	it("loadCursor returns null when no cursor exists", () => {
		expect(persistence.loadCursor("unknown")).toBeNull();
	});

	it("saveCursor + loadCursor roundtrip", () => {
		const cursor = JSON.stringify({ cursorStates: { orders: "2025-01-01" } });
		persistence.saveCursor("my-connector", cursor);
		expect(persistence.loadCursor("my-connector")).toBe(cursor);
	});

	it("saveCursor overwrites previous value (upsert)", () => {
		persistence.saveCursor("conn", "first");
		persistence.saveCursor("conn", "second");
		expect(persistence.loadCursor("conn")).toBe("second");
	});

	it("different connectors have independent cursors", () => {
		persistence.saveCursor("conn-a", "a");
		persistence.saveCursor("conn-b", "b");
		expect(persistence.loadCursor("conn-a")).toBe("a");
		expect(persistence.loadCursor("conn-b")).toBe("b");
	});

	it("cursors survive across instances (crash recovery)", () => {
		const cursor = JSON.stringify({ cursorStates: { users: 42 } });
		persistence.saveCursor("pg-source", cursor);
		persistence.close();

		// Open new instance on same file — simulates restart
		const recovered = new SqlitePersistence(dbPath);
		expect(recovered.loadCursor("pg-source")).toBe(cursor);
		recovered.close();

		// Reassign so afterEach doesn't double-close
		persistence = new SqlitePersistence(dbPath);
	});

	it("cursor table is independent from delta table", () => {
		persistence.saveCursor("conn", "cursor-value");
		persistence.clear(); // clears deltas only
		expect(persistence.loadCursor("conn")).toBe("cursor-value");
	});
});

// ---------------------------------------------------------------------------
// SourcePoller — cursor state export/import
// ---------------------------------------------------------------------------

describe("SourcePoller cursor state", () => {
	it("getCursorState returns empty object when no polls have run", () => {
		const poller = new SourcePoller(
			{
				name: "test",
				queryFn: async () => [],
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			mockGateway(),
		);

		expect(poller.getCursorState()).toEqual({ cursorStates: {} });
	});

	it("setCursorState restores cursor for subsequent polls", async () => {
		const queryFn = vi.fn().mockResolvedValue([]);
		const poller = new SourcePoller(
			{
				name: "test",
				queryFn,
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			mockGateway(),
		);

		// Restore a cursor state
		poller.setCursorState({ cursorStates: { orders: "2025-06-01T00:00:00Z" } });

		// Run a poll — should use the restored cursor
		await poller.poll();

		expect(queryFn).toHaveBeenCalledTimes(1);
		const sql = queryFn.mock.calls[0]![0] as string;
		// Should have a WHERE clause since cursor was restored (not a first poll)
		expect(sql).toContain("WHERE");
		expect(sql).toContain("updated_at");
	});

	it("getCursorState after poll reflects updated cursor", async () => {
		const queryFn = vi
			.fn()
			.mockResolvedValue([{ id: "1", name: "Order 1", updated_at: "2025-06-15T12:00:00Z" }]);
		const poller = new SourcePoller(
			{
				name: "test",
				queryFn,
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			mockGateway(),
		);

		await poller.poll();

		const state = poller.getCursorState();
		expect(state.cursorStates).toEqual({ orders: "2025-06-15T12:00:00Z" });
	});

	it("onCursorUpdate callback is invoked after poll", async () => {
		const queryFn = vi
			.fn()
			.mockResolvedValue([{ id: "1", name: "Order 1", updated_at: "2025-06-15T12:00:00Z" }]);
		const callback = vi.fn();

		const poller = new SourcePoller(
			{
				name: "test",
				queryFn,
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			mockGateway(),
		);
		poller.onCursorUpdate = callback;

		await poller.poll();

		expect(callback).toHaveBeenCalledTimes(1);
		expect(callback).toHaveBeenCalledWith({ cursorStates: { orders: "2025-06-15T12:00:00Z" } });
	});

	it("onCursorUpdate is called even when no new rows found", async () => {
		const queryFn = vi.fn().mockResolvedValue([]);
		const callback = vi.fn();

		const poller = new SourcePoller(
			{
				name: "test",
				queryFn,
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			mockGateway(),
		);
		poller.onCursorUpdate = callback;

		await poller.poll();

		expect(callback).toHaveBeenCalledTimes(1);
	});

	it("full round-trip: poll -> save -> restore -> poll resumes from cursor", async () => {
		const persistence = new MemoryPersistence();
		const rows = [{ id: "1", name: "Order 1", updated_at: "2025-06-15T12:00:00Z" }];
		const queryFn1 = vi.fn().mockResolvedValue(rows);
		const gateway = mockGateway();

		// First poller: poll and persist cursor
		const poller1 = new SourcePoller(
			{
				name: "orders-source",
				queryFn: queryFn1,
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			gateway,
		);
		poller1.onCursorUpdate = (state) => {
			persistence.saveCursor("orders-source", JSON.stringify(state));
		};

		await poller1.poll();

		// Verify cursor was persisted
		const saved = persistence.loadCursor("orders-source");
		expect(saved).not.toBeNull();

		// Second poller: simulates restart — restore cursor and poll again
		const queryFn2 = vi.fn().mockResolvedValue([]);
		const poller2 = new SourcePoller(
			{
				name: "orders-source",
				queryFn: queryFn2,
				tables: [
					{
						table: "orders",
						query: "SELECT * FROM orders",
						strategy: { type: "cursor", cursorColumn: "updated_at" },
					},
				],
			},
			gateway,
		);

		// Restore cursor
		poller2.setCursorState(JSON.parse(saved!));

		await poller2.poll();

		// Should have used the restored cursor (WHERE clause present)
		expect(queryFn2).toHaveBeenCalledTimes(1);
		const sql = queryFn2.mock.calls[0]![0] as string;
		expect(sql).toContain("WHERE");

		// First poller should NOT have had a WHERE clause (first poll = full sync)
		const firstSql = queryFn1.mock.calls[0]![0] as string;
		expect(firstSql).not.toContain("WHERE");
	});
});
