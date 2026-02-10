import { unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { HLCTimestamp, RowDelta } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MemoryPersistence, SqlitePersistence } from "../persistence";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a minimal RowDelta for testing. */
function makeDelta(overrides: Partial<RowDelta> = {}): RowDelta {
	return {
		deltaId: overrides.deltaId ?? crypto.randomUUID(),
		table: overrides.table ?? "tasks",
		rowId: overrides.rowId ?? crypto.randomUUID(),
		clientId: overrides.clientId ?? "client-1",
		hlc: overrides.hlc ?? ((BigInt(Date.now()) << 16n) as HLCTimestamp),
		op: overrides.op ?? "INSERT",
		columns: overrides.columns ?? [{ column: "title", value: "Test" }],
	};
}

/** Generate a unique temp DB path. */
function tempDbPath(): string {
	return join(
		tmpdir(),
		`lakesync-test-${Date.now()}-${Math.random().toString(36).slice(2)}.sqlite`,
	);
}

/** Safely remove a file, ignoring errors if it doesn't exist. */
function safeUnlink(path: string): void {
	try {
		unlinkSync(path);
	} catch {
		// Ignore cleanup errors
	}
	// Also remove WAL and SHM files that better-sqlite3 may create
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

// ---------------------------------------------------------------------------
// MemoryPersistence
// ---------------------------------------------------------------------------

describe("MemoryPersistence", () => {
	let persistence: MemoryPersistence;

	beforeEach(() => {
		persistence = new MemoryPersistence();
	});

	it("appendBatch + loadAll roundtrip with a single delta", () => {
		const delta = makeDelta();
		persistence.appendBatch([delta]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(1);
		expect(loaded[0]!.deltaId).toBe(delta.deltaId);
		expect(loaded[0]!.table).toBe(delta.table);
		expect(loaded[0]!.rowId).toBe(delta.rowId);
	});

	it("appendBatch + loadAll with multiple deltas", () => {
		const d1 = makeDelta({ clientId: "c1" });
		const d2 = makeDelta({ clientId: "c2" });
		const d3 = makeDelta({ clientId: "c3" });

		persistence.appendBatch([d1, d2, d3]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(3);
		expect(loaded[0]!.clientId).toBe("c1");
		expect(loaded[1]!.clientId).toBe("c2");
		expect(loaded[2]!.clientId).toBe("c3");
	});

	it("loadAll on empty persistence returns empty array", () => {
		expect(persistence.loadAll()).toEqual([]);
	});

	it("multiple appendBatch calls accumulate correctly", () => {
		persistence.appendBatch([makeDelta({ clientId: "c1" })]);
		persistence.appendBatch([makeDelta({ clientId: "c2" })]);
		persistence.appendBatch([makeDelta({ clientId: "c3" })]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(3);
		expect(loaded.map((d) => d.clientId)).toEqual(["c1", "c2", "c3"]);
	});

	it("clear() removes all data", () => {
		persistence.appendBatch([makeDelta(), makeDelta()]);
		expect(persistence.loadAll()).toHaveLength(2);

		persistence.clear();
		expect(persistence.loadAll()).toHaveLength(0);
	});

	it("loadAll returns a copy, not a reference to internal buffer", () => {
		const delta = makeDelta();
		persistence.appendBatch([delta]);

		const loaded1 = persistence.loadAll();
		const loaded2 = persistence.loadAll();
		expect(loaded1).not.toBe(loaded2);
		expect(loaded1).toEqual(loaded2);
	});

	it("close() empties the buffer", () => {
		persistence.appendBatch([makeDelta()]);
		persistence.close();
		expect(persistence.loadAll()).toHaveLength(0);
	});
});

// ---------------------------------------------------------------------------
// SqlitePersistence
// ---------------------------------------------------------------------------

describe("SqlitePersistence", () => {
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

	// -- Basic operations --

	it("appendBatch + loadAll roundtrip with a single delta", () => {
		const delta = makeDelta();
		persistence.appendBatch([delta]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(1);
		expect(loaded[0]!.deltaId).toBe(delta.deltaId);
		expect(loaded[0]!.table).toBe(delta.table);
		expect(loaded[0]!.rowId).toBe(delta.rowId);
		expect(loaded[0]!.clientId).toBe(delta.clientId);
		expect(loaded[0]!.op).toBe(delta.op);
		expect(loaded[0]!.columns).toEqual(delta.columns);
	});

	it("appendBatch + loadAll with multiple deltas", () => {
		const d1 = makeDelta({ clientId: "c1" });
		const d2 = makeDelta({ clientId: "c2" });
		const d3 = makeDelta({ clientId: "c3" });

		persistence.appendBatch([d1, d2, d3]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(3);
		expect(loaded[0]!.clientId).toBe("c1");
		expect(loaded[1]!.clientId).toBe("c2");
		expect(loaded[2]!.clientId).toBe("c3");
	});

	it("loadAll on empty database returns empty array", () => {
		expect(persistence.loadAll()).toEqual([]);
	});

	it("multiple appendBatch calls accumulate correctly", () => {
		persistence.appendBatch([makeDelta({ clientId: "c1" })]);
		persistence.appendBatch([makeDelta({ clientId: "c2" })]);
		persistence.appendBatch([makeDelta({ clientId: "c3" })]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(3);
		expect(loaded.map((d) => d.clientId)).toEqual(["c1", "c2", "c3"]);
	});

	it("clear() removes all data, subsequent loadAll returns empty", () => {
		persistence.appendBatch([makeDelta(), makeDelta()]);
		expect(persistence.loadAll()).toHaveLength(2);

		persistence.clear();
		expect(persistence.loadAll()).toHaveLength(0);
	});

	// -- HLC bigint handling --

	it("HLC bigint values survive serialisation/deserialisation", () => {
		const hlc = (BigInt(Date.now()) << 16n) as HLCTimestamp;
		const delta = makeDelta({ hlc });

		persistence.appendBatch([delta]);
		const loaded = persistence.loadAll();

		expect(typeof loaded[0]!.hlc).toBe("bigint");
		expect(loaded[0]!.hlc).toBe(hlc);
	});

	it("large HLC values near 64-bit max preserved correctly", () => {
		// 48-bit wall clock at max value + 16-bit counter at max
		const largeHlc = (((2n ** 48n - 1n) << 16n) | 0xffffn) as HLCTimestamp;
		const delta = makeDelta({ hlc: largeHlc });

		persistence.appendBatch([delta]);
		const loaded = persistence.loadAll();

		expect(typeof loaded[0]!.hlc).toBe("bigint");
		expect(loaded[0]!.hlc).toBe(largeHlc);
	});

	it("HLC with counter component preserved", () => {
		// Wall clock timestamp with counter = 42
		const hlc = ((BigInt(Date.now()) << 16n) | 42n) as HLCTimestamp;
		const delta = makeDelta({ hlc });

		persistence.appendBatch([delta]);
		const loaded = persistence.loadAll();

		expect(loaded[0]!.hlc).toBe(hlc);
		// Verify counter component is preserved
		expect(loaded[0]!.hlc & 0xffffn).toBe(42n);
	});

	// -- Recovery simulation --

	it("data recovers when opening new instance on same DB file", () => {
		const d1 = makeDelta({ clientId: "recover-1" });
		const d2 = makeDelta({ clientId: "recover-2" });
		persistence.appendBatch([d1, d2]);
		persistence.close();

		// Open a new instance on the same file
		const recovered = new SqlitePersistence(dbPath);
		const loaded = recovered.loadAll();

		expect(loaded).toHaveLength(2);
		expect(loaded[0]!.clientId).toBe("recover-1");
		expect(loaded[1]!.clientId).toBe("recover-2");
		expect(typeof loaded[0]!.hlc).toBe("bigint");

		recovered.close();
		// Reassign so afterEach close doesn't double-close
		persistence = new SqlitePersistence(dbPath);
	});

	it("batches across multiple instances are all preserved", () => {
		persistence.appendBatch([makeDelta({ clientId: "batch-1" })]);
		persistence.close();

		const p2 = new SqlitePersistence(dbPath);
		p2.appendBatch([makeDelta({ clientId: "batch-2" })]);
		p2.close();

		const p3 = new SqlitePersistence(dbPath);
		p3.appendBatch([makeDelta({ clientId: "batch-3" })]);

		const loaded = p3.loadAll();
		expect(loaded).toHaveLength(3);
		expect(loaded.map((d) => d.clientId)).toEqual(["batch-1", "batch-2", "batch-3"]);

		p3.close();
		// Reassign so afterEach doesn't double-close
		persistence = new SqlitePersistence(dbPath);
	});

	// -- Edge cases --

	it("large batch (500 deltas) persisted correctly", () => {
		const deltas: RowDelta[] = [];
		for (let i = 0; i < 500; i++) {
			deltas.push(makeDelta({ clientId: `client-${i}` }));
		}

		persistence.appendBatch(deltas);
		const loaded = persistence.loadAll();

		expect(loaded).toHaveLength(500);
		expect(loaded[0]!.clientId).toBe("client-0");
		expect(loaded[499]!.clientId).toBe("client-499");
	});

	it("delta with empty columns array", () => {
		const delta = makeDelta({ columns: [] });
		persistence.appendBatch([delta]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(1);
		expect(loaded[0]!.columns).toEqual([]);
	});

	it("delta with many columns", () => {
		const columns = Array.from({ length: 50 }, (_, i) => ({
			column: `col_${i}`,
			value: `value_${i}`,
		}));
		const delta = makeDelta({ columns });
		persistence.appendBatch([delta]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(1);
		expect(loaded[0]!.columns).toHaveLength(50);
		expect(loaded[0]!.columns[0]!.column).toBe("col_0");
		expect(loaded[0]!.columns[49]!.column).toBe("col_49");
	});

	it("preserves INSERT, UPDATE, and DELETE operations", () => {
		const d1 = makeDelta({ op: "INSERT" });
		const d2 = makeDelta({ op: "UPDATE" });
		const d3 = makeDelta({ op: "DELETE" });

		persistence.appendBatch([d1, d2, d3]);
		const loaded = persistence.loadAll();

		expect(loaded[0]!.op).toBe("INSERT");
		expect(loaded[1]!.op).toBe("UPDATE");
		expect(loaded[2]!.op).toBe("DELETE");
	});

	it("clear followed by new append works correctly", () => {
		persistence.appendBatch([makeDelta({ clientId: "old" })]);
		persistence.clear();
		persistence.appendBatch([makeDelta({ clientId: "new" })]);

		const loaded = persistence.loadAll();
		expect(loaded).toHaveLength(1);
		expect(loaded[0]!.clientId).toBe("new");
	});
});
