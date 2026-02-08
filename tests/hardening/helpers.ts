import {
	LocalDB,
	LocalTransport,
	MemoryQueue,
	registerSchema,
	SyncCoordinator,
} from "@lakesync/client";
import type { HLCTimestamp, RowDelta, TableSchema } from "@lakesync/core";
import { HLC, unwrapOrThrow } from "@lakesync/core";
import type { SyncGateway } from "@lakesync/gateway";

export {
	createMockAdapter,
	createTestGateway,
	createTestHLC,
	makeDelta,
} from "../integration/helpers";

export const TodoSchema: TableSchema = {
	table: "todos",
	columns: [
		{ name: "title", type: "string" },
		{ name: "completed", type: "boolean" },
	],
};

/**
 * Creates a shared monotonic clock source for multi-client tests.
 *
 * Each call to `tick()` advances the shared time by `stepMs`.
 * The `clock(offset)` method returns a wallClock function for HLC
 * construction that adds a fixed offset to the shared time.
 */
export function createSharedClock(startMs = 1_000_000, stepMs = 100) {
	let now = startMs;
	return {
		/** Advance the shared time by one step and return the new value */
		tick(): number {
			now += stepMs;
			return now;
		},
		/** Return a wallClock function for HLC with a fixed offset */
		clock(offset = 0): () => number {
			return () => now + offset;
		},
		/** Get the current shared time */
		get now() {
			return now;
		},
	};
}

/**
 * Helper to open a LocalDB, register schemas, and build a
 * SyncCoordinator with an injectable HLC clock and MemoryQueue.
 */
export async function createClient(
	gateway: SyncGateway,
	opts: {
		clientId: string;
		wallClock: () => number;
		dbName: string;
		schemas?: TableSchema[];
	},
) {
	const db = unwrapOrThrow(await LocalDB.open({ name: opts.dbName, backend: "memory" }));
	const schemas = opts.schemas ?? [TodoSchema];
	for (const schema of schemas) {
		unwrapOrThrow(await registerSchema(db, schema));
	}

	const hlc = new HLC(opts.wallClock);
	const transport = new LocalTransport(gateway);
	const queue = new MemoryQueue();
	const coordinator = new SyncCoordinator(db, transport, {
		hlc,
		queue,
		clientId: opts.clientId,
	});

	return { db, coordinator, tracker: coordinator.tracker, queue };
}

/**
 * Generate N deltas with sequential HLCs from a given HLC instance.
 */
export function makeBatchDeltas(
	hlc: HLC,
	count: number,
	opts?: {
		table?: string;
		clientId?: string;
		rowIdPrefix?: string;
		columnValue?: string;
	},
): RowDelta[] {
	const table = opts?.table ?? "todos";
	const clientId = opts?.clientId ?? "batch-client";
	const prefix = opts?.rowIdPrefix ?? "row";
	const value = opts?.columnValue ?? "test";

	const deltas: RowDelta[] = [];
	for (let i = 0; i < count; i++) {
		deltas.push({
			op: "UPDATE",
			table,
			rowId: `${prefix}-${i}`,
			clientId,
			columns: [{ column: "title", value }],
			hlc: hlc.now(),
			deltaId: `batch-${prefix}-${i}-${Math.random().toString(36).slice(2)}`,
		});
	}
	return deltas;
}

/**
 * Assert that an array of HLCTimestamps is strictly monotonically increasing.
 */
export function assertMonotonicallyIncreasing(timestamps: HLCTimestamp[]) {
	for (let i = 1; i < timestamps.length; i++) {
		const prev = timestamps[i - 1]!;
		const curr = timestamps[i]!;
		if (curr <= prev) {
			throw new Error(`Monotonicity violation at index ${i}: ${curr} <= ${prev}`);
		}
	}
}
