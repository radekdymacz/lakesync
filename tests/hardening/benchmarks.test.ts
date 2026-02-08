import type { HLCTimestamp, RowDelta } from "@lakesync/core";
import { HLC, resolveLWW, unwrapOrThrow } from "@lakesync/core";
import { DeltaBuffer } from "@lakesync/gateway";
import { decodeRowDelta, encodeRowDelta } from "@lakesync/proto";
import { describe, expect, it } from "vitest";
import { createTestHLC, makeDelta } from "./helpers";

describe("Benchmarks", () => {
	it("DeltaBuffer.append() — 100K ops", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();

		const start = performance.now();
		const N = 100_000;

		for (let i = 0; i < N; i++) {
			advance(1);
			buffer.append(
				makeDelta({
					hlc: hlc.now(),
					rowId: `row-${i}`,
					clientId: "bench-client",
					deltaId: `bench-${i}`,
				}),
			);
		}

		const elapsed = performance.now() - start;
		const opsPerSec = Math.round(N / (elapsed / 1000));
		console.log(
			`DeltaBuffer.append(): ${opsPerSec.toLocaleString()} ops/sec (${Math.round(elapsed)}ms for ${N.toLocaleString()} ops)`,
		);

		expect(buffer.logSize).toBe(N);
	});

	it("HLC.now() — 1M ops", () => {
		const hlc = new HLC(() => Date.now());
		const N = 1_000_000;

		const start = performance.now();
		for (let i = 0; i < N; i++) {
			hlc.now();
		}
		const elapsed = performance.now() - start;
		const opsPerSec = Math.round(N / (elapsed / 1000));
		console.log(
			`HLC.now(): ${opsPerSec.toLocaleString()} ops/sec (${Math.round(elapsed)}ms for ${N.toLocaleString()} ops)`,
		);

		// No assertion — just ensure it ran
		expect(elapsed).toBeGreaterThan(0);
	});

	it("resolveLWW() — 100K conflicts", () => {
		const hlc1 = new HLC(() => 1_000_000);
		const hlc2 = new HLC(() => 1_000_001); // slightly ahead
		const N = 100_000;

		// Pre-generate deltas
		const locals: RowDelta[] = [];
		const remotes: RowDelta[] = [];
		for (let i = 0; i < N; i++) {
			locals.push(
				makeDelta({
					hlc: hlc1.now(),
					table: "todos",
					rowId: `row-${i}`,
					clientId: "client-a",
					deltaId: `local-${i}`,
				}),
			);
			remotes.push(
				makeDelta({
					hlc: hlc2.now(),
					table: "todos",
					rowId: `row-${i}`,
					clientId: "client-b",
					deltaId: `remote-${i}`,
				}),
			);
		}

		const start = performance.now();
		for (let i = 0; i < N; i++) {
			resolveLWW(locals[i]!, remotes[i]!);
		}
		const elapsed = performance.now() - start;
		const opsPerSec = Math.round(N / (elapsed / 1000));
		console.log(
			`resolveLWW(): ${opsPerSec.toLocaleString()} ops/sec (${Math.round(elapsed)}ms for ${N.toLocaleString()} ops)`,
		);

		expect(elapsed).toBeGreaterThan(0);
	});

	it("Proto encode/decode — 10K deltas", () => {
		const { hlc, advance } = createTestHLC();
		const N = 10_000;

		// Pre-generate deltas
		const deltas: RowDelta[] = [];
		for (let i = 0; i < N; i++) {
			advance(1);
			deltas.push(
				makeDelta({
					hlc: hlc.now(),
					rowId: `row-${i}`,
					clientId: "proto-client",
					deltaId: `proto-${i}`,
				}),
			);
		}

		// Encode
		const encodeStart = performance.now();
		const encoded: Uint8Array[] = [];
		for (const delta of deltas) {
			encoded.push(unwrapOrThrow(encodeRowDelta(delta)));
		}
		const encodeElapsed = performance.now() - encodeStart;
		const encodeOps = Math.round(N / (encodeElapsed / 1000));
		console.log(
			`Proto encodeRowDelta(): ${encodeOps.toLocaleString()} ops/sec (${Math.round(encodeElapsed)}ms)`,
		);

		// Decode
		const decodeStart = performance.now();
		for (const bytes of encoded) {
			unwrapOrThrow(decodeRowDelta(bytes));
		}
		const decodeElapsed = performance.now() - decodeStart;
		const decodeOps = Math.round(N / (decodeElapsed / 1000));
		console.log(
			`Proto decodeRowDelta(): ${decodeOps.toLocaleString()} ops/sec (${Math.round(decodeElapsed)}ms)`,
		);

		expect(encoded).toHaveLength(N);
	});

	it("Full push→pull cycle — 1K deltas", async () => {
		const { LocalDB, LocalTransport, MemoryQueue, registerSchema, SyncCoordinator } = await import(
			"@lakesync/client"
		);
		const { SyncGateway } = await import("@lakesync/gateway");

		let time = 1_000_000;
		const wallClock = () => time;
		const tick = () => {
			time += 100;
		};

		const gw = new SyncGateway({
			gatewayId: "bench-gw",
			maxBufferBytes: 500 * 1024 * 1024,
			maxBufferAgeMs: 60_000,
			flushFormat: "json" as const,
		});
		(gw as unknown as { hlc: HLC }).hlc = new HLC(wallClock);

		const schema = {
			table: "todos",
			columns: [
				{ name: "title", type: "string" as const },
				{ name: "completed", type: "boolean" as const },
			],
		};

		const db1 = unwrapOrThrow(await LocalDB.open({ name: "bench-push", backend: "memory" }));
		unwrapOrThrow(await registerSchema(db1, schema));
		const coord1 = new SyncCoordinator(db1, new LocalTransport(gw), {
			hlc: new HLC(wallClock),
			queue: new MemoryQueue(),
			clientId: "bench-writer",
		});

		const db2 = unwrapOrThrow(await LocalDB.open({ name: "bench-pull", backend: "memory" }));
		unwrapOrThrow(await registerSchema(db2, schema));
		const coord2 = new SyncCoordinator(db2, new LocalTransport(gw), {
			hlc: new HLC(wallClock),
			queue: new MemoryQueue(),
			clientId: "bench-reader",
		});

		const N = 1_000;

		// Insert
		for (let i = 0; i < N; i++) {
			tick();
			unwrapOrThrow(
				await coord1.tracker.insert("todos", `row-${i}`, {
					title: `Item ${i}`,
					completed: 0,
				}),
			);
		}

		const start = performance.now();

		// Push — queue peeks 100 at a time, loop to drain
		let depth = await coord1.queueDepth();
		while (depth > 0) {
			tick();
			await coord1.pushToGateway();
			depth = await coord1.queueDepth();
		}

		// Pull (may need multiple pages)
		let pulled: number;
		do {
			tick();
			pulled = await coord2.pullFromGateway();
		} while (pulled > 0);

		const elapsed = performance.now() - start;
		console.log(`Full push→pull (${N} deltas): ${Math.round(elapsed)}ms`);

		const rows = unwrapOrThrow(await db2.query<{ _rowId: string }>("SELECT _rowId FROM todos"));
		expect(rows).toHaveLength(N);

		await db1.close();
		await db2.close();
	});

	it("getEventsSince() — 100K buffer, various cursor positions", () => {
		const buffer = new DeltaBuffer();
		const { hlc, advance } = createTestHLC();
		const N = 100_000;
		const timestamps: HLCTimestamp[] = [];

		for (let i = 0; i < N; i++) {
			advance(1);
			const ts = hlc.now();
			timestamps.push(ts);
			buffer.append(
				makeDelta({
					hlc: ts,
					rowId: `row-${i}`,
					clientId: "cursor-client",
					deltaId: `cursor-${i}`,
				}),
			);
		}

		// Measure at various positions: 0%, 25%, 50%, 75%, 99%
		const positions = [0, 0.25, 0.5, 0.75, 0.99].map((pct) => Math.floor(pct * (N - 1)));

		for (const idx of positions) {
			const cursor = timestamps[idx]!;
			const start = performance.now();
			const iterations = 1_000;
			for (let i = 0; i < iterations; i++) {
				buffer.getEventsSince(cursor, 100);
			}
			const elapsed = performance.now() - start;
			const opsPerSec = Math.round(iterations / (elapsed / 1000));
			console.log(
				`getEventsSince(${Math.round((idx / N) * 100)}%): ${opsPerSec.toLocaleString()} ops/sec`,
			);
		}

		expect(buffer.logSize).toBe(N);
	});
});
