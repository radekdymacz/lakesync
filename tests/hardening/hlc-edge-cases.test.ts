import type { HLCTimestamp } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { assertMonotonicallyIncreasing } from "./helpers";

describe("HLC Edge Cases", () => {
	it("counter overflow at MAX_COUNTER — wall advances +1ms, counter resets", () => {
		// Fixed clock at 1_000_000ms — counter must overflow
		const hlc = new HLC(() => 1_000_000);

		let lastTs: HLCTimestamp | undefined;
		const timestamps: HLCTimestamp[] = [];

		// Generate MAX_COUNTER + 2 timestamps to trigger overflow
		for (let i = 0; i <= HLC.MAX_COUNTER + 1; i++) {
			const ts = hlc.now();
			timestamps.push(ts);
			lastTs = ts;
		}

		// Last timestamp should have wall = 1_000_001 (advanced by 1ms)
		const decoded = HLC.decode(lastTs!);
		expect(decoded.wall).toBe(1_000_001);
		expect(decoded.counter).toBe(0);

		// All timestamps are strictly monotonic
		assertMonotonicallyIncreasing(timestamps);
	});

	it("counter overflow via recv() — wall advances +1ms, counter resets", () => {
		const hlc = new HLC(() => 1_000_000);

		// Create a remote timestamp with counter at MAX_COUNTER on same wall
		const remoteTs = HLC.encode(1_000_000, HLC.MAX_COUNTER);
		const result = hlc.recv(remoteTs);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const decoded = HLC.decode(result.value);
			// recv with counter at MAX will try counter = MAX + 1 = 65536 > MAX_COUNTER
			// So it should advance wall by 1ms and reset counter to 0
			expect(decoded.wall).toBe(1_000_001);
			expect(decoded.counter).toBe(0);
		}
	});

	it("clock drift at exactly MAX_DRIFT boundary — 5000ms succeeds, 5001ms fails", () => {
		const hlc = new HLC(() => 1_000_000);

		// Remote exactly at drift boundary: 1_000_000 + 5_000 = 1_005_000
		const atBoundary = HLC.encode(1_000_000 + HLC.MAX_DRIFT_MS, 0);
		const okResult = hlc.recv(atBoundary);
		expect(okResult.ok).toBe(true);

		// Remote 1ms past boundary
		const hlc2 = new HLC(() => 1_000_000);
		const pastBoundary = HLC.encode(1_000_000 + HLC.MAX_DRIFT_MS + 1, 0);
		const errResult = hlc2.recv(pastBoundary);
		expect(errResult.ok).toBe(false);
	});

	it("100K sequential now() calls — all strictly monotonic", () => {
		const N = 100_000;
		// Use a clock that advances slowly (simulating fast calls within same ms)
		let time = 1_000_000;
		const hlc = new HLC(() => {
			// Advance every 100 calls to exercise counter
			if (Math.random() < 0.01) time++;
			return time;
		});

		const timestamps: HLCTimestamp[] = [];
		for (let i = 0; i < N; i++) {
			timestamps.push(hlc.now());
		}

		assertMonotonicallyIncreasing(timestamps);
	});

	it("wall clock regression (goes backwards) — HLC stays monotonic", () => {
		let time = 2_000_000;
		const hlc = new HLC(() => time);

		const ts1 = hlc.now();

		// Move clock backwards by 1 second
		time -= 1_000;
		const ts2 = hlc.now();

		// Move clock backwards by another second
		time -= 1_000;
		const ts3 = hlc.now();

		// All timestamps must still be monotonically increasing
		expect(ts2 > ts1).toBe(true);
		expect(ts3 > ts2).toBe(true);

		// The wall component should NOT have gone backwards
		const d1 = HLC.decode(ts1);
		const d2 = HLC.decode(ts2);
		const d3 = HLC.decode(ts3);
		expect(d2.wall).toBeGreaterThanOrEqual(d1.wall);
		expect(d3.wall).toBeGreaterThanOrEqual(d2.wall);
	});

	it("encode/decode roundtrip at maximum values", () => {
		// Max wall: 2^48 - 1 = 281_474_976_710_655
		const maxWall = 2 ** 48 - 1;
		const maxCounter = HLC.MAX_COUNTER;

		const ts = HLC.encode(maxWall, maxCounter);
		const decoded = HLC.decode(ts);

		expect(decoded.wall).toBe(maxWall);
		expect(decoded.counter).toBe(maxCounter);

		// Minimum values
		const minTs = HLC.encode(0, 0);
		const minDecoded = HLC.decode(minTs);
		expect(minDecoded.wall).toBe(0);
		expect(minDecoded.counter).toBe(0);

		// Typical values
		const typical = HLC.encode(Date.now(), 42);
		const typicalDecoded = HLC.decode(typical);
		expect(typicalDecoded.counter).toBe(42);
	});

	it("compare ordering matches generation order", () => {
		const hlc = new HLC(() => 1_000_000);

		const ts1 = hlc.now();
		const ts2 = hlc.now();
		const ts3 = hlc.now();

		expect(HLC.compare(ts1, ts2)).toBe(-1);
		expect(HLC.compare(ts2, ts3)).toBe(-1);
		expect(HLC.compare(ts1, ts3)).toBe(-1);
		expect(HLC.compare(ts2, ts1)).toBe(1);
		expect(HLC.compare(ts1, ts1)).toBe(0);
	});

	it("recv with remote behind local — local counter increments", () => {
		let time = 2_000_000;
		const hlc = new HLC(() => time);

		// Advance local clock
		const localTs = hlc.now();
		time += 100;

		// Send a remote timestamp that's behind local
		const oldRemote = HLC.encode(1_000_000, 0);
		const result = hlc.recv(oldRemote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// Result should be ahead of localTs
			expect(result.value > localTs).toBe(true);
		}
	});
});
