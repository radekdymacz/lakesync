import { describe, expect, it } from "vitest";
import { HLC } from "../hlc";
import type { HLCTimestamp } from "../types";
import { ClockDriftError } from "../../result";

describe("HLC", () => {
	it("now() returns monotonically increasing timestamps", () => {
		let time = 1_000_000;
		const clock = new HLC(() => time++);

		const a = clock.now();
		const b = clock.now();
		const c = clock.now();

		expect(a < b).toBe(true);
		expect(b < c).toBe(true);
	});

	it("now() increments logical counter when wall clock is unchanged", () => {
		const fixedTime = 1_000_000;
		const clock = new HLC(() => fixedTime);

		const a = clock.now();
		const b = clock.now();
		const c = clock.now();

		const da = HLC.decode(a);
		const db = HLC.decode(b);
		const dc = HLC.decode(c);

		// Wall should remain the same
		expect(da.wall).toBe(fixedTime);
		expect(db.wall).toBe(fixedTime);
		expect(dc.wall).toBe(fixedTime);

		// Counter should increment
		expect(da.counter).toBe(0);
		expect(db.counter).toBe(1);
		expect(dc.counter).toBe(2);
	});

	it("now() absorbs backward clock jump", () => {
		let time = 1_000_000;
		const clock = new HLC(() => time);

		const a = clock.now();
		// Jump backward by 500ms
		time = 999_500;
		const b = clock.now();

		expect(b > a).toBe(true);

		const da = HLC.decode(a);
		const db = HLC.decode(b);

		// Wall should not go backward
		expect(db.wall).toBeGreaterThanOrEqual(da.wall);
	});

	it("recv() with valid remote returns Ok with advanced local timestamp", () => {
		const localTime = 1_000_000;
		const clock = new HLC(() => localTime);

		// Generate an initial local timestamp
		const local = clock.now();

		// Create a remote timestamp slightly in the future (within drift)
		const remote = HLC.encode(localTime + 100, 5);

		const result = clock.recv(remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// The new local timestamp must be greater than both the old local and the remote
			expect(result.value > local).toBe(true);
			expect(result.value > remote).toBe(true);
		}
	});

	it("recv() returns Err(ClockDriftError) when remote is >5s ahead", () => {
		const localTime = 1_000_000;
		const clock = new HLC(() => localTime);

		// Remote is 6 seconds ahead — exceeds MAX_DRIFT_MS
		const remote = HLC.encode(localTime + 6_000, 0);

		const result = clock.recv(remote);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(ClockDriftError);
			expect(result.error.code).toBe("CLOCK_DRIFT");
		}
	});

	it("recv() with past timestamp still advances the local clock", () => {
		const localTime = 1_000_000;
		const clock = new HLC(() => localTime);

		const before = clock.now();

		// Remote is in the past
		const remote = HLC.encode(localTime - 500, 3);

		const result = clock.recv(remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// Must still be strictly greater than what we had before
			expect(result.value > before).toBe(true);
		}
	});

	it("encode()/decode() roundtrip preserves edge values", () => {
		const cases: Array<{ wall: number; counter: number }> = [
			{ wall: 0, counter: 0 },
			{ wall: 0, counter: 65535 },
			{ wall: 281474976710655, counter: 0 }, // 2^48 - 1 (max 48-bit)
			{ wall: 281474976710655, counter: 65535 }, // max wall + max counter
			{ wall: 1_700_000_000_000, counter: 42 }, // realistic epoch ms
		];

		for (const { wall, counter } of cases) {
			const encoded = HLC.encode(wall, counter);
			const decoded = HLC.decode(encoded);
			expect(decoded.wall).toBe(wall);
			expect(decoded.counter).toBe(counter);
		}
	});

	it("compare() orders timestamps correctly", () => {
		const a = HLC.encode(100, 0);
		const b = HLC.encode(100, 1);
		const c = HLC.encode(200, 0);
		const d = HLC.encode(100, 0);

		expect(HLC.compare(a, b)).toBe(-1);
		expect(HLC.compare(b, a)).toBe(1);
		expect(HLC.compare(a, c)).toBe(-1);
		expect(HLC.compare(c, a)).toBe(1);
		expect(HLC.compare(a, d)).toBe(0);
	});

	it("counter overflow advances wall by 1ms and resets counter", () => {
		const fixedTime = 1_000_000;
		const clock = new HLC(() => fixedTime);

		// Call now() 65536 times to exhaust the 16-bit counter (0..65535)
		let last: HLCTimestamp = clock.now(); // counter=0
		for (let i = 1; i <= HLC.MAX_COUNTER; i++) {
			last = clock.now();
		}

		// At this point counter should be 65535
		const beforeOverflow = HLC.decode(last);
		expect(beforeOverflow.wall).toBe(fixedTime);
		expect(beforeOverflow.counter).toBe(HLC.MAX_COUNTER);

		// One more call should overflow: wall advances, counter resets
		const overflowed = clock.now();
		const after = HLC.decode(overflowed);

		expect(after.wall).toBe(fixedTime + 1);
		expect(after.counter).toBe(0);

		// And it must still be monotonically increasing
		expect(overflowed > last).toBe(true);
	});
});
