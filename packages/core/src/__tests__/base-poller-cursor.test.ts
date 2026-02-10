import { describe, expect, it } from "vitest";
import { BaseSourcePoller, type HLCTimestamp, Ok, type PushTarget, type SyncPush } from "../index";

// ---------------------------------------------------------------------------
// Minimal concrete poller for cursor serialisation tests
// ---------------------------------------------------------------------------

class CursorTestPoller extends BaseSourcePoller {
	lastUpdated: string | null = null;
	offset = 0;

	constructor(gateway: PushTarget) {
		super({ name: "cursor-test", intervalMs: 60_000, gateway });
	}

	async poll(): Promise<void> {
		// no-op for these tests
	}

	getCursorState(): Record<string, unknown> {
		return { lastUpdated: this.lastUpdated, offset: this.offset };
	}

	setCursorState(state: Record<string, unknown>): void {
		this.lastUpdated = (state.lastUpdated as string | null) ?? null;
		this.offset = (state.offset as number) ?? 0;
	}
}

function createPushTarget(): PushTarget {
	return {
		handlePush(push: SyncPush) {
			return Ok({
				serverHlc: 0n as HLCTimestamp,
				accepted: push.deltas.length,
				deltas: push.deltas,
			});
		},
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("BaseSourcePoller — cursor serialisation", () => {
	it("getCursorState returns the current cursor", () => {
		const poller = new CursorTestPoller(createPushTarget());
		poller.lastUpdated = "2026-01-15T00:00:00Z";
		poller.offset = 42;

		const state = poller.getCursorState();
		expect(state).toEqual({ lastUpdated: "2026-01-15T00:00:00Z", offset: 42 });
	});

	it("setCursorState restores a previously exported cursor", () => {
		const poller = new CursorTestPoller(createPushTarget());

		poller.setCursorState({ lastUpdated: "2026-02-01T12:00:00Z", offset: 100 });

		expect(poller.lastUpdated).toBe("2026-02-01T12:00:00Z");
		expect(poller.offset).toBe(100);
	});

	it("round-trips cursor state through get/set", () => {
		const pollerA = new CursorTestPoller(createPushTarget());
		pollerA.lastUpdated = "2026-01-20T08:30:00Z";
		pollerA.offset = 7;

		const snapshot = pollerA.getCursorState();

		const pollerB = new CursorTestPoller(createPushTarget());
		pollerB.setCursorState(snapshot);

		expect(pollerB.getCursorState()).toEqual(snapshot);
		expect(pollerB.lastUpdated).toBe("2026-01-20T08:30:00Z");
		expect(pollerB.offset).toBe(7);
	});

	it("getCursorState returns a copy, not a reference", () => {
		const poller = new CursorTestPoller(createPushTarget());
		poller.lastUpdated = "2026-01-01T00:00:00Z";

		const state = poller.getCursorState();
		state.lastUpdated = "mutated";

		expect(poller.lastUpdated).toBe("2026-01-01T00:00:00Z");
	});

	it("setCursorState does not hold a reference to the input", () => {
		const poller = new CursorTestPoller(createPushTarget());
		const input = { lastUpdated: "2026-01-01T00:00:00Z", offset: 5 };

		poller.setCursorState(input);
		input.offset = 999;

		expect(poller.offset).toBe(5);
	});

	it("handles empty/default cursor state", () => {
		const poller = new CursorTestPoller(createPushTarget());

		const state = poller.getCursorState();
		expect(state).toEqual({ lastUpdated: null, offset: 0 });
	});
});

describe("BaseSourcePoller — pollOnce", () => {
	it("executes a single poll cycle", async () => {
		let pollCount = 0;

		class CountingPoller extends BaseSourcePoller {
			constructor(gateway: PushTarget) {
				super({ name: "counting", intervalMs: 60_000, gateway });
			}

			async poll(): Promise<void> {
				pollCount++;
			}

			getCursorState(): Record<string, unknown> {
				return {};
			}

			setCursorState(_state: Record<string, unknown>): void {
				// no-op
			}
		}

		const poller = new CountingPoller(createPushTarget());
		await poller.pollOnce();

		expect(pollCount).toBe(1);
		// Should not start the timer loop
		expect(poller.isRunning).toBe(false);
	});

	it("can be called multiple times", async () => {
		let pollCount = 0;

		class CountingPoller extends BaseSourcePoller {
			constructor(gateway: PushTarget) {
				super({ name: "counting", intervalMs: 60_000, gateway });
			}

			async poll(): Promise<void> {
				pollCount++;
			}

			getCursorState(): Record<string, unknown> {
				return {};
			}

			setCursorState(_state: Record<string, unknown>): void {
				// no-op
			}
		}

		const poller = new CountingPoller(createPushTarget());
		await poller.pollOnce();
		await poller.pollOnce();
		await poller.pollOnce();

		expect(pollCount).toBe(3);
	});
});
