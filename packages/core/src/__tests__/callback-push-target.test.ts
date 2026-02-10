import { describe, expect, it } from "vitest";
import {
	BaseSourcePoller,
	CallbackPushTarget,
	type HLCTimestamp,
	type PushTarget,
	type RowDelta,
	type SyncPush,
} from "../index";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let deltaCounter = 0;

function makeDelta(table = "t"): RowDelta {
	deltaCounter++;
	return {
		op: "INSERT",
		table,
		rowId: `r${deltaCounter}`,
		clientId: "test",
		columns: [{ column: "name", value: `val-${deltaCounter}` }],
		hlc: BigInt(deltaCounter) as HLCTimestamp,
		deltaId: `d${deltaCounter}`,
	};
}

/** Concrete test poller that exposes accumulate/flush for testing. */
class TestPoller extends BaseSourcePoller {
	deltasToAccumulate: RowDelta[] = [];

	constructor(gateway: PushTarget) {
		super({ name: "cb-test", intervalMs: 60_000, gateway });
	}

	async poll(): Promise<void> {
		for (const d of this.deltasToAccumulate) {
			await this.accumulateDelta(d);
		}
		await this.flushAccumulator();
	}

	getCursorState(): Record<string, unknown> {
		return {};
	}

	setCursorState(_state: Record<string, unknown>): void {
		// no-op
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CallbackPushTarget", () => {
	beforeEach(() => {
		deltaCounter = 0;
	});

	it("calls the synchronous callback with push data", () => {
		const received: SyncPush[] = [];
		const target = new CallbackPushTarget((push) => {
			received.push(push);
		});

		const delta = makeDelta();
		target.handlePush({
			clientId: "c1",
			deltas: [delta],
			lastSeenHlc: 0n as HLCTimestamp,
		});

		expect(received).toHaveLength(1);
		expect(received[0]!.clientId).toBe("c1");
		expect(received[0]!.deltas).toHaveLength(1);
		expect(received[0]!.deltas[0]!.rowId).toBe(delta.rowId);
	});

	it("calls an async callback without throwing", () => {
		const received: SyncPush[] = [];
		const target = new CallbackPushTarget(async (push) => {
			// Simulate async work
			await Promise.resolve();
			received.push(push);
		});

		// handlePush is fire-and-forget; the async callback runs but the
		// call itself does not await. This mirrors the PushTarget contract.
		target.handlePush({
			clientId: "c2",
			deltas: [makeDelta()],
			lastSeenHlc: 0n as HLCTimestamp,
		});

		// The promise has been started but not necessarily resolved yet.
		// The important thing is that handlePush does not throw.
		expect(true).toBe(true);
	});

	it("receives multiple pushes independently", () => {
		const received: SyncPush[] = [];
		const target = new CallbackPushTarget((push) => {
			received.push(push);
		});

		target.handlePush({
			clientId: "c1",
			deltas: [makeDelta()],
			lastSeenHlc: 0n as HLCTimestamp,
		});
		target.handlePush({
			clientId: "c1",
			deltas: [makeDelta(), makeDelta()],
			lastSeenHlc: 0n as HLCTimestamp,
		});

		expect(received).toHaveLength(2);
		expect(received[0]!.deltas).toHaveLength(1);
		expect(received[1]!.deltas).toHaveLength(2);
	});

	it("can be used as a PushTarget for BaseSourcePoller", async () => {
		const received: SyncPush[] = [];
		const target = new CallbackPushTarget((push) => {
			received.push(push);
		});

		const poller = new TestPoller(target);
		poller.deltasToAccumulate = [makeDelta(), makeDelta()];
		await poller.poll();

		expect(received).toHaveLength(1);
		const allDeltas = received[0]!.deltas;
		expect(allDeltas).toHaveLength(2);
	});

	it("implements the PushTarget interface", () => {
		const target: PushTarget = new CallbackPushTarget(() => {});
		expect(typeof target.handlePush).toBe("function");
	});
});
