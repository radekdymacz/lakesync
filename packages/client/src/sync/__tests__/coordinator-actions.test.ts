import type {
	ActionPush,
	ActionResponse,
	HLCTimestamp,
	LakeSyncError,
	Result,
	SyncResponse,
} from "@lakesync/core";
import { HLC, Ok } from "@lakesync/core";
import { describe, expect, it, vi } from "vitest";
import { MemoryActionQueue } from "../../queue/memory-action-queue";
import { MemoryQueue } from "../../queue/memory-queue";
import { SyncCoordinator } from "../coordinator";
import type { ActionTransport, TransportWithCapabilities } from "../transport";

/** Minimal LocalDB stub for testing. */
function createMockDb() {
	return {
		exec: vi.fn(),
		run: vi.fn(),
		getRows: vi.fn().mockReturnValue([]),
		getAllRows: vi.fn().mockReturnValue([]),
		getFirstRow: vi.fn().mockReturnValue(undefined),
		export: vi.fn().mockReturnValue(new Uint8Array()),
		close: vi.fn(),
	};
}

/** Minimal transport that supports executeAction. */
function createMockTransport(
	executeActionFn?: ActionTransport["executeAction"],
): TransportWithCapabilities {
	return {
		async push(): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>> {
			return Ok({ serverHlc: 100n as HLCTimestamp, accepted: 0 });
		},
		async pull(): Promise<Result<SyncResponse, LakeSyncError>> {
			return Ok({ deltas: [], serverHlc: 100n as HLCTimestamp, hasMore: false });
		},
		executeAction: executeActionFn,
	};
}

describe("SyncCoordinator action support", () => {
	it("executeAction pushes to the action queue", async () => {
		const actionQueue = new MemoryActionQueue();
		const transport = createMockTransport(
			async (): Promise<Result<ActionResponse, LakeSyncError>> =>
				Ok({
					results: [],
					serverHlc: 200n as HLCTimestamp,
				}),
		);

		const coordinator = new SyncCoordinator(createMockDb() as never, transport, {
			queue: new MemoryQueue(),
			actionQueue,
			clientId: "client-1",
			hlc: new HLC(),
		});

		await coordinator.executeAction({
			connector: "github",
			actionType: "create_pr",
			params: { title: "Test PR" },
		});

		// Wait for async processing
		await new Promise((resolve) => setTimeout(resolve, 50));

		// The action should have been processed (queue should be empty after ack)
		const depth = await actionQueue.depth();
		expect(depth.ok).toBe(true);
		if (depth.ok) expect(depth.value).toBe(0);
	});

	it("emits onActionComplete event on success", async () => {
		const actionQueue = new MemoryActionQueue();
		const executeActionFn = vi.fn(
			async (msg: ActionPush): Promise<Result<ActionResponse, LakeSyncError>> =>
				Ok({
					results: msg.actions.map((a) => ({
						actionId: a.actionId,
						data: { success: true },
						serverHlc: 300n as HLCTimestamp,
					})),
					serverHlc: 300n as HLCTimestamp,
				}),
		);

		const transport = createMockTransport(executeActionFn);
		const coordinator = new SyncCoordinator(createMockDb() as never, transport, {
			queue: new MemoryQueue(),
			actionQueue,
			clientId: "client-1",
			hlc: new HLC(),
		});

		const completedActions: string[] = [];
		coordinator.on("onActionComplete", (actionId) => {
			completedActions.push(actionId);
		});

		await coordinator.executeAction({
			connector: "slack",
			actionType: "send",
			params: { text: "hello" },
		});

		await new Promise((resolve) => setTimeout(resolve, 50));
		expect(completedActions).toHaveLength(1);
	});

	it("does not crash when no action queue is configured", async () => {
		const transport = createMockTransport();
		const coordinator = new SyncCoordinator(createMockDb() as never, transport, {
			queue: new MemoryQueue(),
			clientId: "client-1",
			hlc: new HLC(),
		});

		// Should not throw, just emit error
		const errors: Error[] = [];
		coordinator.on("onError", (err) => errors.push(err));

		await coordinator.executeAction({
			connector: "github",
			actionType: "create_pr",
			params: {},
		});

		expect(errors).toHaveLength(1);
		expect(errors[0]!.message).toContain("No action queue");
	});

	it("does not process actions when transport has no executeAction", async () => {
		const actionQueue = new MemoryActionQueue();
		const transport = createMockTransport(); // no executeAction
		delete transport.executeAction;

		const coordinator = new SyncCoordinator(createMockDb() as never, transport, {
			queue: new MemoryQueue(),
			actionQueue,
			clientId: "client-1",
			hlc: new HLC(),
		});

		// Manually push an action
		await actionQueue.push({
			actionId: "a1",
			clientId: "client-1",
			hlc: 100n as HLCTimestamp,
			connector: "test",
			actionType: "test",
			params: {},
		});

		// processActionQueue should be a no-op
		await coordinator.processActionQueue();

		const depth = await actionQueue.depth();
		expect(depth.ok).toBe(true);
		if (depth.ok) expect(depth.value).toBe(1); // Still in queue
	});
});
