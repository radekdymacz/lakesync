import type { Action, HLCTimestamp } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { MemoryActionQueue } from "../memory-action-queue";

function createAction(overrides?: Partial<Action>): Action {
	return {
		actionId: `action-${Math.random().toString(36).slice(2)}`,
		clientId: "client-1",
		hlc: 100n as HLCTimestamp,
		connector: "github",
		actionType: "create_pr",
		params: { title: "Fix bug" },
		...overrides,
	};
}

describe("MemoryActionQueue", () => {
	it("pushes and peeks entries", async () => {
		const queue = new MemoryActionQueue();
		const action = createAction();

		const pushResult = await queue.push(action);
		expect(pushResult.ok).toBe(true);

		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(1);
		expect(peekResult.value[0]!.action.actionId).toBe(action.actionId);
		expect(peekResult.value[0]!.status).toBe("pending");
	});

	it("marks entries as sending", async () => {
		const queue = new MemoryActionQueue();
		const action = createAction();
		const pushResult = await queue.push(action);
		if (!pushResult.ok) return;

		await queue.markSending([pushResult.value.id]);

		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		// Sending entries should not appear in peek
		expect(peekResult.value).toHaveLength(0);
	});

	it("acks entries (removes them)", async () => {
		const queue = new MemoryActionQueue();
		const pushResult = await queue.push(createAction());
		if (!pushResult.ok) return;

		await queue.ack([pushResult.value.id]);

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		if (depth.ok) expect(depth.value).toBe(0);
	});

	it("nacks entries with exponential backoff", async () => {
		const queue = new MemoryActionQueue();
		const pushResult = await queue.push(createAction());
		if (!pushResult.ok) return;

		await queue.markSending([pushResult.value.id]);
		await queue.nack([pushResult.value.id]);

		// Entry should be pending again but with a retryAfter in the future
		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		if (depth.ok) expect(depth.value).toBe(1);

		// Peek should return empty (retryAfter is in the future)
		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(0);
	});

	it("reports depth correctly", async () => {
		const queue = new MemoryActionQueue();
		await queue.push(createAction());
		await queue.push(createAction());
		await queue.push(createAction());

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		if (depth.ok) expect(depth.value).toBe(3);
	});

	it("clears all entries", async () => {
		const queue = new MemoryActionQueue();
		await queue.push(createAction());
		await queue.push(createAction());

		await queue.clear();

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		if (depth.ok) expect(depth.value).toBe(0);
	});

	it("respects peek limit", async () => {
		const queue = new MemoryActionQueue();
		for (let i = 0; i < 5; i++) {
			await queue.push(createAction());
		}

		const peekResult = await queue.peek(2);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(2);
	});
});
