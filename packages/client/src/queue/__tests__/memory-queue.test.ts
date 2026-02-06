import { HLC } from "@lakesync/core";
import type { RowDelta } from "@lakesync/core";
import { beforeEach, describe, expect, it } from "vitest";
import { MemoryQueue } from "../memory-queue";

function makeDelta(id: string): RowDelta {
	return {
		op: "UPDATE",
		table: "todos",
		rowId: id,
		clientId: "test-client",
		columns: [{ column: "title", value: "Test" }],
		hlc: HLC.encode(Date.now(), 0),
		deltaId: `delta-${id}`,
	};
}

describe("MemoryQueue", () => {
	let queue: MemoryQueue;

	beforeEach(() => {
		queue = new MemoryQueue();
	});

	it("push creates entry with status pending", async () => {
		const result = await queue.push(makeDelta("1"));

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value.status).toBe("pending");
		expect(result.value.id).toMatch(/^mem-/);
		expect(result.value.retryCount).toBe(0);
		expect(result.value.delta.rowId).toBe("1");
	});

	it("peek returns entries ordered by createdAt", async () => {
		// Push with slight time gaps to ensure ordering
		await queue.push(makeDelta("1"));
		await queue.push(makeDelta("2"));
		await queue.push(makeDelta("3"));

		const result = await queue.peek(10);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toHaveLength(3);
		for (let i = 1; i < result.value.length; i++) {
			const prev = result.value[i - 1];
			const curr = result.value[i];
			if (prev && curr) {
				expect(prev.createdAt).toBeLessThanOrEqual(curr.createdAt);
			}
		}
	});

	it("peek respects limit", async () => {
		await queue.push(makeDelta("1"));
		await queue.push(makeDelta("2"));
		await queue.push(makeDelta("3"));

		const result = await queue.peek(2);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toHaveLength(2);
	});

	it("peek only returns pending entries", async () => {
		await queue.push(makeDelta("1"));
		await queue.push(makeDelta("2"));
		await queue.push(makeDelta("3"));

		// Mark first entry as sending
		const peekResult = await queue.peek(1);
		if (!peekResult.ok) return;
		const firstId = peekResult.value[0]?.id;
		if (firstId) {
			await queue.markSending([firstId]);
		}

		const result = await queue.peek(10);

		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.value).toHaveLength(2);
		for (const entry of result.value) {
			expect(entry.status).toBe("pending");
		}
	});

	it("markSending transitions status from pending to sending", async () => {
		const pushResult = await queue.push(makeDelta("1"));
		if (!pushResult.ok) return;

		const markResult = await queue.markSending([pushResult.value.id]);
		expect(markResult.ok).toBe(true);

		// Entry should no longer appear in peek (pending only)
		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(0);
	});

	it("ack removes entries from the queue", async () => {
		const r1 = await queue.push(makeDelta("1"));
		const r2 = await queue.push(makeDelta("2"));
		if (!r1.ok || !r2.ok) return;

		await queue.ack([r1.value.id]);

		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(1);
	});

	it("nack resets to pending and increments retryCount", async () => {
		const pushResult = await queue.push(makeDelta("1"));
		if (!pushResult.ok) return;

		await queue.markSending([pushResult.value.id]);
		await queue.nack([pushResult.value.id]);

		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		if (!peekResult.ok) return;
		expect(peekResult.value).toHaveLength(1);

		const entry = peekResult.value[0];
		expect(entry?.status).toBe("pending");
		expect(entry?.retryCount).toBe(1);
	});

	it("depth returns correct count of pending + sending entries", async () => {
		await queue.push(makeDelta("1"));
		await queue.push(makeDelta("2"));
		await queue.push(makeDelta("3"));

		const peekResult = await queue.peek(1);
		if (!peekResult.ok) return;
		const firstId = peekResult.value[0]?.id;
		if (firstId) {
			await queue.markSending([firstId]);
		}

		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		// 2 pending + 1 sending = 3
		expect(depthResult.value).toBe(3);
	});

	it("clear empties the queue", async () => {
		await queue.push(makeDelta("1"));
		await queue.push(makeDelta("2"));

		await queue.clear();

		const depthResult = await queue.depth();
		expect(depthResult.ok).toBe(true);
		if (!depthResult.ok) return;
		expect(depthResult.value).toBe(0);
	});

	it("concurrent peek + markSending prevents double-processing", async () => {
		// Push 3 entries
		await queue.push(makeDelta("1"));
		await queue.push(makeDelta("2"));
		await queue.push(makeDelta("3"));

		// First consumer peeks 2, then marks them as sending
		const firstPeek = await queue.peek(2);
		expect(firstPeek.ok).toBe(true);
		if (!firstPeek.ok) return;
		expect(firstPeek.value).toHaveLength(2);

		const firstIds = firstPeek.value.map((e) => e.id);
		await queue.markSending(firstIds);

		// Second consumer peeks 2, but only 1 remains pending
		const secondPeek = await queue.peek(2);
		expect(secondPeek.ok).toBe(true);
		if (!secondPeek.ok) return;
		expect(secondPeek.value).toHaveLength(1);

		// The remaining entry should not overlap with the first batch
		const secondIds = secondPeek.value.map((e) => e.id);
		for (const id of secondIds) {
			expect(firstIds).not.toContain(id);
		}
	});
});
