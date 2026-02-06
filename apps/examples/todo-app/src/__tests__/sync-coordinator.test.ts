import type { LakeAdapter } from "@lakesync/adapter";
import { LocalDB, MemoryQueue, registerSchema } from "@lakesync/client";
import { HLC } from "@lakesync/core";
import { SyncGateway } from "@lakesync/gateway";
import { createMockAdapter } from "../../../../../tests/integration/helpers";
import type { Todo } from "../db";
import { todoSchema } from "../db";
import { SyncCoordinator } from "../sync";

/**
 * Create a monotonic clock shared across all HLC instances in a test.
 * Each call to the clock function returns a value 10ms ahead,
 * ensuring strictly increasing HLC timestamps regardless of real time.
 */
function createSharedClock(start = Date.now()) {
	let now = start;
	return () => {
		now += 10;
		return now;
	};
}

/**
 * Create a test gateway with a shared clock.
 * The gateway's internal HLC uses the same clock as the coordinators.
 */
function createTestGatewayWithClock(clock: () => number, adapter?: LakeAdapter) {
	const gateway = new SyncGateway(
		{
			gatewayId: "test-gateway",
			maxBufferBytes: 100 * 1024 * 1024,
			maxBufferAgeMs: 60_000,
			flushFormat: "json" as const,
			tableSchema: todoSchema,
		},
		adapter,
	);

	// Replace the gateway's internal HLC with one using our shared clock.
	// The gateway constructs `this.hlc = new HLC()` in its constructor,
	// so we need to override it after construction.
	(gateway as unknown as { hlc: HLC }).hlc = new HLC(clock);

	return gateway;
}

/** Create a fully wired SyncCoordinator for testing. */
async function createTestCoordinator(opts?: {
	clientId?: string;
	adapter?: LakeAdapter;
	gateway?: SyncGateway;
	clock?: () => number;
}) {
	const db = await LocalDB.open({ name: "test", backend: "memory" });
	if (!db.ok) throw new Error("Failed to open test DB");
	await registerSchema(db.value, todoSchema);

	const clock = opts?.clock ?? (() => Date.now());
	const gateway = opts?.gateway ?? createTestGatewayWithClock(clock, opts?.adapter);

	const queue = new MemoryQueue();
	const hlc = new HLC(clock);

	const coordinator = new SyncCoordinator(db.value, gateway, {
		queue,
		hlc,
		clientId: opts?.clientId ?? "test-client",
	});

	return { coordinator, db: db.value, gateway, queue, hlc };
}

/** Query all todos from a coordinator's tracker. */
async function queryTodos(coordinator: SyncCoordinator): Promise<Todo[]> {
	const result = await coordinator.tracker.query<Todo>("SELECT * FROM todos ORDER BY _rowId");
	return result.ok ? result.value : [];
}

// ---------------------------------------------------------------------------
// Group 1: CRUD via SyncTracker
// ---------------------------------------------------------------------------
describe("CRUD via SyncTracker", () => {
	it("insert creates a todo and queues an INSERT delta", async () => {
		const { coordinator, queue } = await createTestCoordinator();
		const tracker = coordinator.tracker;

		const result = await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		expect(result.ok).toBe(true);

		const todos = await queryTodos(coordinator);
		expect(todos).toHaveLength(1);
		expect(todos[0]!.title).toBe("Buy milk");

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		expect(depth.value).toBe(1);
	});

	it("update modifies a todo and queues an UPDATE delta with only changed columns", async () => {
		const { coordinator, queue } = await createTestCoordinator();
		const tracker = coordinator.tracker;

		await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});

		const result = await tracker.update("todos", "row-1", { completed: 1 });
		expect(result.ok).toBe(true);

		const todos = await queryTodos(coordinator);
		expect(todos[0]!.completed).toBe(1);

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		expect(depth.value).toBe(2); // INSERT + UPDATE

		// Peek at the UPDATE delta — should only contain `completed`
		const peekResult = await queue.peek(10);
		expect(peekResult.ok).toBe(true);
		const updateEntry = peekResult.value.find((e) => e.delta.op === "UPDATE");
		expect(updateEntry).toBeDefined();
		expect(updateEntry!.delta.columns).toHaveLength(1);
		expect(updateEntry!.delta.columns[0]!.column).toBe("completed");
	});

	it("delete removes a todo and queues a DELETE delta", async () => {
		const { coordinator, queue } = await createTestCoordinator();
		const tracker = coordinator.tracker;

		await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});

		const result = await tracker.delete("todos", "row-1");
		expect(result.ok).toBe(true);

		const todos = await queryTodos(coordinator);
		expect(todos).toHaveLength(0);

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		expect(depth.value).toBe(2); // INSERT + DELETE
	});

	it("update on non-existent row returns ROW_NOT_FOUND error", async () => {
		const { coordinator } = await createTestCoordinator();
		const result = await coordinator.tracker.update("todos", "nonexistent", {
			title: "x",
		});
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("ROW_NOT_FOUND");
		}
	});

	it("no-op update (same values) skips queue", async () => {
		const { coordinator, queue } = await createTestCoordinator();
		const tracker = coordinator.tracker;

		await tracker.insert("todos", "row-1", {
			title: "Buy milk",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});

		// Update with the exact same values
		await tracker.update("todos", "row-1", { title: "Buy milk" });

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		expect(depth.value).toBe(1); // Only the INSERT
	});
});

// ---------------------------------------------------------------------------
// Group 2: Push/Pull lifecycle
// ---------------------------------------------------------------------------
describe("Push/Pull lifecycle", () => {
	it("push drains queue to gateway", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);
		const { coordinator, queue } = await createTestCoordinator({
			gateway,
			clock,
		});
		const tracker = coordinator.tracker;

		for (let i = 1; i <= 3; i++) {
			await tracker.insert("todos", `row-${i}`, {
				title: `Todo ${i}`,
				completed: 0,
				created_at: "2025-01-01",
				updated_at: "2025-01-01",
			});
		}

		await coordinator.pushToGateway();

		const depth = await queue.depth();
		expect(depth.ok).toBe(true);
		expect(depth.value).toBe(0);
		expect(gateway.bufferStats.logSize).toBe(3);
	});

	it("pull applies remote deltas to local DB", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// A inserts and pushes
		await coordA.coordinator.tracker.insert("todos", "row-1", {
			title: "From A",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls
		const pulled = await coordB.coordinator.pullFromGateway();
		expect(pulled).toBeGreaterThan(0);

		const todos = await queryTodos(coordB.coordinator);
		expect(todos).toHaveLength(1);
		expect(todos[0]!.title).toBe("From A");
	});

	it("pull returns 0 when no new deltas", async () => {
		const { coordinator } = await createTestCoordinator();
		const pulled = await coordinator.pullFromGateway();
		expect(pulled).toBe(0);
	});

	it("push with empty queue is a no-op", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);
		const { coordinator } = await createTestCoordinator({
			gateway,
			clock,
		});
		await coordinator.pushToGateway();
		expect(gateway.bufferStats.logSize).toBe(0);
	});
});

// ---------------------------------------------------------------------------
// Group 3: Multi-client sync
// ---------------------------------------------------------------------------
describe("Multi-client sync", () => {
	it("two clients sync non-conflicting rows through shared gateway", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// A inserts todo-1, pushes
		await coordA.coordinator.tracker.insert("todos", "todo-1", {
			title: "A's todo",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls first (correct sync order: pull-then-push)
		await coordB.coordinator.pullFromGateway();

		// B inserts todo-2, pushes
		await coordB.coordinator.tracker.insert("todos", "todo-2", {
			title: "B's todo",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordB.coordinator.pushToGateway();

		// A pulls → sees todo-2
		await coordA.coordinator.pullFromGateway();

		const todosA = await queryTodos(coordA.coordinator);
		const todosB = await queryTodos(coordB.coordinator);

		expect(todosA).toHaveLength(2);
		expect(todosB).toHaveLength(2);
	});

	it("two clients sync non-conflicting columns on same row", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// A inserts row, pushes
		await coordA.coordinator.tracker.insert("todos", "row-1", {
			title: "Original",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls → gets row
		await coordB.coordinator.pullFromGateway();

		// A updates title, pushes
		await coordA.coordinator.tracker.update("todos", "row-1", {
			title: "Updated by A",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls A's title update, then updates completed, pushes
		await coordB.coordinator.pullFromGateway();
		await coordB.coordinator.tracker.update("todos", "row-1", {
			completed: 1,
		});
		await coordB.coordinator.pushToGateway();

		// A pulls B's completed update
		await coordA.coordinator.pullFromGateway();

		const todosA = await queryTodos(coordA.coordinator);
		const todosB = await queryTodos(coordB.coordinator);

		// Both should have merged columns
		expect(todosA[0]!.title).toBe("Updated by A");
		expect(todosA[0]!.completed).toBe(1);
		expect(todosB[0]!.title).toBe("Updated by A");
		expect(todosB[0]!.completed).toBe(1);
	});
});

// ---------------------------------------------------------------------------
// Group 4: Conflict resolution
// ---------------------------------------------------------------------------
describe("Conflict resolution", () => {
	it("same-column conflict: later HLC wins (via gateway LWW)", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// A inserts row, pushes
		await coordA.coordinator.tracker.insert("todos", "row-1", {
			title: "Original",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls, gets row
		await coordB.coordinator.pullFromGateway();

		// A updates title first (earlier HLC), pushes
		await coordA.coordinator.tracker.update("todos", "row-1", {
			title: "A's version",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls A's update first, then updates same column (later HLC), pushes
		await coordB.coordinator.pullFromGateway();
		await coordB.coordinator.tracker.update("todos", "row-1", {
			title: "B's version",
		});
		await coordB.coordinator.pushToGateway();

		// A pulls → gateway resolved via LWW, B's version wins (later HLC)
		await coordA.coordinator.pullFromGateway();

		const todosA = await queryTodos(coordA.coordinator);
		expect(todosA[0]!.title).toBe("B's version");
	});

	it("conflict with pending local delta: remote wins if later HLC", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// A inserts, pushes. B pulls.
		await coordA.coordinator.tracker.insert("todos", "row-1", {
			title: "Original",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();
		await coordB.coordinator.pullFromGateway();

		// B updates title locally (queued but not pushed)
		await coordB.coordinator.tracker.update("todos", "row-1", {
			title: "B's local version",
		});

		// A updates same title LATER (higher HLC from shared clock), pushes
		await coordA.coordinator.tracker.update("todos", "row-1", {
			title: "A's remote version",
		});
		await coordA.coordinator.pushToGateway();

		// B pulls → A's update is remote with later HLC. applyRemoteDeltas
		// resolves the conflict: remote (A) wins, local delta acked.
		await coordB.coordinator.pullFromGateway();

		const todosB = await queryTodos(coordB.coordinator);
		expect(todosB[0]!.title).toBe("A's remote version");
	});

	it("conflict with pending local delta: local wins if later HLC", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// A inserts, pushes. B pulls.
		await coordA.coordinator.tracker.insert("todos", "row-1", {
			title: "Original",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();
		await coordB.coordinator.pullFromGateway();

		// A updates title first (earlier HLC), pushes
		await coordA.coordinator.tracker.update("todos", "row-1", {
			title: "A's remote version",
		});
		await coordA.coordinator.pushToGateway();

		// B updates same title LATER (higher HLC from shared clock), queued
		await coordB.coordinator.tracker.update("todos", "row-1", {
			title: "B's local version",
		});

		// B pulls → A's remote update has earlier HLC than B's local delta.
		// applyRemoteDeltas resolves: local (B) wins, remote skipped.
		await coordB.coordinator.pullFromGateway();

		const todosB = await queryTodos(coordB.coordinator);
		expect(todosB[0]!.title).toBe("B's local version");
	});
});

// ---------------------------------------------------------------------------
// Group 5: Flush to storage
// ---------------------------------------------------------------------------
describe("Flush to storage", () => {
	it("flush writes to mock adapter", async () => {
		const adapter = createMockAdapter();
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock, adapter);
		const { coordinator } = await createTestCoordinator({
			gateway,
			clock,
		});

		await coordinator.tracker.insert("todos", "row-1", {
			title: "Flush me",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordinator.pushToGateway();

		const result = await coordinator.flush();
		expect(result.ok).toBe(true);

		expect(adapter.stored.size).toBe(1);
		expect(coordinator.stats.logSize).toBe(0);
	});

	it("flush with no adapter returns error", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);
		const { coordinator } = await createTestCoordinator({
			gateway,
			clock,
		});

		await coordinator.tracker.insert("todos", "row-1", {
			title: "Flush me",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordinator.pushToGateway();

		const result = await coordinator.flush();
		expect(result.ok).toBe(false);
		expect(result.message).toContain("No adapter configured");
	});
});

// ---------------------------------------------------------------------------
// Group 6: Stats and metadata
// ---------------------------------------------------------------------------
describe("Stats and metadata", () => {
	it("bufferStats reflects gateway state", async () => {
		const adapter = createMockAdapter();
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock, adapter);
		const { coordinator } = await createTestCoordinator({
			gateway,
			clock,
		});

		for (let i = 1; i <= 5; i++) {
			await coordinator.tracker.insert("todos", `row-${i}`, {
				title: `Todo ${i}`,
				completed: 0,
				created_at: "2025-01-01",
				updated_at: "2025-01-01",
			});
		}

		await coordinator.pushToGateway();
		expect(coordinator.stats.logSize).toBe(5);

		await coordinator.flush();
		expect(coordinator.stats.logSize).toBe(0);
	});

	it("queueDepth reflects pending entries", async () => {
		const { coordinator } = await createTestCoordinator();

		for (let i = 1; i <= 3; i++) {
			await coordinator.tracker.insert("todos", `row-${i}`, {
				title: `Todo ${i}`,
				completed: 0,
				created_at: "2025-01-01",
				updated_at: "2025-01-01",
			});
		}

		expect(await coordinator.queueDepth()).toBe(3);

		await coordinator.pushToGateway();
		expect(await coordinator.queueDepth()).toBe(0);
	});

	it("lastSyncTime updates after successful push/pull", async () => {
		const clock = createSharedClock();
		const gateway = createTestGatewayWithClock(clock);

		const coordA = await createTestCoordinator({
			clientId: "client-a",
			gateway,
			clock,
		});
		const coordB = await createTestCoordinator({
			clientId: "client-b",
			gateway,
			clock,
		});

		// Initially null
		expect(coordA.coordinator.lastSyncTime).toBeNull();

		// After push → non-null
		await coordA.coordinator.tracker.insert("todos", "row-1", {
			title: "Sync me",
			completed: 0,
			created_at: "2025-01-01",
			updated_at: "2025-01-01",
		});
		await coordA.coordinator.pushToGateway();
		const afterPush = coordA.coordinator.lastSyncTime;
		expect(afterPush).toBeInstanceOf(Date);

		// After pull with deltas → updated
		const pulled = await coordB.coordinator.pullFromGateway();
		expect(pulled).toBeGreaterThan(0);
		expect(coordB.coordinator.lastSyncTime).toBeInstanceOf(Date);
	});
});
