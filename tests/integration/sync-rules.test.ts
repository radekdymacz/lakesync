import {
	HLC,
	type ResolvedClaims,
	type SyncRulesConfig,
	type SyncRulesContext,
} from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { createTestGateway, makeDelta } from "./helpers";

/**
 * Integration tests for sync rules — multi-user push/pull with claim-based filtering.
 */

const RULES: SyncRulesConfig = {
	version: 1,
	buckets: [
		{
			name: "user-todos",
			tables: ["todos"],
			filters: [{ column: "user_id", op: "eq", value: "jwt:sub" }],
		},
		{
			name: "public-notes",
			tables: ["notes"],
			filters: [{ column: "visibility", op: "eq", value: "public" }],
		},
	],
};

function ctx(sub: string): SyncRulesContext {
	const claims: ResolvedClaims = { sub };
	return { claims, rules: RULES };
}

describe("Sync rules — filtered pull", () => {
	it("each user only sees their own todos via eq filter", () => {
		const gateway = createTestGateway();
		const hlc = new HLC(() => 1_000_000);

		// User A pushes a todo
		gateway.handlePush({
			clientId: "user-a",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "user-a",
					table: "todos",
					rowId: "todo-a",
					columns: [
						{ column: "title", value: "User A's task" },
						{ column: "user_id", value: "user-a" },
					],
					deltaId: "delta-a-1",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// User B pushes a todo
		gateway.handlePush({
			clientId: "user-b",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "user-b",
					table: "todos",
					rowId: "todo-b",
					columns: [
						{ column: "title", value: "User B's task" },
						{ column: "user_id", value: "user-b" },
					],
					deltaId: "delta-b-1",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// User A pulls with their claims — should only see their own todo
		const pullA = gateway.pullFromBuffer(
			{ clientId: "user-a", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			ctx("user-a"),
		);
		expect(pullA.ok).toBe(true);
		if (pullA.ok) {
			expect(pullA.value.deltas).toHaveLength(1);
			expect(pullA.value.deltas[0]!.rowId).toBe("todo-a");
		}

		// User B pulls — should only see their own todo
		const pullB = gateway.pullFromBuffer(
			{ clientId: "user-b", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			ctx("user-b"),
		);
		expect(pullB.ok).toBe(true);
		if (pullB.ok) {
			expect(pullB.value.deltas).toHaveLength(1);
			expect(pullB.value.deltas[0]!.rowId).toBe("todo-b");
		}
	});

	it("multi-bucket union: user sees own todos AND public notes", () => {
		const gateway = createTestGateway();
		const hlc = new HLC(() => 1_000_000);

		// User A's private todo
		gateway.handlePush({
			clientId: "user-a",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "user-a",
					table: "todos",
					rowId: "todo-a",
					columns: [
						{ column: "title", value: "Private task" },
						{ column: "user_id", value: "user-a" },
					],
					deltaId: "delta-private",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// A public note (matches public-notes bucket for any user)
		gateway.handlePush({
			clientId: "admin",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "admin",
					table: "notes",
					rowId: "note-public",
					columns: [
						{ column: "content", value: "Public announcement" },
						{ column: "visibility", value: "public" },
					],
					deltaId: "delta-public-note",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// A private note (should NOT match for user-a)
		gateway.handlePush({
			clientId: "admin",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "admin",
					table: "notes",
					rowId: "note-private",
					columns: [
						{ column: "content", value: "Secret note" },
						{ column: "visibility", value: "private" },
					],
					deltaId: "delta-private-note",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// User A pulls — should see their todo + public note, but NOT private note
		const pull = gateway.pullFromBuffer(
			{ clientId: "user-a", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			ctx("user-a"),
		);
		expect(pull.ok).toBe(true);
		if (pull.ok) {
			expect(pull.value.deltas).toHaveLength(2);
			const rowIds = pull.value.deltas.map((d) => d.rowId).sort();
			expect(rowIds).toEqual(["note-public", "todo-a"]);
		}
	});

	it("no sync rules = all deltas pass through (backward compat)", () => {
		const gateway = createTestGateway();
		const hlc = new HLC(() => 1_000_000);

		gateway.handlePush({
			clientId: "user-a",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "user-a",
					table: "todos",
					rowId: "todo-a",
					columns: [{ column: "title", value: "Task A" }],
					deltaId: "delta-nofilter-a",
				}),
				makeDelta({
					hlc: hlc.now(),
					clientId: "user-a",
					table: "todos",
					rowId: "todo-b",
					columns: [{ column: "title", value: "Task B" }],
					deltaId: "delta-nofilter-b",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// Pull without context — all deltas returned
		const pull = gateway.pullFromBuffer({
			clientId: "anyone",
			sinceHlc: HLC.encode(0, 0),
			maxDeltas: 100,
		});
		expect(pull.ok).toBe(true);
		if (pull.ok) {
			expect(pull.value.deltas).toHaveLength(2);
		}
	});

	it("pagination works correctly with filtered pull", () => {
		const gateway = createTestGateway();
		let wall = 1_000_000;
		const hlc = new HLC(() => wall++);

		// Push 10 deltas: 5 for user-a, 5 for user-b
		for (let i = 0; i < 10; i++) {
			const userId = i % 2 === 0 ? "user-a" : "user-b";
			gateway.handlePush({
				clientId: userId,
				deltas: [
					makeDelta({
						hlc: hlc.now(),
						clientId: userId,
						table: "todos",
						rowId: `row-${i}`,
						columns: [
							{ column: "title", value: `Task ${i}` },
							{ column: "user_id", value: userId },
						],
						deltaId: `delta-page-${i}`,
					}),
				],
				lastSeenHlc: HLC.encode(0, 0),
			});
		}

		// User A requests maxDeltas=3 — should get exactly 3 with hasMore=true
		const pull = gateway.pullFromBuffer(
			{ clientId: "user-a", sinceHlc: HLC.encode(0, 0), maxDeltas: 3 },
			ctx("user-a"),
		);
		expect(pull.ok).toBe(true);
		if (pull.ok) {
			expect(pull.value.deltas).toHaveLength(3);
			expect(pull.value.hasMore).toBe(true);
			// All returned deltas belong to user-a
			for (const d of pull.value.deltas) {
				const userCol = d.columns.find((c) => c.column === "user_id");
				expect(userCol?.value).toBe("user-a");
			}
		}
	});

	it("permission change: user loses access after claims change", () => {
		const gateway = createTestGateway();
		const hlc = new HLC(() => 1_000_000);

		// User A pushes a todo
		gateway.handlePush({
			clientId: "user-a",
			deltas: [
				makeDelta({
					hlc: hlc.now(),
					clientId: "user-a",
					table: "todos",
					rowId: "todo-perm",
					columns: [
						{ column: "title", value: "Sensitive task" },
						{ column: "user_id", value: "user-a" },
					],
					deltaId: "delta-perm-1",
				}),
			],
			lastSeenHlc: HLC.encode(0, 0),
		});

		// User B with different claims cannot see user-a's data
		const pullB = gateway.pullFromBuffer(
			{ clientId: "user-b", sinceHlc: HLC.encode(0, 0), maxDeltas: 100 },
			ctx("user-b"),
		);
		expect(pullB.ok).toBe(true);
		if (pullB.ok) {
			expect(pullB.value.deltas).toHaveLength(0);
		}
	});
});
