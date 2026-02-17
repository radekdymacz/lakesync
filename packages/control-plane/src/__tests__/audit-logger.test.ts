import { describe, expect, it } from "vitest";
import { PgAuditLogger } from "../audit/pg-audit-logger";
import type { RecordAuditInput } from "../audit/types";
import { createMockPool } from "./test-helpers";

/** Build a mock audit event row as it would come from Postgres */
function mockAuditRow(overrides: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
	return {
		id: "aud_abc123",
		org_id: "org_abc123",
		actor_id: "user_abc123",
		actor_type: "user",
		action: "gateway.create",
		resource: "gw_abc123",
		metadata: null,
		ip_address: null,
		timestamp: new Date("2025-06-01T12:00:00Z").toISOString(),
		...overrides,
	};
}

describe("PgAuditLogger", () => {
	// ── record ──────────────────────────────────────────────────────

	it("inserts an audit event into the database", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([]); // INSERT returns no rows

		const logger = new PgAuditLogger(pool as never);
		const input: RecordAuditInput = {
			orgId: "org_abc123",
			actorId: "user_abc123",
			actorType: "user",
			action: "gateway.create",
			resource: "gw_abc123",
			metadata: { name: "My Gateway" },
			ipAddress: "192.168.1.1",
		};

		const result = await logger.record(input);

		expect(result.ok).toBe(true);
		expect(mockQuery).toHaveBeenCalledTimes(1);

		const [sql, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(sql).toContain("INSERT INTO audit_events");
		expect(params[1]).toBe("org_abc123");
		expect(params[2]).toBe("user_abc123");
		expect(params[3]).toBe("user");
		expect(params[4]).toBe("gateway.create");
		expect(params[5]).toBe("gw_abc123");
		expect(params[6]).toBe(JSON.stringify({ name: "My Gateway" }));
		expect(params[7]).toBe("192.168.1.1");
	});

	it("handles null metadata and ipAddress", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([]);

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.record({
			orgId: "org_1",
			actorId: "system",
			actorType: "system",
			action: "flush.manual",
			resource: "gw_1",
		});

		expect(result.ok).toBe(true);
		const [, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(params[6]).toBeNull(); // metadata
		expect(params[7]).toBeNull(); // ipAddress
	});

	it("wraps database errors in ControlPlaneError", async () => {
		const { pool, mockQuery } = createMockPool();
		mockQuery.mockRejectedValueOnce(new Error("connection refused"));

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.record({
			orgId: "org_1",
			actorId: "user_1",
			actorType: "user",
			action: "gateway.delete",
			resource: "gw_1",
		});

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toBe("Failed to record audit event");
		}
	});

	// ── query ──────────────────────────────────────────────────────

	it("queries events for an org", async () => {
		const { pool, queueResult } = createMockPool();
		queueResult([
			mockAuditRow({ id: "aud_1", action: "gateway.create" }),
			mockAuditRow({ id: "aud_2", action: "gateway.update" }),
		]);

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.query({ orgId: "org_abc123" });

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.events).toHaveLength(2);
			expect(result.value.hasMore).toBe(false);
			expect(result.value.cursor).toBeUndefined();
		}
	});

	it("applies action filter", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([mockAuditRow({ action: "api_key.create" })]);

		const logger = new PgAuditLogger(pool as never);
		await logger.query({ orgId: "org_abc123", action: "api_key.create" });

		const [sql, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(sql).toContain("action =");
		expect(params).toContain("api_key.create");
	});

	it("applies date range filters", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([]);

		const logger = new PgAuditLogger(pool as never);
		const from = new Date("2025-01-01");
		const to = new Date("2025-06-01");
		await logger.query({ orgId: "org_1", from, to });

		const [sql, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(sql).toContain("timestamp >=");
		expect(sql).toContain("timestamp <=");
		expect(params).toContain(from.toISOString());
		expect(params).toContain(to.toISOString());
	});

	it("supports cursor pagination", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([mockAuditRow({ id: "aud_cursor_next" })]);

		const logger = new PgAuditLogger(pool as never);
		await logger.query({ orgId: "org_1", cursor: "aud_prev" });

		const [sql, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(sql).toContain("id <");
		expect(params).toContain("aud_prev");
	});

	it("detects hasMore when extra row is returned", async () => {
		const { pool, queueResult } = createMockPool();
		// Default limit is 50, so return 51 rows to trigger hasMore
		const rows = Array.from({ length: 51 }, (_, i) => mockAuditRow({ id: `aud_${i}` }));
		queueResult(rows);

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.query({ orgId: "org_1" });

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.events).toHaveLength(50);
			expect(result.value.hasMore).toBe(true);
			expect(result.value.cursor).toBe("aud_49");
		}
	});

	it("respects custom limit", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([]);

		const logger = new PgAuditLogger(pool as never);
		await logger.query({ orgId: "org_1", limit: 10 });

		const [, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		// limit + 1 for hasMore detection
		expect(params[params.length - 1]).toBe(11);
	});

	it("caps limit at 200", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([]);

		const logger = new PgAuditLogger(pool as never);
		await logger.query({ orgId: "org_1", limit: 500 });

		const [, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		// max(200) + 1
		expect(params[params.length - 1]).toBe(201);
	});

	it("applies actorId filter", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([]);

		const logger = new PgAuditLogger(pool as never);
		await logger.query({ orgId: "org_1", actorId: "user_filter" });

		const [sql, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(sql).toContain("actor_id =");
		expect(params).toContain("user_filter");
	});

	// ── deleteOlderThan ────────────────────────────────────────────

	it("deletes events older than the given date", async () => {
		const { pool, mockQuery, queueResult } = createMockPool();
		queueResult([], 5); // 5 rows deleted

		const logger = new PgAuditLogger(pool as never);
		const before = new Date("2025-01-01");
		const result = await logger.deleteOlderThan("org_1", before);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBe(5);
		}

		const [sql, params] = mockQuery.mock.calls[0] as [string, unknown[]];
		expect(sql).toContain("DELETE FROM audit_events");
		expect(params[0]).toBe("org_1");
		expect(params[1]).toBe(before.toISOString());
	});

	it("returns 0 when no old events exist", async () => {
		const { pool, queueResult } = createMockPool();
		queueResult([], 0);

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.deleteOlderThan("org_1", new Date());

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBe(0);
		}
	});

	// ── row mapping ────────────────────────────────────────────────

	it("correctly maps metadata from JSONB", async () => {
		const { pool, queueResult } = createMockPool();
		queueResult([
			mockAuditRow({
				metadata: { region: "eu-west-1", oldName: "Old", newName: "New" },
			}),
		]);

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.query({ orgId: "org_1" });

		expect(result.ok).toBe(true);
		if (result.ok) {
			const event = result.value.events[0];
			expect(event).toBeDefined();
			expect(event!.metadata).toEqual({
				region: "eu-west-1",
				oldName: "Old",
				newName: "New",
			});
		}
	});

	it("maps undefined for null metadata", async () => {
		const { pool, queueResult } = createMockPool();
		queueResult([mockAuditRow({ metadata: null })]);

		const logger = new PgAuditLogger(pool as never);
		const result = await logger.query({ orgId: "org_1" });

		expect(result.ok).toBe(true);
		if (result.ok) {
			const event = result.value.events[0];
			expect(event).toBeDefined();
			expect(event!.metadata).toBeUndefined();
		}
	});
});
