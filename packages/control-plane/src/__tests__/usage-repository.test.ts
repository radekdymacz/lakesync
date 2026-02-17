import type { UsageAggregate } from "@lakesync/core";
import type { Pool } from "pg";
import { beforeEach, describe, expect, it } from "vitest";
import { PgUsageRepository } from "../postgres/usage-repository";
import { createMockPool } from "./test-helpers";

function mockUsageRow(overrides: Partial<Record<string, unknown>> = {}): Record<string, unknown> {
	return {
		gateway_id: "gw-1",
		org_id: "org-1",
		event_type: "push_deltas",
		count: "42",
		window_start: new Date("2026-01-15T10:30:00Z").toISOString(),
		...overrides,
	};
}

describe("PgUsageRepository", () => {
	let repo: PgUsageRepository;
	let mock: ReturnType<typeof createMockPool>;

	beforeEach(() => {
		mock = createMockPool();
		repo = new PgUsageRepository(mock.pool as unknown as Pool);
	});

	describe("recordAggregates", () => {
		it("inserts aggregates with ON CONFLICT upsert", async () => {
			mock.queueResult([]);

			const aggregates: UsageAggregate[] = [
				{
					gatewayId: "gw-1",
					orgId: "org-1",
					eventType: "push_deltas",
					count: 10,
					windowStart: new Date("2026-01-15T10:30:00Z"),
				},
			];

			const result = await repo.recordAggregates(aggregates);
			expect(result.ok).toBe(true);
			expect(mock.mockQuery).toHaveBeenCalledOnce();

			const sql = mock.mockQuery.mock.calls[0]![0] as string;
			expect(sql).toContain("INSERT INTO usage_events");
			expect(sql).toContain("ON CONFLICT");
			expect(sql).toContain("DO UPDATE SET count = usage_events.count + EXCLUDED.count");
		});

		it("handles multiple aggregates in a single batch", async () => {
			mock.queueResult([]);

			const aggregates: UsageAggregate[] = [
				{
					gatewayId: "gw-1",
					orgId: "org-1",
					eventType: "push_deltas",
					count: 10,
					windowStart: new Date("2026-01-15T10:30:00Z"),
				},
				{
					gatewayId: "gw-1",
					orgId: "org-1",
					eventType: "pull_deltas",
					count: 5,
					windowStart: new Date("2026-01-15T10:30:00Z"),
				},
			];

			const result = await repo.recordAggregates(aggregates);
			expect(result.ok).toBe(true);

			// Should have 10 parameter placeholders (5 per aggregate)
			const params = mock.mockQuery.mock.calls[0]![1] as unknown[];
			expect(params).toHaveLength(10);
		});

		it("returns Ok immediately for empty aggregates", async () => {
			const result = await repo.recordAggregates([]);
			expect(result.ok).toBe(true);
			expect(mock.mockQuery).not.toHaveBeenCalled();
		});

		it("passes null for missing orgId", async () => {
			mock.queueResult([]);

			const aggregates: UsageAggregate[] = [
				{
					gatewayId: "gw-1",
					eventType: "flush_bytes",
					count: 1024,
					windowStart: new Date("2026-01-15T10:30:00Z"),
				},
			];

			const result = await repo.recordAggregates(aggregates);
			expect(result.ok).toBe(true);

			const params = mock.mockQuery.mock.calls[0]![1] as unknown[];
			// orgId is the second parameter
			expect(params[1]).toBeNull();
		});

		it("returns Err on database failure", async () => {
			mock.mockQuery.mockRejectedValueOnce(new Error("connection lost"));

			const aggregates: UsageAggregate[] = [
				{
					gatewayId: "gw-1",
					eventType: "push_deltas",
					count: 1,
					windowStart: new Date("2026-01-15T10:30:00Z"),
				},
			];

			const result = await repo.recordAggregates(aggregates);
			expect(result.ok).toBe(false);
		});
	});

	describe("queryUsage", () => {
		it("returns usage rows for an org within time range", async () => {
			mock.queueResult([
				mockUsageRow({ count: "100" }),
				mockUsageRow({ event_type: "pull_deltas", count: "50" }),
			]);

			const result = await repo.queryUsage({
				orgId: "org-1",
				from: new Date("2026-01-15T00:00:00Z"),
				to: new Date("2026-01-16T00:00:00Z"),
			});

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
				expect(result.value[0]!.count).toBe(100);
				expect(result.value[0]!.gatewayId).toBe("gw-1");
				expect(result.value[1]!.eventType).toBe("pull_deltas");
			}
		});

		it("filters by event type when specified", async () => {
			mock.queueResult([mockUsageRow()]);

			await repo.queryUsage({
				orgId: "org-1",
				from: new Date("2026-01-15T00:00:00Z"),
				to: new Date("2026-01-16T00:00:00Z"),
				eventType: "push_deltas",
			});

			const sql = mock.mockQuery.mock.calls[0]![0] as string;
			expect(sql).toContain("event_type = $4");
			const params = mock.mockQuery.mock.calls[0]![1] as unknown[];
			expect(params[3]).toBe("push_deltas");
		});

		it("returns empty array when no rows match", async () => {
			mock.queueResult([]);

			const result = await repo.queryUsage({
				orgId: "org-1",
				from: new Date("2026-01-15T00:00:00Z"),
				to: new Date("2026-01-16T00:00:00Z"),
			});

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(0);
			}
		});
	});

	describe("queryGatewayUsage", () => {
		it("returns hourly aggregates grouped by gateway", async () => {
			mock.queueResult([
				mockUsageRow({ gateway_id: "gw-1", count: "200" }),
				mockUsageRow({ gateway_id: "gw-2", count: "150" }),
			]);

			const result = await repo.queryGatewayUsage({
				orgId: "org-1",
				from: new Date("2026-01-15T00:00:00Z"),
				to: new Date("2026-01-16T00:00:00Z"),
			});

			expect(result.ok).toBe(true);
			if (result.ok) {
				expect(result.value).toHaveLength(2);
				expect(result.value[0]!.gatewayId).toBe("gw-1");
				expect(result.value[1]!.gatewayId).toBe("gw-2");
			}

			const sql = mock.mockQuery.mock.calls[0]![0] as string;
			expect(sql).toContain("date_trunc('hour'");
		});

		it("filters by event type when specified", async () => {
			mock.queueResult([]);

			await repo.queryGatewayUsage({
				orgId: "org-1",
				from: new Date("2026-01-15T00:00:00Z"),
				to: new Date("2026-01-16T00:00:00Z"),
				eventType: "flush_bytes",
			});

			const params = mock.mockQuery.mock.calls[0]![1] as unknown[];
			expect(params[3]).toBe("flush_bytes");
		});
	});
});
