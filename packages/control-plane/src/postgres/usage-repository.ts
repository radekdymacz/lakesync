import type { Result, UsageAggregate } from "@lakesync/core";
import type { Pool } from "pg";
import type { ControlPlaneError } from "../errors";
import { wrapControlPlane } from "../errors";
import type { UsageQuery, UsageRepository, UsageRow } from "../repositories";

/**
 * Postgres-backed usage repository.
 *
 * Stores per-minute usage aggregates in a `usage_events` table and provides
 * query methods for billing and dashboard views.
 */
export class PgUsageRepository implements UsageRepository {
	constructor(private readonly pool: Pool) {}

	async recordAggregates(aggregates: UsageAggregate[]): Promise<Result<void, ControlPlaneError>> {
		if (aggregates.length === 0) {
			return { ok: true, value: undefined };
		}

		return wrapControlPlane(async () => {
			// Build a multi-row INSERT with ON CONFLICT to accumulate counts
			const values: unknown[] = [];
			const placeholders: string[] = [];
			let idx = 1;

			for (const agg of aggregates) {
				placeholders.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`);
				values.push(agg.gatewayId, agg.orgId ?? null, agg.eventType, agg.count, agg.windowStart);
			}

			await this.pool.query(
				`INSERT INTO usage_events (gateway_id, org_id, event_type, count, window_start)
				 VALUES ${placeholders.join(", ")}
				 ON CONFLICT (gateway_id, event_type, window_start)
				 DO UPDATE SET count = usage_events.count + EXCLUDED.count`,
				values,
			);
		}, "Failed to record usage aggregates");
	}

	async queryUsage(query: UsageQuery): Promise<Result<UsageRow[], ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const params: unknown[] = [query.orgId, query.from, query.to];
			let where = "org_id = $1 AND window_start >= $2 AND window_start < $3";
			if (query.eventType) {
				params.push(query.eventType);
				where += ` AND event_type = $${params.length}`;
			}

			const result = await this.pool.query(
				`SELECT gateway_id, org_id, event_type, SUM(count)::bigint AS count, window_start
				 FROM usage_events
				 WHERE ${where}
				 GROUP BY gateway_id, org_id, event_type, window_start
				 ORDER BY window_start ASC`,
				params,
			);

			return result.rows.map(rowToUsageRow);
		}, "Failed to query usage");
	}

	async queryGatewayUsage(query: UsageQuery): Promise<Result<UsageRow[], ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const params: unknown[] = [query.orgId, query.from, query.to];
			let where = "org_id = $1 AND window_start >= $2 AND window_start < $3";
			if (query.eventType) {
				params.push(query.eventType);
				where += ` AND event_type = $${params.length}`;
			}

			const result = await this.pool.query(
				`SELECT gateway_id, org_id, event_type, SUM(count)::bigint AS count,
				        date_trunc('hour', window_start) AS window_start
				 FROM usage_events
				 WHERE ${where}
				 GROUP BY gateway_id, org_id, event_type, date_trunc('hour', window_start)
				 ORDER BY window_start ASC, gateway_id ASC`,
				params,
			);

			return result.rows.map(rowToUsageRow);
		}, "Failed to query gateway usage");
	}
}

function rowToUsageRow(row: Record<string, unknown>): UsageRow {
	return {
		gatewayId: row.gateway_id as string,
		orgId: row.org_id as string,
		eventType: row.event_type as UsageRow["eventType"],
		count: Number(row.count),
		windowStart: new Date(row.window_start as string),
	};
}
