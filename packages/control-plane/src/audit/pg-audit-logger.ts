import type { Result } from "@lakesync/core";
import type { Pool } from "pg";
import type { ControlPlaneError } from "../errors";
import { wrapControlPlane } from "../errors";
import type { AuditLogger } from "./audit-logger";
import type { AuditEvent, AuditPage, AuditQuery, RecordAuditInput } from "./types";

const DEFAULT_PAGE_LIMIT = 50;
const MAX_PAGE_LIMIT = 200;

function rowToAuditEvent(row: Record<string, unknown>): AuditEvent {
	return {
		id: row.id as string,
		orgId: row.org_id as string,
		actorId: row.actor_id as string,
		actorType: row.actor_type as AuditEvent["actorType"],
		action: row.action as AuditEvent["action"],
		resource: row.resource as string,
		metadata: row.metadata ? (row.metadata as Record<string, unknown>) : undefined,
		ipAddress: (row.ip_address as string) ?? undefined,
		timestamp: new Date(row.timestamp as string),
	};
}

/** Postgres-backed append-only audit logger */
export class PgAuditLogger implements AuditLogger {
	constructor(private readonly pool: Pool) {}

	async record(input: RecordAuditInput): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const id = crypto.randomUUID().replace(/-/g, "").slice(0, 21);
			await this.pool.query(
				`INSERT INTO audit_events (id, org_id, actor_id, actor_type, action, resource, metadata, ip_address)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				[
					id,
					input.orgId,
					input.actorId,
					input.actorType,
					input.action,
					input.resource,
					input.metadata ? JSON.stringify(input.metadata) : null,
					input.ipAddress ?? null,
				],
			);
		}, "Failed to record audit event");
	}

	async query(params: AuditQuery): Promise<Result<AuditPage, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const conditions: string[] = ["org_id = $1"];
			const values: unknown[] = [params.orgId];
			let paramIdx = 2;

			if (params.from) {
				conditions.push(`timestamp >= $${paramIdx++}`);
				values.push(params.from.toISOString());
			}
			if (params.to) {
				conditions.push(`timestamp <= $${paramIdx++}`);
				values.push(params.to.toISOString());
			}
			if (params.action) {
				conditions.push(`action = $${paramIdx++}`);
				values.push(params.action);
			}
			if (params.actorId) {
				conditions.push(`actor_id = $${paramIdx++}`);
				values.push(params.actorId);
			}
			if (params.cursor) {
				conditions.push(`id < $${paramIdx++}`);
				values.push(params.cursor);
			}

			const limit = Math.min(params.limit ?? DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT);
			// Fetch one extra to determine hasMore
			values.push(limit + 1);

			const sql = `
				SELECT * FROM audit_events
				WHERE ${conditions.join(" AND ")}
				ORDER BY timestamp DESC, id DESC
				LIMIT $${paramIdx}
			`;

			const result = await this.pool.query(sql, values);
			const rows = result.rows as Record<string, unknown>[];
			const hasMore = rows.length > limit;
			const pageRows = hasMore ? rows.slice(0, limit) : rows;
			const events = pageRows.map(rowToAuditEvent);
			const lastEvent = events[events.length - 1];
			const cursor = hasMore && lastEvent ? lastEvent.id : undefined;

			return { events, cursor, hasMore };
		}, "Failed to query audit events");
	}

	async deleteOlderThan(orgId: string, before: Date): Promise<Result<number, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"DELETE FROM audit_events WHERE org_id = $1 AND timestamp < $2",
				[orgId, before.toISOString()],
			);
			return result.rowCount ?? 0;
		}, "Failed to delete old audit events");
	}
}
