import { Err, Ok, type Result } from "@lakesync/core";
import type { Pool } from "pg";
import type { CreateGatewayInput, Gateway, UpdateGatewayInput } from "../entities";
import { ControlPlaneError, wrapControlPlane } from "../errors";
import type { GatewayRepository } from "../repositories";

function generateId(): string {
	return crypto.randomUUID().replace(/-/g, "").slice(0, 21);
}

function rowToGateway(row: Record<string, unknown>): Gateway {
	return {
		id: row.id as string,
		orgId: row.org_id as string,
		name: row.name as string,
		region: (row.region as string) ?? undefined,
		status: row.status as Gateway["status"],
		createdAt: new Date(row.created_at as string),
		updatedAt: new Date(row.updated_at as string),
	};
}

/** Postgres-backed gateway repository */
export class PgGatewayRepository implements GatewayRepository {
	constructor(private readonly pool: Pool) {}

	async create(input: CreateGatewayInput): Promise<Result<Gateway, ControlPlaneError>> {
		const id = generateId();

		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				`INSERT INTO gateways (id, org_id, name, region)
				 VALUES ($1, $2, $3, $4)
				 RETURNING *`,
				[id, input.orgId, input.name, input.region ?? null],
			);
			return rowToGateway(result.rows[0] as Record<string, unknown>);
		}, "Failed to create gateway");
	}

	async getById(id: string): Promise<Result<Gateway | null, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("SELECT * FROM gateways WHERE id = $1", [id]);
			if (result.rows.length === 0) return null;
			return rowToGateway(result.rows[0] as Record<string, unknown>);
		}, "Failed to get gateway");
	}

	async listByOrg(orgId: string): Promise<Result<Gateway[], ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"SELECT * FROM gateways WHERE org_id = $1 ORDER BY created_at ASC",
				[orgId],
			);
			return result.rows.map((row) => rowToGateway(row as Record<string, unknown>));
		}, "Failed to list gateways");
	}

	async update(id: string, input: UpdateGatewayInput): Promise<Result<Gateway, ControlPlaneError>> {
		const setClauses: string[] = [];
		const values: unknown[] = [];
		let paramIdx = 1;

		if (input.name !== undefined) {
			setClauses.push(`name = $${paramIdx++}`);
			values.push(input.name);
		}
		if (input.region !== undefined) {
			setClauses.push(`region = $${paramIdx++}`);
			values.push(input.region);
		}
		if (input.status !== undefined) {
			setClauses.push(`status = $${paramIdx++}`);
			values.push(input.status);
		}

		if (setClauses.length === 0) {
			return this.getById(id).then((r) => {
				if (!r.ok) return r;
				if (r.value === null) {
					return Err(new ControlPlaneError(`Gateway "${id}" not found`, "NOT_FOUND"));
				}
				return Ok(r.value);
			});
		}

		setClauses.push(`updated_at = now()`);
		values.push(id);

		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				`UPDATE gateways SET ${setClauses.join(", ")} WHERE id = $${paramIdx} RETURNING *`,
				values,
			);
			if (result.rows.length === 0) {
				throw new ControlPlaneError(`Gateway "${id}" not found`, "NOT_FOUND");
			}
			return rowToGateway(result.rows[0] as Record<string, unknown>);
		}, "Failed to update gateway");
	}

	async delete(id: string): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("DELETE FROM gateways WHERE id = $1", [id]);
			if (result.rowCount === 0) {
				throw new ControlPlaneError(`Gateway "${id}" not found`, "NOT_FOUND");
			}
		}, "Failed to delete gateway");
	}
}
