import { Err, Ok, type Result } from "@lakesync/core";
import type { Pool } from "pg";
import type { CreateOrgInput, Organisation, UpdateOrgInput } from "../entities";
import { ControlPlaneError, wrapControlPlane } from "../errors";
import type { OrgRepository } from "../repositories";

function generateId(): string {
	return crypto.randomUUID().replace(/-/g, "").slice(0, 21);
}

function rowToOrg(row: Record<string, unknown>): Organisation {
	return {
		id: row.id as string,
		name: row.name as string,
		slug: row.slug as string,
		plan: row.plan as Organisation["plan"],
		stripeCustomerId: (row.stripe_customer_id as string) ?? undefined,
		stripeSubscriptionId: (row.stripe_subscription_id as string) ?? undefined,
		createdAt: new Date(row.created_at as string),
		updatedAt: new Date(row.updated_at as string),
	};
}

/** Postgres-backed organisation repository */
export class PgOrgRepository implements OrgRepository {
	constructor(private readonly pool: Pool) {}

	async create(input: CreateOrgInput): Promise<Result<Organisation, ControlPlaneError>> {
		const id = generateId();
		const plan = input.plan ?? "free";

		return wrapControlPlane(async () => {
			try {
				const result = await this.pool.query(
					`INSERT INTO organisations (id, name, slug, plan, stripe_customer_id, stripe_subscription_id)
					 VALUES ($1, $2, $3, $4, $5, $6)
					 RETURNING *`,
					[
						id,
						input.name,
						input.slug,
						plan,
						input.stripeCustomerId ?? null,
						input.stripeSubscriptionId ?? null,
					],
				);
				return rowToOrg(result.rows[0] as Record<string, unknown>);
			} catch (error: unknown) {
				if (isDuplicateError(error)) {
					throw new ControlPlaneError(
						`Organisation with slug "${input.slug}" already exists`,
						"DUPLICATE",
					);
				}
				throw error;
			}
		}, "Failed to create organisation");
	}

	async getById(id: string): Promise<Result<Organisation | null, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("SELECT * FROM organisations WHERE id = $1", [id]);
			if (result.rows.length === 0) return null;
			return rowToOrg(result.rows[0] as Record<string, unknown>);
		}, "Failed to get organisation");
	}

	async getBySlug(slug: string): Promise<Result<Organisation | null, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("SELECT * FROM organisations WHERE slug = $1", [slug]);
			if (result.rows.length === 0) return null;
			return rowToOrg(result.rows[0] as Record<string, unknown>);
		}, "Failed to get organisation by slug");
	}

	async update(
		id: string,
		input: UpdateOrgInput,
	): Promise<Result<Organisation, ControlPlaneError>> {
		const setClauses: string[] = [];
		const values: unknown[] = [];
		let paramIdx = 1;

		if (input.name !== undefined) {
			setClauses.push(`name = $${paramIdx++}`);
			values.push(input.name);
		}
		if (input.slug !== undefined) {
			setClauses.push(`slug = $${paramIdx++}`);
			values.push(input.slug);
		}
		if (input.plan !== undefined) {
			setClauses.push(`plan = $${paramIdx++}`);
			values.push(input.plan);
		}
		if (input.stripeCustomerId !== undefined) {
			setClauses.push(`stripe_customer_id = $${paramIdx++}`);
			values.push(input.stripeCustomerId);
		}
		if (input.stripeSubscriptionId !== undefined) {
			setClauses.push(`stripe_subscription_id = $${paramIdx++}`);
			values.push(input.stripeSubscriptionId);
		}

		if (setClauses.length === 0) {
			return this.getById(id).then((r) => {
				if (!r.ok) return r;
				if (r.value === null) {
					return Err(new ControlPlaneError(`Organisation "${id}" not found`, "NOT_FOUND"));
				}
				return Ok(r.value);
			});
		}

		setClauses.push(`updated_at = now()`);
		values.push(id);

		return wrapControlPlane(async () => {
			try {
				const result = await this.pool.query(
					`UPDATE organisations SET ${setClauses.join(", ")} WHERE id = $${paramIdx} RETURNING *`,
					values,
				);
				if (result.rows.length === 0) {
					throw new ControlPlaneError(`Organisation "${id}" not found`, "NOT_FOUND");
				}
				return rowToOrg(result.rows[0] as Record<string, unknown>);
			} catch (error: unknown) {
				if (isDuplicateError(error)) {
					throw new ControlPlaneError(
						`Organisation slug "${input.slug}" already taken`,
						"DUPLICATE",
					);
				}
				throw error;
			}
		}, "Failed to update organisation");
	}

	async delete(id: string): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("DELETE FROM organisations WHERE id = $1", [id]);
			if (result.rowCount === 0) {
				throw new ControlPlaneError(`Organisation "${id}" not found`, "NOT_FOUND");
			}
		}, "Failed to delete organisation");
	}
}

function isDuplicateError(error: unknown): boolean {
	return (
		typeof error === "object" &&
		error !== null &&
		"code" in error &&
		(error as { code: string }).code === "23505"
	);
}
