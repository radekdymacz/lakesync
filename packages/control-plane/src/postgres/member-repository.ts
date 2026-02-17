import type { Result } from "@lakesync/core";
import type { Pool } from "pg";
import type { CreateMemberInput, OrgMember, OrgRole } from "../entities";
import { ControlPlaneError, wrapControlPlane } from "../errors";
import type { MemberRepository } from "../repositories";

function rowToMember(row: Record<string, unknown>): OrgMember {
	return {
		orgId: row.org_id as string,
		userId: row.user_id as string,
		role: row.role as OrgRole,
		createdAt: new Date(row.created_at as string),
	};
}

/** Postgres-backed organisation member repository */
export class PgMemberRepository implements MemberRepository {
	constructor(private readonly pool: Pool) {}

	async add(input: CreateMemberInput): Promise<Result<OrgMember, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			try {
				const result = await this.pool.query(
					`INSERT INTO org_members (org_id, user_id, role)
					 VALUES ($1, $2, $3)
					 RETURNING *`,
					[input.orgId, input.userId, input.role],
				);
				return rowToMember(result.rows[0] as Record<string, unknown>);
			} catch (error: unknown) {
				if (isDuplicateError(error)) {
					throw new ControlPlaneError(
						`User "${input.userId}" is already a member of organisation "${input.orgId}"`,
						"DUPLICATE",
					);
				}
				throw error;
			}
		}, "Failed to add member");
	}

	async remove(orgId: string, userId: string): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"DELETE FROM org_members WHERE org_id = $1 AND user_id = $2",
				[orgId, userId],
			);
			if (result.rowCount === 0) {
				throw new ControlPlaneError(
					`Member "${userId}" not found in organisation "${orgId}"`,
					"NOT_FOUND",
				);
			}
		}, "Failed to remove member");
	}

	async listByOrg(orgId: string): Promise<Result<OrgMember[], ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"SELECT * FROM org_members WHERE org_id = $1 ORDER BY created_at ASC",
				[orgId],
			);
			return result.rows.map((row) => rowToMember(row as Record<string, unknown>));
		}, "Failed to list members");
	}

	async getRole(
		orgId: string,
		userId: string,
	): Promise<Result<OrgRole | null, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"SELECT role FROM org_members WHERE org_id = $1 AND user_id = $2",
				[orgId, userId],
			);
			if (result.rows.length === 0) return null;
			return (result.rows[0] as { role: string }).role as OrgRole;
		}, "Failed to get member role");
	}

	async updateRole(
		orgId: string,
		userId: string,
		role: OrgRole,
	): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"UPDATE org_members SET role = $1 WHERE org_id = $2 AND user_id = $3",
				[role, orgId, userId],
			);
			if (result.rowCount === 0) {
				throw new ControlPlaneError(
					`Member "${userId}" not found in organisation "${orgId}"`,
					"NOT_FOUND",
				);
			}
		}, "Failed to update member role");
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
