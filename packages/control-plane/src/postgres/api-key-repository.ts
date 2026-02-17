import type { Result } from "@lakesync/core";
import type { Pool } from "pg";
import type { ApiKey, CreateApiKeyInput } from "../entities";
import { ControlPlaneError, wrapControlPlane } from "../errors";
import type { ApiKeyRepository } from "../repositories";

function generateId(): string {
	return crypto.randomUUID().replace(/-/g, "").slice(0, 21);
}

/** Generate a cryptographically random API key with the lk_ prefix */
function generateRawKey(): string {
	const bytes = new Uint8Array(32);
	crypto.getRandomValues(bytes);
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
	let key = "";
	for (const byte of bytes) {
		key += chars[byte % chars.length];
	}
	return `lk_${key}`;
}

/** Hash a raw key with SHA-256 and return the hex digest */
async function hashKey(rawKey: string): Promise<string> {
	const encoder = new TextEncoder();
	const data = encoder.encode(rawKey);
	const hashBuffer = await crypto.subtle.digest("SHA-256", data);
	const hashArray = new Uint8Array(hashBuffer);
	return Array.from(hashArray)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}

function rowToApiKey(row: Record<string, unknown>): ApiKey {
	return {
		id: row.id as string,
		orgId: row.org_id as string,
		gatewayId: (row.gateway_id as string) ?? undefined,
		name: row.name as string,
		keyHash: row.key_hash as string,
		keyPrefix: row.key_prefix as string,
		role: row.role as ApiKey["role"],
		scopes: (row.scopes as string[]) ?? undefined,
		expiresAt: row.expires_at ? new Date(row.expires_at as string) : undefined,
		lastUsedAt: row.last_used_at ? new Date(row.last_used_at as string) : undefined,
		createdAt: new Date(row.created_at as string),
	};
}

/** Postgres-backed API key repository */
export class PgApiKeyRepository implements ApiKeyRepository {
	constructor(private readonly pool: Pool) {}

	async create(
		input: CreateApiKeyInput,
	): Promise<Result<{ apiKey: ApiKey; rawKey: string }, ControlPlaneError>> {
		const id = generateId();
		const rawKey = generateRawKey();

		return wrapControlPlane(async () => {
			const keyHash = await hashKey(rawKey);
			const keyPrefix = rawKey.slice(0, 11); // "lk_" + first 8 chars

			const result = await this.pool.query(
				`INSERT INTO api_keys (id, org_id, gateway_id, name, key_hash, key_prefix, role, scopes, expires_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				 RETURNING *`,
				[
					id,
					input.orgId,
					input.gatewayId ?? null,
					input.name,
					keyHash,
					keyPrefix,
					input.role,
					input.scopes ? JSON.stringify(input.scopes) : null,
					input.expiresAt ?? null,
				],
			);
			return {
				apiKey: rowToApiKey(result.rows[0] as Record<string, unknown>),
				rawKey,
			};
		}, "Failed to create API key");
	}

	async getByHash(keyHash: string): Promise<Result<ApiKey | null, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("SELECT * FROM api_keys WHERE key_hash = $1", [keyHash]);
			if (result.rows.length === 0) return null;
			return rowToApiKey(result.rows[0] as Record<string, unknown>);
		}, "Failed to get API key by hash");
	}

	async listByOrg(orgId: string): Promise<Result<ApiKey[], ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query(
				"SELECT * FROM api_keys WHERE org_id = $1 ORDER BY created_at ASC",
				[orgId],
			);
			return result.rows.map((row) => rowToApiKey(row as Record<string, unknown>));
		}, "Failed to list API keys");
	}

	async revoke(id: string): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			const result = await this.pool.query("DELETE FROM api_keys WHERE id = $1", [id]);
			if (result.rowCount === 0) {
				throw new ControlPlaneError(`API key "${id}" not found`, "NOT_FOUND");
			}
		}, "Failed to revoke API key");
	}

	async updateLastUsed(id: string): Promise<Result<void, ControlPlaneError>> {
		return wrapControlPlane(async () => {
			await this.pool.query("UPDATE api_keys SET last_used_at = now() WHERE id = $1", [id]);
		}, "Failed to update last used");
	}
}

/** Hash a raw API key — exported for use by auth middleware */
export { hashKey };
