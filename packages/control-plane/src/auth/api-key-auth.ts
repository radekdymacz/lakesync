import { Err, Ok, type Result } from "@lakesync/core";
import type { ApiKeyRole } from "../entities";
import { ControlPlaneError } from "../errors";
import type { ApiKeyRepository } from "../repositories";

/** Minimal typing for Web Crypto digest to avoid TS 5.7 Uint8Array variance issues */
interface DigestSubtle {
	digest(algorithm: string, data: Uint8Array): Promise<ArrayBuffer>;
}

/** Context extracted from a valid API key */
export interface ApiKeyAuthContext {
	readonly apiKeyId: string;
	readonly orgId: string;
	readonly gatewayId?: string;
	readonly role: ApiKeyRole;
}

/**
 * Authenticate a request using an API key.
 *
 * Expects the raw key (e.g. `lk_...`), hashes it with SHA-256,
 * looks it up in the repository, and returns the auth context.
 * Also updates the `lastUsedAt` timestamp (fire-and-forget).
 *
 * @param rawKey - The raw API key from the Authorization header
 * @param apiKeyRepo - The API key repository
 * @returns Auth context on success, ControlPlaneError on failure
 */
export async function authenticateApiKey(
	rawKey: string,
	apiKeyRepo: ApiKeyRepository,
): Promise<Result<ApiKeyAuthContext, ControlPlaneError>> {
	if (!rawKey.startsWith("lk_")) {
		return Err(new ControlPlaneError("Invalid API key format", "INVALID_INPUT"));
	}

	const keyHash = await hashApiKey(rawKey);
	const result = await apiKeyRepo.getByHash(keyHash);
	if (!result.ok) return result;

	const apiKey = result.value;
	if (apiKey === null) {
		return Err(new ControlPlaneError("Invalid API key", "NOT_FOUND"));
	}

	// Check expiry
	if (apiKey.expiresAt && apiKey.expiresAt < new Date()) {
		return Err(new ControlPlaneError("API key has expired", "NOT_FOUND"));
	}

	// Fire-and-forget: update last used timestamp
	apiKeyRepo.updateLastUsed(apiKey.id).catch(() => {
		// Intentionally swallowed — non-critical
	});

	return Ok({
		apiKeyId: apiKey.id,
		orgId: apiKey.orgId,
		gatewayId: apiKey.gatewayId,
		role: apiKey.role,
	});
}

/** Hash a raw API key with SHA-256 and return the hex digest */
async function hashApiKey(rawKey: string): Promise<string> {
	const encoder = new TextEncoder();
	const data = encoder.encode(rawKey);
	const hashBuffer = await (crypto.subtle as unknown as DigestSubtle).digest("SHA-256", data);
	const hashArray = new Uint8Array(hashBuffer);
	return Array.from(hashArray)
		.map((b) => b.toString(16).padStart(2, "0"))
		.join("");
}
