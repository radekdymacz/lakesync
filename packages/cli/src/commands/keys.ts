import { loadConfig } from "../config";
import { request, requireToken, requireUrl } from "../http";
import { fatal, print, printTable } from "../output";

interface ApiKey {
	id: string;
	orgId: string;
	gatewayId?: string;
	name: string;
	keyPrefix: string;
	role: string;
	expiresAt?: string;
	createdAt: string;
}

/**
 * Resolve the org ID from flags, config, or die.
 */
function requireOrgId(flags: Record<string, string>, orgId?: string): string {
	const id = flags.org ?? orgId;
	if (!id) {
		fatal("--org is required (or set orgId in ~/.lakesync/config.json)");
	}
	return id;
}

/**
 * `lakesync keys list` — List all API keys for the organisation.
 */
export async function keysList(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();
	const url = requireUrl(config);
	const token = requireToken(config);
	const orgId = requireOrgId(flags, config.orgId);

	const response = await request(url, token, `/v1/orgs/${encodeURIComponent(orgId)}/api-keys`);

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Failed to list API keys (${response.status}): ${text}`);
	}

	const keys = (await response.json()) as ApiKey[];

	if (keys.length === 0) {
		print("No API keys found.");
		return;
	}

	printTable(
		keys.map((key) => ({
			id: key.id,
			name: key.name,
			prefix: key.keyPrefix,
			role: key.role,
			gateway: key.gatewayId ?? "(all)",
			expires: key.expiresAt ?? "never",
		})),
	);
}

/**
 * `lakesync keys create` — Create a new API key.
 */
export async function keysCreate(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();
	const url = requireUrl(config);
	const token = requireToken(config);
	const orgId = requireOrgId(flags, config.orgId);

	const name = flags.name;
	if (!name) {
		fatal("--name is required");
	}

	const role = flags.role ?? "client";
	if (role !== "admin" && role !== "client") {
		fatal("--role must be 'admin' or 'client'");
	}

	const body: {
		orgId: string;
		name: string;
		role: string;
		gatewayId?: string;
		expiresAt?: string;
	} = { orgId, name, role };

	if (flags.gateway) {
		body.gatewayId = flags.gateway;
	}
	if (flags.expires) {
		body.expiresAt = flags.expires;
	}

	const response = await request(url, token, "/v1/api-keys", {
		method: "POST",
		body,
	});

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Failed to create API key (${response.status}): ${text}`);
	}

	const result = (await response.json()) as ApiKey & { rawKey: string };
	print(`Created API key: ${result.id}`);
	print(`  Name:   ${result.name}`);
	print(`  Prefix: ${result.keyPrefix}`);
	print(`  Role:   ${result.role}`);
	print("");
	print(`  Raw key (shown once): ${result.rawKey}`);
	print("");
	print("  Store this key securely. It cannot be retrieved again.");
}

/**
 * `lakesync keys revoke` — Revoke (delete) an API key.
 */
export async function keysRevoke(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();
	const url = requireUrl(config);
	const token = requireToken(config);

	const keyId = flags.id;
	if (!keyId) {
		fatal("--id is required (API key ID to revoke)");
	}

	const response = await request(url, token, `/v1/api-keys/${encodeURIComponent(keyId)}`, {
		method: "DELETE",
	});

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Failed to revoke API key (${response.status}): ${text}`);
	}

	print(`Revoked API key: ${keyId}`);
}
