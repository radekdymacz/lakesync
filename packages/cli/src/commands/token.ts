import { signToken } from "@lakesync/core";
import { loadConfig } from "../config";
import { fatal, print } from "../output";

/**
 * `lakesync token create` — Generate a JWT for development and testing.
 *
 * Uses `signToken` from `@lakesync/core` to create HMAC-SHA256 tokens
 * compatible with both gateway-worker and gateway-server.
 */
export async function tokenCreate(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();

	const secret = flags.secret ?? process.env.LAKESYNC_JWT_SECRET;
	if (!secret) {
		fatal("--secret is required (or set LAKESYNC_JWT_SECRET environment variable)");
	}

	const gateway = flags.gateway ?? config.gatewayId;
	if (!gateway) {
		fatal("--gateway is required (or set default in ~/.lakesync/config.json)");
	}

	const client = flags.client ?? "cli-user";
	const role = flags.role ?? "client";
	if (role !== "admin" && role !== "client") {
		fatal("--role must be 'admin' or 'client'");
	}

	const ttl = flags.ttl ? Number.parseInt(flags.ttl, 10) : 3600;
	if (Number.isNaN(ttl) || ttl <= 0) {
		fatal("--ttl must be a positive number of seconds");
	}

	const exp = Math.floor(Date.now() / 1000) + ttl;

	const token = await signToken(
		{ sub: client, gw: gateway, role: role as "admin" | "client", exp },
		secret,
	);

	print(token);
}
