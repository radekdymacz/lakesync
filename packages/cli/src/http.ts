import type { CliConfig } from "./config";
import { fatal } from "./output";

/** Resolve the gateway URL from config or die. */
export function requireUrl(config: CliConfig): string {
	if (!config.gatewayUrl) {
		fatal("No gateway URL configured. Run `lakesync init` or set --url.");
	}
	return config.gatewayUrl;
}

/** Resolve the gateway ID from config or die. */
export function requireGatewayId(config: CliConfig, flag?: string): string {
	const id = flag ?? config.gatewayId;
	if (!id) {
		fatal("No gateway ID configured. Run `lakesync init` or pass --gateway-id.");
	}
	return id;
}

/** Resolve the token from config or die. */
export function requireToken(config: CliConfig): string {
	if (!config.token) {
		fatal("No token configured. Run `lakesync login` or `lakesync token create`.");
	}
	return config.token;
}

/** Make an authenticated HTTP request to the gateway. */
export async function request(
	url: string,
	token: string,
	path: string,
	opts?: { method?: string; body?: unknown },
): Promise<Response> {
	const method = opts?.method ?? "GET";
	const headers: Record<string, string> = {
		Authorization: `Bearer ${token}`,
		"Content-Type": "application/json",
	};

	const init: RequestInit = { method, headers };
	if (opts?.body !== undefined) {
		init.body = JSON.stringify(opts.body);
	}

	const fullUrl = `${url.replace(/\/$/, "")}${path}`;
	try {
		return await fetch(fullUrl, init);
	} catch (error) {
		fatal(`Request failed: ${error instanceof Error ? error.message : String(error)}`);
	}
}
