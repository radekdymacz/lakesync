import { loadConfig, saveConfig } from "../config";
import { fatal, print } from "../output";

/**
 * `lakesync login` — Store gateway URL and token in ~/.lakesync/config.json.
 */
export function login(flags: Record<string, string>): void {
	const url = flags.url ?? process.env.LAKESYNC_GATEWAY_URL;
	const token = flags.token ?? process.env.LAKESYNC_TOKEN;
	const gatewayId = flags.gateway ?? process.env.LAKESYNC_GATEWAY_ID;

	if (!url) {
		fatal("--url is required (or set LAKESYNC_GATEWAY_URL)");
	}
	if (!token) {
		fatal("--token is required (or set LAKESYNC_TOKEN)");
	}

	const config = loadConfig();
	config.gatewayUrl = url;
	config.token = token;
	if (gatewayId) config.gatewayId = gatewayId;

	saveConfig(config);
	print(`Logged in to ${url}`);
	if (gatewayId) print(`Default gateway: ${gatewayId}`);
}
