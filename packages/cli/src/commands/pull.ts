import { bigintReplacer, bigintReviver } from "@lakesync/core";
import type { SyncResponse } from "@lakesync/core";
import { loadConfig } from "../config";
import { fatal, print } from "../output";

/**
 * `lakesync pull` — Pull deltas from the gateway.
 *
 * Outputs deltas as JSON to stdout. Use --since to pull incrementally.
 */
export async function pull(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();

	const gatewayUrl = flags.url ?? config.gatewayUrl;
	if (!gatewayUrl) {
		fatal("--url is required (or set gatewayUrl in ~/.lakesync/config.json)");
	}

	const gatewayId = flags.gateway ?? config.gatewayId;
	if (!gatewayId) {
		fatal("--gateway is required (or set gatewayId in ~/.lakesync/config.json)");
	}

	const token = flags.token ?? config.token ?? process.env.LAKESYNC_TOKEN;
	if (!token) {
		fatal("--token is required (or set LAKESYNC_TOKEN environment variable)");
	}

	const since = flags.since ?? "0";
	const clientId = flags.client ?? "cli";
	const limit = flags.limit ?? "1000";

	const params = new URLSearchParams({ since, clientId, limit });
	if (flags.source) {
		params.set("source", flags.source);
	}

	const url = `${gatewayUrl}/v1/sync/${gatewayId}/pull?${params}`;
	const response = await fetch(url, {
		method: "GET",
		headers: {
			Authorization: `Bearer ${token}`,
		},
	});

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Pull failed (${response.status}): ${text}`);
	}

	const result = JSON.parse(await response.text(), bigintReviver) as SyncResponse;

	// Output as JSON (with BigInt handling)
	print(JSON.stringify(result, bigintReplacer, "\t"));
}
