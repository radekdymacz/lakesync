import { loadConfig } from "../config";
import { fatal, print } from "../output";

/**
 * `lakesync status` — Show gateway health and metrics.
 */
export async function status(flags: Record<string, string>): Promise<void> {
	const config = loadConfig();

	const gatewayUrl = flags.url ?? config.gatewayUrl;
	if (!gatewayUrl) {
		fatal("--url is required (or set gatewayUrl in ~/.lakesync/config.json)");
	}

	// Health check (unauthenticated)
	try {
		const healthResponse = await fetch(`${gatewayUrl}/health`);
		const health = (await healthResponse.json()) as { status: string };
		print(`Health: ${health.status}`);
	} catch (_err) {
		fatal(`Cannot reach gateway at ${gatewayUrl}`);
	}

	// If we have a gateway ID and token, fetch metrics
	const gatewayId = flags.gateway ?? config.gatewayId;
	const token = flags.token ?? config.token ?? process.env.LAKESYNC_TOKEN;

	if (gatewayId && token) {
		try {
			const metricsResponse = await fetch(`${gatewayUrl}/v1/admin/metrics/${gatewayId}`, {
				headers: { Authorization: `Bearer ${token}` },
			});

			if (metricsResponse.ok) {
				const metrics = (await metricsResponse.json()) as Record<string, unknown>;
				print("");
				print(`Gateway: ${gatewayId}`);
				for (const [key, value] of Object.entries(metrics)) {
					if (typeof value === "number" || typeof value === "string") {
						print(`  ${key}: ${value}`);
					}
				}
			}
		} catch {
			// Metrics endpoint not available — skip silently
		}
	}
}
