import { readFileSync } from "node:fs";
import type { HLCTimestamp, RowDelta, SyncPush } from "@lakesync/core";
import { bigintReplacer, bigintReviver } from "@lakesync/core";
import { loadConfig } from "../config";
import { fatal, print } from "../output";

/**
 * `lakesync push <file>` — Push deltas from a JSON file to the gateway.
 *
 * The file should contain an array of RowDelta objects or a full SyncPush body.
 */
export async function push(flags: Record<string, string>, positional: string[]): Promise<void> {
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

	const file = positional[0];
	if (!file) {
		fatal("Usage: lakesync push <file.json>");
	}

	let raw: string;
	try {
		raw = readFileSync(file, "utf-8");
	} catch (err) {
		fatal(`Cannot read file: ${file}`);
	}

	let body: SyncPush;
	try {
		const parsed = JSON.parse(raw, bigintReviver);

		// Accept either a full SyncPush body or a plain array of deltas
		if (Array.isArray(parsed)) {
			const clientId = flags.client ?? "cli";
			body = { clientId, deltas: parsed as RowDelta[], lastSeenHlc: 0n as HLCTimestamp };
		} else if (parsed.clientId && parsed.deltas) {
			body = parsed as SyncPush;
		} else {
			fatal("File must contain a JSON array of deltas or a { clientId, deltas } object");
		}
	} catch {
		fatal("File is not valid JSON");
	}

	const url = `${gatewayUrl}/v1/sync/${gatewayId}/push`;
	const response = await fetch(url, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			Authorization: `Bearer ${token}`,
		},
		body: JSON.stringify(body, bigintReplacer),
	});

	if (!response.ok) {
		const text = await response.text().catch(() => "Unknown error");
		fatal(`Push failed (${response.status}): ${text}`);
	}

	const result = JSON.parse(await response.text(), bigintReviver) as {
		serverHlc: HLCTimestamp;
		accepted: number;
	};

	print(
		`Pushed ${body.deltas.length} deltas (accepted: ${result.accepted}, serverHlc: ${result.serverHlc})`,
	);
}
