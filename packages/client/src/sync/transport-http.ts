import type {
	HLCTimestamp,
	LakeSyncError,
	Result,
	SyncPull,
	SyncPush,
	SyncResponse,
} from "@lakesync/core";
import { Err, LakeSyncError as LSError, Ok } from "@lakesync/core";
import type { SyncTransport } from "./transport";

/** Configuration for the HTTP sync transport */
export interface HttpTransportConfig {
	/** Base URL of the gateway (e.g. "https://gateway.example.com") */
	baseUrl: string;
	/** Gateway identifier */
	gatewayId: string;
	/** Bearer token for authentication */
	token: string;
	/** Optional custom fetch implementation (useful for testing) */
	fetch?: typeof globalThis.fetch;
}

/** BigInt-safe JSON replacer — converts bigint values to strings */
function bigintReplacer(_key: string, value: unknown): unknown {
	return typeof value === "bigint" ? value.toString() : value;
}

/** BigInt-aware JSON reviver — restores HLC fields from string to BigInt */
function bigintReviver(key: string, value: unknown): unknown {
	if (typeof value === "string" && /hlc$/i.test(key)) {
		return BigInt(value);
	}
	return value;
}

/**
 * HTTP-based sync transport for communicating with a remote gateway.
 *
 * Sends push requests via POST and pull requests via GET, using
 * BigInt-safe JSON serialisation for HLC timestamps.
 */
export class HttpTransport implements SyncTransport {
	private readonly baseUrl: string;
	private readonly gatewayId: string;
	private readonly token: string;
	private readonly _fetch: typeof globalThis.fetch;

	constructor(config: HttpTransportConfig) {
		this.baseUrl = config.baseUrl.replace(/\/+$/, "");
		this.gatewayId = config.gatewayId;
		this.token = config.token;
		this._fetch = config.fetch ?? globalThis.fetch.bind(globalThis);
	}

	/**
	 * Push local deltas to the remote gateway.
	 *
	 * Sends a POST request with the push payload as BigInt-safe JSON.
	 */
	async push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>> {
		const url = `${this.baseUrl}/sync/${this.gatewayId}/push`;

		try {
			const response = await this._fetch(url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${this.token}`,
				},
				body: JSON.stringify(msg, bigintReplacer),
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "Unknown error");
				return Err(new LSError(`Push failed (${response.status}): ${text}`, "TRANSPORT_ERROR"));
			}

			const raw = await response.text();
			const data = JSON.parse(raw, bigintReviver) as {
				serverHlc: HLCTimestamp;
				accepted: number;
			};
			return Ok(data);
		} catch (error) {
			return Err(
				new LSError(
					`Push request failed: ${error instanceof Error ? error.message : String(error)}`,
					"TRANSPORT_ERROR",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/**
	 * Pull remote deltas from the gateway.
	 *
	 * Sends a GET request with query parameters for the pull cursor.
	 */
	async pull(msg: SyncPull): Promise<Result<SyncResponse, LakeSyncError>> {
		const params = new URLSearchParams({
			since: msg.sinceHlc.toString(),
			clientId: msg.clientId,
			limit: msg.maxDeltas.toString(),
		});
		const url = `${this.baseUrl}/sync/${this.gatewayId}/pull?${params}`;

		try {
			const response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${this.token}`,
				},
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "Unknown error");
				return Err(new LSError(`Pull failed (${response.status}): ${text}`, "TRANSPORT_ERROR"));
			}

			const raw = await response.text();
			const data = JSON.parse(raw, bigintReviver) as SyncResponse;
			return Ok(data);
		} catch (error) {
			return Err(
				new LSError(
					`Pull request failed: ${error instanceof Error ? error.message : String(error)}`,
					"TRANSPORT_ERROR",
					error instanceof Error ? error : undefined,
				),
			);
		}
	}
}
