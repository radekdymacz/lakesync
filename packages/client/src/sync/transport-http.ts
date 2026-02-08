import type {
	HLCTimestamp,
	LakeSyncError,
	Result,
	SyncPull,
	SyncPush,
	SyncResponse,
} from "@lakesync/core";
import {
	bigintReplacer,
	bigintReviver,
	Err,
	LakeSyncError as LSError,
	Ok,
	toError,
} from "@lakesync/core";
import { decodeSyncResponse } from "@lakesync/proto";
import type { CheckpointResponse, SyncTransport } from "./transport";

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
			const cause = toError(error);
			return Err(new LSError(`Push request failed: ${cause.message}`, "TRANSPORT_ERROR", cause));
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
		if (msg.source) {
			params.set("source", msg.source);
		}
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
			const cause = toError(error);
			return Err(new LSError(`Pull request failed: ${cause.message}`, "TRANSPORT_ERROR", cause));
		}
	}

	/**
	 * Download checkpoint for initial sync.
	 *
	 * Sends a GET request to the checkpoint endpoint. Returns null if no
	 * checkpoint is available (404). The response body is proto-encoded
	 * and contains pre-filtered deltas (filtered server-side by JWT claims).
	 */
	async checkpoint(): Promise<Result<CheckpointResponse | null, LakeSyncError>> {
		const url = `${this.baseUrl}/sync/${this.gatewayId}/checkpoint`;

		try {
			const response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${this.token}`,
				},
			});

			if (response.status === 404) {
				return Ok(null);
			}

			if (!response.ok) {
				const text = await response.text().catch(() => "Unknown error");
				return Err(
					new LSError(`Checkpoint failed (${response.status}): ${text}`, "TRANSPORT_ERROR"),
				);
			}

			const bytes = new Uint8Array(await response.arrayBuffer());
			const decoded = decodeSyncResponse(bytes);

			if (!decoded.ok) {
				return Err(
					new LSError(
						`Checkpoint decode failed: ${decoded.error.message}`,
						"TRANSPORT_ERROR",
						decoded.error,
					),
				);
			}

			const hlcHeader = response.headers.get("X-Checkpoint-Hlc");
			const snapshotHlc = hlcHeader ? (BigInt(hlcHeader) as HLCTimestamp) : decoded.value.serverHlc;

			return Ok({
				deltas: decoded.value.deltas,
				snapshotHlc,
			});
		} catch (error) {
			const cause = toError(error);
			return Err(
				new LSError(`Checkpoint request failed: ${cause.message}`, "TRANSPORT_ERROR", cause),
			);
		}
	}
}
