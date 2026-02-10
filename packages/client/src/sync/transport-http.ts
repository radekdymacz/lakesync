import type {
	ActionDiscovery,
	ActionPush,
	ActionResponse,
	HLCTimestamp,
	LakeSyncError,
	Result,
	RowDelta,
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
	 * Execute imperative actions against external systems via the gateway.
	 *
	 * Sends a POST request with the action payload as BigInt-safe JSON.
	 */
	async executeAction(msg: ActionPush): Promise<Result<ActionResponse, LakeSyncError>> {
		const url = `${this.baseUrl}/sync/${this.gatewayId}/action`;

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
				return Err(new LSError(`Action failed (${response.status}): ${text}`, "TRANSPORT_ERROR"));
			}

			const raw = await response.text();
			const data = JSON.parse(raw, bigintReviver) as ActionResponse;
			return Ok(data);
		} catch (error) {
			const cause = toError(error);
			return Err(new LSError(`Action request failed: ${cause.message}`, "TRANSPORT_ERROR", cause));
		}
	}

	/**
	 * Discover available connectors and their supported action types.
	 *
	 * Sends a GET request to the actions discovery endpoint.
	 */
	async describeActions(): Promise<Result<ActionDiscovery, LakeSyncError>> {
		const url = `${this.baseUrl}/sync/${this.gatewayId}/actions`;

		try {
			const response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${this.token}`,
				},
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "Unknown error");
				return Err(
					new LSError(`Describe actions failed (${response.status}): ${text}`, "TRANSPORT_ERROR"),
				);
			}

			const data = (await response.json()) as ActionDiscovery;
			return Ok(data);
		} catch (error) {
			const cause = toError(error);
			return Err(
				new LSError(`Describe actions request failed: ${cause.message}`, "TRANSPORT_ERROR", cause),
			);
		}
	}

	/**
	 * Download checkpoint for initial sync.
	 *
	 * Requests the streaming checkpoint format via Accept header and reads
	 * length-prefixed proto frames from the response body.
	 */
	async checkpoint(): Promise<Result<CheckpointResponse | null, LakeSyncError>> {
		const url = `${this.baseUrl}/sync/${this.gatewayId}/checkpoint`;

		try {
			const response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${this.token}`,
					Accept: "application/x-lakesync-checkpoint-stream",
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

			const deltas = await readStreamingCheckpointDeltas(response);
			const hlcHeader = response.headers.get("X-Checkpoint-Hlc");
			const snapshotHlc = hlcHeader ? (BigInt(hlcHeader) as HLCTimestamp) : (0n as HLCTimestamp);
			return Ok({ deltas, snapshotHlc });
		} catch (error) {
			const cause = toError(error);
			return Err(
				new LSError(`Checkpoint request failed: ${cause.message}`, "TRANSPORT_ERROR", cause),
			);
		}
	}
}

/**
 * Read length-prefixed proto frames from a streaming checkpoint response.
 *
 * Each frame is: 4-byte big-endian length prefix + proto-encoded SyncResponse.
 * Collects all deltas across frames.
 */
async function readStreamingCheckpointDeltas(response: Response): Promise<RowDelta[]> {
	const reader = response.body!.getReader();
	const allDeltas: RowDelta[] = [];
	let buffer = new Uint8Array(0);

	for (;;) {
		const { done, value } = await reader.read();
		if (done) break;

		const newBuffer = new Uint8Array(buffer.length + value.length);
		newBuffer.set(buffer);
		newBuffer.set(value, buffer.length);
		buffer = newBuffer;

		while (buffer.length >= 4) {
			const frameLength = new DataView(buffer.buffer, buffer.byteOffset).getUint32(0, false);
			if (buffer.length < 4 + frameLength) break;

			const frameData = buffer.slice(4, 4 + frameLength);
			buffer = buffer.slice(4 + frameLength);

			const decoded = decodeSyncResponse(frameData);
			if (decoded.ok) {
				allDeltas.push(...decoded.value.deltas);
			}
		}
	}

	return allDeltas;
}
