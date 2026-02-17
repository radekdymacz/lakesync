import type {
	ActionDiscovery,
	ActionPush,
	ActionResponse,
	ApiErrorCode,
	ConnectorDescriptor,
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
import type {
	ActionTransport,
	CheckpointResponse,
	CheckpointTransport,
	SyncTransport,
} from "./transport";

/** Parsed error response from the gateway. */
interface GatewayErrorResponse {
	error: string;
	code?: ApiErrorCode;
	requestId?: string;
}

/** Try to parse a gateway error response, falling back to raw text. */
async function parseErrorResponse(
	response: Response,
): Promise<{ message: string; code?: ApiErrorCode; requestId?: string }> {
	const text = await response.text().catch(() => "Unknown error");
	try {
		const parsed = JSON.parse(text) as GatewayErrorResponse;
		if (parsed.error) {
			return { message: parsed.error, code: parsed.code, requestId: parsed.requestId };
		}
	} catch {
		// Not JSON — fall through
	}
	return { message: text };
}

/** Format an error message with optional code and request ID for debugging. */
function formatTransportError(
	prefix: string,
	status: number,
	info: { message: string; code?: string; requestId?: string },
): string {
	let msg = `${prefix} (${status}): ${info.message}`;
	if (info.code) msg += ` [${info.code}]`;
	if (info.requestId) msg += ` (requestId: ${info.requestId})`;
	return msg;
}

/** Configuration for the HTTP sync transport */
export interface HttpTransportConfig {
	/** Base URL of the gateway (e.g. "https://gateway.example.com") */
	baseUrl: string;
	/** Gateway identifier */
	gatewayId: string;
	/** Bearer token for authentication */
	token: string;
	/**
	 * Optional callback to retrieve a fresh token before each request.
	 * When set, takes priority over the static `token` field.
	 * On a 401 response, the callback is invoked again and the request retried once.
	 */
	getToken?: () => string | Promise<string>;
	/** Optional custom fetch implementation (useful for testing) */
	fetch?: typeof globalThis.fetch;
	/**
	 * API version path prefix prepended to all route paths.
	 * Defaults to `"/v1"`. Set to `""` to use unversioned paths.
	 */
	apiVersion?: string;
}

/**
 * HTTP-based sync transport for communicating with a remote gateway.
 *
 * Sends push requests via POST and pull requests via GET, using
 * BigInt-safe JSON serialisation for HLC timestamps.
 */
export class HttpTransport implements SyncTransport, CheckpointTransport, ActionTransport {
	private readonly baseUrl: string;
	private readonly gatewayId: string;
	private readonly token: string;
	private readonly getToken: (() => string | Promise<string>) | undefined;
	private readonly _fetch: typeof globalThis.fetch;
	private readonly apiVersion: string;

	constructor(config: HttpTransportConfig) {
		this.baseUrl = config.baseUrl.replace(/\/+$/, "");
		this.gatewayId = config.gatewayId;
		this.token = config.token;
		this.getToken = config.getToken;
		this._fetch = config.fetch ?? globalThis.fetch.bind(globalThis);
		this.apiVersion = config.apiVersion ?? "/v1";
	}

	/** Resolve the current bearer token, preferring getToken callback over static token. */
	private async resolveToken(): Promise<string> {
		if (this.getToken) {
			return this.getToken();
		}
		return this.token;
	}

	/**
	 * Push local deltas to the remote gateway.
	 *
	 * Sends a POST request with the push payload as BigInt-safe JSON.
	 * On 401, if `getToken` is configured, refreshes the token and retries once.
	 */
	async push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>> {
		const url = `${this.baseUrl}${this.apiVersion}/sync/${this.gatewayId}/push`;
		const body = JSON.stringify(msg, bigintReplacer);

		try {
			let token = await this.resolveToken();
			let response = await this._fetch(url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${token}`,
				},
				body,
			});

			if (response.status === 401 && this.getToken) {
				token = await this.getToken();
				response = await this._fetch(url, {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						Authorization: `Bearer ${token}`,
					},
					body,
				});
			}

			if (!response.ok) {
				const info = await parseErrorResponse(response);
				return Err(
					new LSError(
						formatTransportError("Push failed", response.status, info),
						"TRANSPORT_ERROR",
					),
				);
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
	 * On 401, if `getToken` is configured, refreshes the token and retries once.
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
		const url = `${this.baseUrl}${this.apiVersion}/sync/${this.gatewayId}/pull?${params}`;

		try {
			let token = await this.resolveToken();
			let response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${token}`,
				},
			});

			if (response.status === 401 && this.getToken) {
				token = await this.getToken();
				response = await this._fetch(url, {
					method: "GET",
					headers: {
						Authorization: `Bearer ${token}`,
					},
				});
			}

			if (!response.ok) {
				const info = await parseErrorResponse(response);
				return Err(
					new LSError(
						formatTransportError("Pull failed", response.status, info),
						"TRANSPORT_ERROR",
					),
				);
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
		const url = `${this.baseUrl}${this.apiVersion}/sync/${this.gatewayId}/action`;
		const body = JSON.stringify(msg, bigintReplacer);

		try {
			const token = await this.resolveToken();
			const response = await this._fetch(url, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Authorization: `Bearer ${token}`,
				},
				body,
			});

			if (!response.ok) {
				const info = await parseErrorResponse(response);
				return Err(
					new LSError(
						formatTransportError("Action failed", response.status, info),
						"TRANSPORT_ERROR",
					),
				);
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
		const url = `${this.baseUrl}${this.apiVersion}/sync/${this.gatewayId}/actions`;

		try {
			const token = await this.resolveToken();
			const response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${token}`,
				},
			});

			if (!response.ok) {
				const info = await parseErrorResponse(response);
				return Err(
					new LSError(
						formatTransportError("Describe actions failed", response.status, info),
						"TRANSPORT_ERROR",
					),
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
	 * List available connector types and their configuration schemas.
	 *
	 * Sends a GET request to the unauthenticated `/connectors/types` endpoint.
	 */
	async listConnectorTypes(): Promise<Result<ConnectorDescriptor[], LakeSyncError>> {
		const url = `${this.baseUrl}${this.apiVersion}/connectors/types`;

		try {
			const response = await this._fetch(url, {
				method: "GET",
			});

			if (!response.ok) {
				const info = await parseErrorResponse(response);
				return Err(
					new LSError(
						formatTransportError("List connector types failed", response.status, info),
						"TRANSPORT_ERROR",
					),
				);
			}

			const data = (await response.json()) as ConnectorDescriptor[];
			return Ok(data);
		} catch (error) {
			const cause = toError(error);
			return Err(
				new LSError(
					`List connector types request failed: ${cause.message}`,
					"TRANSPORT_ERROR",
					cause,
				),
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
		const url = `${this.baseUrl}${this.apiVersion}/sync/${this.gatewayId}/checkpoint`;

		try {
			const token = await this.resolveToken();
			const response = await this._fetch(url, {
				method: "GET",
				headers: {
					Authorization: `Bearer ${token}`,
					Accept: "application/x-lakesync-checkpoint-stream",
				},
			});

			if (response.status === 404) {
				return Ok(null);
			}

			if (!response.ok) {
				const info = await parseErrorResponse(response);
				return Err(
					new LSError(
						formatTransportError("Checkpoint failed", response.status, info),
						"TRANSPORT_ERROR",
					),
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
