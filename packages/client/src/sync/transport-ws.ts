import type {
	HLCTimestamp,
	LakeSyncError,
	Result,
	RowDelta,
	SyncPull,
	SyncPush,
	SyncResponse,
} from "@lakesync/core";
import { Err, LakeSyncError as LSError, Ok, toError } from "@lakesync/core";
import {
	decodeBroadcastFrame,
	decodeSyncResponse,
	encodeSyncPull,
	encodeSyncPush,
	TAG_BROADCAST,
	TAG_SYNC_PULL,
	TAG_SYNC_PUSH,
} from "@lakesync/proto";
import type {
	CheckpointResponse,
	CheckpointTransport,
	RealtimeTransport,
	SyncTransport,
} from "./transport";
import { HttpTransport, type HttpTransportConfig } from "./transport-http";

/** Configuration for the WebSocket sync transport. */
export interface WebSocketTransportConfig {
	/** WebSocket URL, e.g. "wss://gateway.example.com/sync/my-gw/ws" */
	url: string;
	/** Bearer token (passed as ?token= query param for browser compat). */
	token: string;
	/** Called when server broadcasts deltas. */
	onBroadcast?: (deltas: RowDelta[], serverHlc: HLCTimestamp) => void;
	/** Reconnect base delay in ms (default 1000). */
	reconnectBaseMs?: number;
	/** Max reconnect delay in ms (default 30000). */
	reconnectMaxMs?: number;
	/** HTTP transport config for checkpoint fallback. */
	httpConfig?: HttpTransportConfig;
}

/** Default reconnect base delay. */
const DEFAULT_RECONNECT_BASE_MS = 1000;

/** Default max reconnect delay. */
const DEFAULT_RECONNECT_MAX_MS = 30_000;

/**
 * WebSocket-based sync transport for real-time delta synchronisation.
 *
 * Uses the binary protobuf protocol with tag-based framing:
 * - `0x01` = SyncPush (client → server)
 * - `0x02` = SyncPull (client → server)
 * - `0x03` = Broadcast (server → client)
 *
 * Automatically reconnects on disconnect with exponential backoff.
 * Checkpoints are delegated to an internal {@link HttpTransport} (large
 * binary payloads are better suited to HTTP).
 */
export class WebSocketTransport implements SyncTransport, RealtimeTransport, CheckpointTransport {
	private readonly config: WebSocketTransportConfig;
	private readonly reconnectBaseMs: number;
	private readonly reconnectMaxMs: number;
	private readonly httpTransport: HttpTransport | null;

	private ws: WebSocket | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private reconnectAttempts = 0;
	private _connected = false;
	private intentionalClose = false;

	/** Pending request/response promise (push or pull). */
	private pending: {
		resolve: (value: Result<SyncResponse, LakeSyncError>) => void;
		reject: (reason: Error) => void;
	} | null = null;

	/** Broadcast callback registered by the SyncCoordinator. */
	private broadcastCallback: ((deltas: RowDelta[], serverHlc: HLCTimestamp) => void) | null = null;

	constructor(config: WebSocketTransportConfig) {
		this.config = config;
		this.reconnectBaseMs = config.reconnectBaseMs ?? DEFAULT_RECONNECT_BASE_MS;
		this.reconnectMaxMs = config.reconnectMaxMs ?? DEFAULT_RECONNECT_MAX_MS;
		this.httpTransport = config.httpConfig ? new HttpTransport(config.httpConfig) : null;

		if (config.onBroadcast) {
			this.broadcastCallback = config.onBroadcast;
		}
	}

	/** Whether the WebSocket is currently connected. */
	get connected(): boolean {
		return this._connected;
	}

	/** Whether this transport supports real-time server push. */
	get supportsRealtime(): boolean {
		return true;
	}

	/** Register callback for server-initiated broadcasts. */
	onBroadcast(callback: (deltas: RowDelta[], serverHlc: HLCTimestamp) => void): void {
		this.broadcastCallback = callback;
	}

	/** Open the WebSocket connection. */
	connect(): void {
		if (this.ws) return;
		this.intentionalClose = false;
		this.openWebSocket();
	}

	/** Close the WebSocket connection and stop reconnecting. */
	disconnect(): void {
		this.intentionalClose = true;
		if (this.reconnectTimer !== null) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
		if (this.ws) {
			this.ws.close(1000, "Client disconnect");
			this.ws = null;
		}
		this._connected = false;
		this.reconnectAttempts = 0;

		// Reject any pending request
		if (this.pending) {
			this.pending.resolve(Err(new LSError("WebSocket disconnected", "TRANSPORT_ERROR")));
			this.pending = null;
		}
	}

	/**
	 * Push local deltas to the gateway via WebSocket.
	 */
	async push(
		msg: SyncPush,
	): Promise<Result<{ serverHlc: HLCTimestamp; accepted: number }, LakeSyncError>> {
		const encoded = encodeSyncPush({
			clientId: msg.clientId,
			deltas: msg.deltas,
			lastSeenHlc: msg.lastSeenHlc,
		});
		if (!encoded.ok) {
			return Err(new LSError(`Failed to encode push: ${encoded.error.message}`, "TRANSPORT_ERROR"));
		}

		const frame = new Uint8Array(1 + encoded.value.length);
		frame[0] = TAG_SYNC_PUSH;
		frame.set(encoded.value, 1);

		const response = await this.sendAndAwaitResponse(frame);
		if (!response.ok) return response;

		return Ok({
			serverHlc: response.value.serverHlc,
			accepted:
				response.value.deltas.length === 0 ? msg.deltas.length : response.value.deltas.length,
		});
	}

	/**
	 * Pull remote deltas from the gateway via WebSocket.
	 */
	async pull(msg: SyncPull): Promise<Result<SyncResponse, LakeSyncError>> {
		const encoded = encodeSyncPull({
			clientId: msg.clientId,
			sinceHlc: msg.sinceHlc,
			maxDeltas: msg.maxDeltas,
		});
		if (!encoded.ok) {
			return Err(new LSError(`Failed to encode pull: ${encoded.error.message}`, "TRANSPORT_ERROR"));
		}

		const frame = new Uint8Array(1 + encoded.value.length);
		frame[0] = TAG_SYNC_PULL;
		frame.set(encoded.value, 1);

		return this.sendAndAwaitResponse(frame);
	}

	/**
	 * Download checkpoint via HTTP (large binary payloads are better over HTTP).
	 */
	async checkpoint(): Promise<Result<CheckpointResponse | null, LakeSyncError>> {
		if (!this.httpTransport) {
			return Ok(null);
		}
		return this.httpTransport.checkpoint();
	}

	// -----------------------------------------------------------------------
	// Internal
	// -----------------------------------------------------------------------

	private openWebSocket(): void {
		const url = `${this.config.url}?token=${encodeURIComponent(this.config.token)}`;
		this.ws = new WebSocket(url);
		this.ws.binaryType = "arraybuffer";

		this.ws.onopen = () => {
			this._connected = true;
			this.reconnectAttempts = 0;
		};

		this.ws.onmessage = (event: MessageEvent) => {
			if (!(event.data instanceof ArrayBuffer)) return;
			const bytes = new Uint8Array(event.data);
			if (bytes.length < 2) return;

			const tag = bytes[0];

			if (tag === TAG_BROADCAST) {
				// Server-initiated broadcast
				const decoded = decodeBroadcastFrame(bytes);
				if (decoded.ok && this.broadcastCallback) {
					this.broadcastCallback(decoded.value.deltas, decoded.value.serverHlc);
				}
			} else {
				// Response to a pending push/pull request
				const decoded = decodeSyncResponse(bytes);
				if (this.pending) {
					if (decoded.ok) {
						this.pending.resolve(Ok(decoded.value));
					} else {
						this.pending.resolve(
							Err(
								new LSError(
									`Failed to decode response: ${decoded.error.message}`,
									"TRANSPORT_ERROR",
								),
							),
						);
					}
					this.pending = null;
				}
			}
		};

		this.ws.onclose = () => {
			this._connected = false;
			this.ws = null;

			// Reject pending request
			if (this.pending) {
				this.pending.resolve(
					Err(new LSError("WebSocket closed before response", "TRANSPORT_ERROR")),
				);
				this.pending = null;
			}

			if (!this.intentionalClose) {
				this.scheduleReconnect();
			}
		};

		this.ws.onerror = () => {
			// onclose will fire after onerror — reconnect handled there
		};
	}

	private scheduleReconnect(): void {
		const delay = Math.min(this.reconnectBaseMs * 2 ** this.reconnectAttempts, this.reconnectMaxMs);
		this.reconnectAttempts++;
		this.reconnectTimer = setTimeout(() => {
			this.reconnectTimer = null;
			this.openWebSocket();
		}, delay);
	}

	private sendAndAwaitResponse(frame: Uint8Array): Promise<Result<SyncResponse, LakeSyncError>> {
		return new Promise((resolve) => {
			if (!this.ws || !this._connected) {
				resolve(Err(new LSError("WebSocket not connected", "TRANSPORT_ERROR")));
				return;
			}

			// Reject any existing pending request
			if (this.pending) {
				this.pending.resolve(
					Err(new LSError("New request superseded pending request", "TRANSPORT_ERROR")),
				);
			}

			this.pending = {
				resolve,
				reject: (reason: Error) => {
					resolve(Err(new LSError(reason.message, "TRANSPORT_ERROR")));
				},
			};

			try {
				this.ws.send(frame);
			} catch (error) {
				const cause = toError(error);
				this.pending = null;
				resolve(Err(new LSError(`WebSocket send failed: ${cause.message}`, "TRANSPORT_ERROR")));
			}
		});
	}
}
