// ---------------------------------------------------------------------------
// WebSocket Manager — upgrade, message parsing, broadcast, client tracking
// ---------------------------------------------------------------------------

import type { IncomingMessage, Server } from "node:http";
import type { HLCTimestamp, ResolvedClaims, RowDelta, SyncRulesContext } from "@lakesync/core";
import { filterDeltas } from "@lakesync/core";
import type { ConfigStore, SyncGateway } from "@lakesync/gateway";
import {
	decodeSyncPull,
	decodeSyncPush,
	encodeBroadcastFrame,
	encodeSyncResponse,
	TAG_SYNC_PULL,
	TAG_SYNC_PUSH,
} from "@lakesync/proto";
import { WebSocketServer, type WebSocket as WsWebSocket } from "ws";
import type { AuthClaims } from "./auth";
import { verifyToken } from "./auth";
import { extractBearerToken } from "./auth-middleware";

/** Metadata stored for each connected WebSocket client. */
interface WsClientMeta {
	clientId: string;
	claims: Record<string, unknown>;
}

/**
 * Manages WebSocket connections, message handling, and broadcasting.
 *
 * Decouples the WebSocket protocol from the HTTP server lifecycle.
 */
export class WebSocketManager {
	private readonly wss: WebSocketServer;
	private readonly clients = new Map<WsWebSocket, WsClientMeta>();

	constructor(
		private readonly gateway: SyncGateway,
		private readonly configStore: ConfigStore,
		private readonly gatewayId: string,
		private readonly jwtSecret: string | undefined,
	) {
		this.wss = new WebSocketServer({ noServer: true });
	}

	/** Attach upgrade listener to an HTTP server. */
	attach(httpServer: Server): void {
		httpServer.on("upgrade", (req, socket, head) => {
			void this.handleUpgrade(req, socket, head);
		});
	}

	/**
	 * Broadcast ingested deltas to all connected WebSocket clients except the sender.
	 */
	broadcastDeltas(deltas: RowDelta[], serverHlc: HLCTimestamp, excludeClientId: string): void {
		if (deltas.length === 0) return;
		void this.broadcastDeltasAsync(deltas, serverHlc, excludeClientId);
	}

	/** Close all connections and shut down the WebSocket server. */
	close(): void {
		for (const ws of this.clients.keys()) {
			try {
				ws.close(1001, "Server shutting down");
			} catch {
				/* ignore */
			}
		}
		this.clients.clear();
		this.wss.close();
	}

	// -----------------------------------------------------------------------
	// Upgrade handling
	// -----------------------------------------------------------------------

	private async handleUpgrade(
		req: IncomingMessage,
		socket: import("node:stream").Duplex,
		head: Buffer,
	): Promise<void> {
		const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);

		// Authenticate
		let token = extractBearerToken(req);
		if (!token) {
			token = url.searchParams.get("token");
		}

		if (!token && this.jwtSecret) {
			socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
			socket.destroy();
			return;
		}

		let auth: AuthClaims | undefined;
		if (this.jwtSecret && token) {
			const authResult = await verifyToken(token, this.jwtSecret);
			if (!authResult.ok) {
				socket.write("HTTP/1.1 401 Unauthorized\r\n\r\n");
				socket.destroy();
				return;
			}
			auth = authResult.value;

			// Verify gateway ID
			const gwMatch = url.pathname.match(/^\/sync\/([^/]+)\/ws$/);
			if (!gwMatch || gwMatch[1] !== auth.gatewayId) {
				socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
				socket.destroy();
				return;
			}
		}

		this.wss.handleUpgrade(req, socket, head, (ws) => {
			const clientId = auth?.clientId ?? `anon-${crypto.randomUUID()}`;
			const claims: Record<string, unknown> = auth?.customClaims ?? {};

			this.clients.set(ws, { clientId, claims });

			ws.on("message", (data: Buffer) => {
				void this.handleMessage(ws, data, clientId, claims);
			});

			ws.on("close", () => {
				this.clients.delete(ws);
			});

			ws.on("error", () => {
				this.clients.delete(ws);
			});
		});
	}

	// -----------------------------------------------------------------------
	// Message handling
	// -----------------------------------------------------------------------

	private async handleMessage(
		ws: WsWebSocket,
		data: Buffer,
		clientId: string,
		claims: Record<string, unknown>,
	): Promise<void> {
		const bytes = new Uint8Array(data);
		if (bytes.length < 2) {
			ws.close(1002, "Message too short");
			return;
		}

		const tag = bytes[0];
		const payload = bytes.subarray(1);

		if (tag === TAG_SYNC_PUSH) {
			const decoded = decodeSyncPush(payload);
			if (!decoded.ok) {
				ws.close(1008, decoded.error.message);
				return;
			}

			const result = this.gateway.handlePush(decoded.value);
			if (!result.ok) {
				ws.close(1008, result.error.message);
				return;
			}

			// Send push response
			const response = encodeSyncResponse({
				deltas: [],
				serverHlc: result.value.serverHlc,
				hasMore: false,
			});
			if (response.ok) {
				ws.send(response.value);
			}

			// Broadcast to other clients
			this.broadcastDeltas(result.value.deltas, result.value.serverHlc, clientId);
		} else if (tag === TAG_SYNC_PULL) {
			const decoded = decodeSyncPull(payload);
			if (!decoded.ok) {
				ws.close(1008, decoded.error.message);
				return;
			}

			const context = await this.buildSyncRulesContext(claims);
			const pullResult = this.gateway.handlePull(
				decoded.value,
				context,
			) as import("@lakesync/core").Result<
				import("@lakesync/core").SyncResponse,
				{ message: string }
			>;
			if (!pullResult.ok) {
				ws.close(1008, pullResult.error.message);
				return;
			}

			const response = encodeSyncResponse(pullResult.value);
			if (response.ok) {
				ws.send(response.value);
			}
		} else {
			ws.close(1002, `Unknown message tag: 0x${tag!.toString(16).padStart(2, "0")}`);
		}
	}

	// -----------------------------------------------------------------------
	// Sync rules
	// -----------------------------------------------------------------------

	private async buildSyncRulesContext(
		claims: Record<string, unknown>,
	): Promise<SyncRulesContext | undefined> {
		const rules = await this.configStore.getSyncRules(this.gatewayId);
		if (!rules || rules.buckets.length === 0) {
			return undefined;
		}
		return { claims: claims as ResolvedClaims, rules };
	}

	// -----------------------------------------------------------------------
	// Broadcast
	// -----------------------------------------------------------------------

	private async broadcastDeltasAsync(
		deltas: RowDelta[],
		serverHlc: HLCTimestamp,
		excludeClientId: string,
	): Promise<void> {
		const rules = await this.configStore.getSyncRules(this.gatewayId);

		for (const [ws, meta] of this.clients) {
			if (meta.clientId === excludeClientId) continue;

			try {
				let filtered: RowDelta[] = deltas;
				if (rules && rules.buckets.length > 0) {
					const context: SyncRulesContext = {
						claims: meta.claims as ResolvedClaims,
						rules,
					};
					filtered = filterDeltas(deltas, context);
				}

				if (filtered.length === 0) continue;

				const frame = encodeBroadcastFrame({
					deltas: filtered,
					serverHlc,
					hasMore: false,
				});

				if (!frame.ok) continue;
				ws.send(frame.value);
			} catch {
				// Socket may have closed -- silently skip
			}
		}
	}
}
