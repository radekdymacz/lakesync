import type { SyncTransport } from "@lakesync/client";
import { HttpTransport, LocalTransport, SyncCoordinator } from "@lakesync/client";
import { SyncGateway } from "@lakesync/gateway";
import { createDevJwt } from "./auth";
import { initDatabase } from "./db";
import { setupUI } from "./ui";

/** Default gateway identifier used for both local and remote modes */
const GATEWAY_ID = "todo-gateway";

/**
 * Create the appropriate transport based on environment configuration.
 *
 * If VITE_GATEWAY_URL is set, uses HttpTransport to connect to a remote
 * gateway. Otherwise, creates an in-process SyncGateway with LocalTransport.
 */
async function createTransport(clientId: string): Promise<SyncTransport> {
	const gatewayUrl = import.meta.env.VITE_GATEWAY_URL as string | undefined;

	if (gatewayUrl) {
		// Remote mode — connect to an external gateway via HTTP
		const token = await createDevJwt(clientId, GATEWAY_ID);
		return new HttpTransport({
			baseUrl: gatewayUrl,
			gatewayId: GATEWAY_ID,
			token,
		});
	}

	// Local mode — in-process gateway for offline-first development
	const gateway = new SyncGateway({
		gatewayId: GATEWAY_ID,
		maxBufferBytes: 10 * 1024 * 1024, // 10 MiB
		maxBufferAgeMs: 30_000, // 30s
		flushFormat: "json",
	});
	return new LocalTransport(gateway);
}

async function main(): Promise<void> {
	// Initialise database
	const db = await initDatabase();

	// Generate a stable client identifier
	const clientId = `client-${crypto.randomUUID()}`;

	// Create transport (local or remote based on env)
	const transport = await createTransport(clientId);

	// Create sync coordinator and start background synchronisation
	const coordinator = new SyncCoordinator(db, transport, { clientId });
	coordinator.startAutoSync();

	// Set up the UI
	setupUI(coordinator);
}

main().catch(console.error);
