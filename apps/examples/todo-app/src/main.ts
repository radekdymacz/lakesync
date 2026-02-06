import { SyncGateway } from "@lakesync/gateway";
import { initDatabase } from "./db";
import { SyncCoordinator } from "./sync";
import { setupUI } from "./ui";

async function main(): Promise<void> {
	// Initialise database
	const db = await initDatabase();

	// Create gateway (no adapter — in-memory only)
	const gateway = new SyncGateway({
		gatewayId: "todo-gateway",
		maxBufferBytes: 10 * 1024 * 1024, // 10 MiB
		maxBufferAgeMs: 30_000, // 30s
		flushFormat: "json",
	});

	// Create sync coordinator
	const coordinator = new SyncCoordinator(db, gateway);

	// Set up the UI
	setupUI(coordinator);
}

main().catch(console.error);
