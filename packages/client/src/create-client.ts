import type { TableSchema } from "@lakesync/core";
import { unwrapOrThrow } from "@lakesync/core";
import { LocalDB } from "./db/local-db";
import { registerSchema } from "./db/schema-registry";
import { MemoryQueue } from "./queue/memory-queue";
import type { SyncQueue } from "./queue/types";
import { SyncCoordinator, type SyncCoordinatorConfig } from "./sync/coordinator";
import type { SyncTransport } from "./sync/transport";
import { HttpTransport } from "./sync/transport-http";

/** Configuration for creating a LakeSync client via {@link createClient}. */
export interface CreateClientConfig {
	/** Database name for IndexedDB/OPFS storage. */
	name: string;
	/** Table schemas to register on open. */
	schemas: TableSchema[];
	/** Client identifier for sync. */
	clientId: string;
	/**
	 * Remote gateway connection details.
	 *
	 * Creates an {@link HttpTransport} under the hood. For in-process usage
	 * with {@link LocalTransport}, construct the coordinator manually instead.
	 */
	gateway: {
		url: string;
		gatewayId: string;
		token?: string;
	};
	/** Auto-sync interval in ms (default 10000). Set to 0 to disable. */
	autoSyncMs?: number;
	/**
	 * Storage backend for the local database.
	 * Defaults to auto-detection (IndexedDB when available, otherwise memory).
	 */
	backend?: "idb" | "memory";
	/** Sync queue implementation. Defaults to MemoryQueue. */
	queue?: SyncQueue;
	/** Additional SyncCoordinator options (merged with factory defaults). */
	coordinatorConfig?: Omit<SyncCoordinatorConfig, "queue" | "clientId" | "autoSyncIntervalMs">;
}

/** A fully-initialised LakeSync client returned by {@link createClient}. */
export interface LakeSyncClient {
	/** The sync coordinator managing push/pull operations. */
	coordinator: SyncCoordinator;
	/** The local SQLite database. */
	db: LocalDB;
	/** The transport used for gateway communication. */
	transport: SyncTransport;
	/** Stop auto-sync, close the database, and release resources. */
	destroy(): Promise<void>;
}

/**
 * Create a fully-initialised LakeSync client in one call.
 *
 * Opens a local SQLite database, registers the provided schemas,
 * creates an HTTP transport to the remote gateway, wires up a
 * SyncCoordinator, and optionally starts auto-sync.
 *
 * @example
 * ```ts
 * const client = await createClient({
 *   name: "my-app",
 *   schemas: [{ table: "todos", columns: [{ name: "title", type: "string" }] }],
 *   clientId: "client-1",
 *   gateway: { url: "https://gw.example.com", gatewayId: "gw-1", token: "jwt..." },
 * });
 *
 * // Use client.coordinator.tracker to insert/update/delete
 * // Use client.coordinator.syncOnce() for manual sync
 * // Call client.destroy() when done
 * ```
 */
export async function createClient(config: CreateClientConfig): Promise<LakeSyncClient> {
	// 1. Open the local database
	const dbResult = await LocalDB.open({
		name: config.name,
		backend: config.backend,
	});
	const db = unwrapOrThrow(dbResult);

	// 2. Register all schemas
	for (const schema of config.schemas) {
		unwrapOrThrow(await registerSchema(db, schema));
	}

	// 3. Create HTTP transport
	const transport = new HttpTransport({
		baseUrl: config.gateway.url,
		gatewayId: config.gateway.gatewayId,
		token: config.gateway.token ?? "",
	});

	// 4. Create SyncCoordinator
	const queue = config.queue ?? new MemoryQueue();
	const autoSyncMs = config.autoSyncMs ?? 10_000;

	const coordinator = new SyncCoordinator(db, transport, {
		...config.coordinatorConfig,
		queue,
		clientId: config.clientId,
		autoSyncIntervalMs: autoSyncMs,
	});

	// 5. Start auto-sync unless disabled
	if (autoSyncMs > 0) {
		coordinator.startAutoSync();
	}

	// 6. Return the client handle
	return {
		coordinator,
		db,
		transport,
		destroy: async () => {
			coordinator.stopAutoSync();
			await db.close();
		},
	};
}
