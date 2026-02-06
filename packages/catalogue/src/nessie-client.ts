import type { Result } from "@lakesync/core";
import { Err, Ok } from "@lakesync/core";
import {
	type CatalogueConfig,
	CatalogueError,
	type DataFile,
	type IcebergSchema,
	type PartitionSpec,
	type Snapshot,
	type TableMetadata,
} from "./types";

/** Response shape returned by the Iceberg REST `/v1/config` endpoint. */
interface CatalogueConfigResponse {
	defaults?: Record<string, string>;
	overrides?: Record<string, string>;
}

/**
 * Encode a namespace array into a URL path segment.
 * For multi-level namespaces, parts are joined with the ASCII unit separator (%1F).
 */
function encodeNamespace(namespace: string[]): string {
	return namespace.map(encodeURIComponent).join("%1F");
}

/**
 * Typed client for the Nessie Iceberg REST Catalogue API v1.
 *
 * Wraps standard Iceberg REST endpoints exposed by the Nessie server,
 * returning `Result<T, CatalogueError>` from every public method.
 *
 * On first use, the client fetches `/v1/config` from the server to discover
 * the catalogue prefix (typically the Nessie branch name, e.g. `"main"`).
 * All subsequent requests include this prefix in the URL path as required
 * by the Iceberg REST specification: `/v1/{prefix}/namespaces/...`.
 */
export class NessieCatalogueClient {
	private readonly baseUri: string;
	private readonly warehouseUri: string;
	private prefixPromise: Promise<string> | null = null;

	constructor(config: CatalogueConfig) {
		this.baseUri = config.nessieUri.replace(/\/$/, "");
		this.warehouseUri = config.warehouseUri;
	}

	/**
	 * Resolve the catalogue prefix by calling the `/v1/config` endpoint.
	 *
	 * The Iceberg REST specification requires a prefix segment in all
	 * API paths (e.g. `/v1/{prefix}/namespaces`). Nessie returns this
	 * value in the `defaults.prefix` field of the config response.
	 *
	 * The result is cached so the config endpoint is only called once
	 * per client instance.
	 *
	 * @returns The resolved prefix string (e.g. `"main"`)
	 */
	private resolvePrefix(): Promise<string> {
		if (this.prefixPromise) {
			return this.prefixPromise;
		}

		this.prefixPromise = (async () => {
			try {
				const url = `${this.baseUri}/v1/config`;
				const response = await fetch(url, {
					method: "GET",
					headers: { Accept: "application/json" },
				});

				if (!response.ok) {
					// Fall back to empty prefix if config endpoint is unavailable
					return "";
				}

				const data = (await response.json()) as CatalogueConfigResponse;
				return data.defaults?.prefix ?? "";
			} catch {
				// Fall back to empty prefix on network errors
				return "";
			}
		})();

		return this.prefixPromise;
	}

	/**
	 * Build the base API path including the resolved prefix.
	 *
	 * @returns URL prefix such as `http://host/iceberg/v1/main` or
	 *          `http://host/iceberg/v1` when no prefix is configured
	 */
	private async apiBase(): Promise<string> {
		const prefix = await this.resolvePrefix();
		if (prefix) {
			return `${this.baseUri}/v1/${encodeURIComponent(prefix)}`;
		}
		return `${this.baseUri}/v1`;
	}

	/**
	 * Create a namespace (idempotent -- ignores 409 Conflict).
	 *
	 * @param namespace - Namespace parts, e.g. `["lakesync"]`
	 * @returns `Ok(void)` on success or if namespace already exists
	 */
	async createNamespace(namespace: string[]): Promise<Result<void, CatalogueError>> {
		const base = await this.apiBase();
		const url = `${base}/namespaces`;
		const body = {
			namespace,
			properties: {},
		};

		try {
			const response = await fetch(url, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(body),
			});

			// 409 Conflict means namespace already exists -- treat as success
			if (response.status === 409) {
				return Ok(undefined);
			}

			if (!response.ok) {
				const text = await response.text().catch(() => "");
				return Err(
					new CatalogueError(
						`Failed to create namespace: ${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`,
						response.status,
					),
				);
			}

			return Ok(undefined);
		} catch (error) {
			return Err(
				new CatalogueError(
					`Network error creating namespace: ${error instanceof Error ? error.message : String(error)}`,
					0,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/**
	 * List all namespaces in the catalogue.
	 *
	 * @returns Array of namespace arrays, e.g. `[["lakesync"], ["other"]]`
	 */
	async listNamespaces(): Promise<Result<string[][], CatalogueError>> {
		const base = await this.apiBase();
		const url = `${base}/namespaces`;

		try {
			const response = await fetch(url, {
				method: "GET",
				headers: { Accept: "application/json" },
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "");
				return Err(
					new CatalogueError(
						`Failed to list namespaces: ${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`,
						response.status,
					),
				);
			}

			const data = (await response.json()) as { namespaces: string[][] };
			return Ok(data.namespaces);
		} catch (error) {
			return Err(
				new CatalogueError(
					`Network error listing namespaces: ${error instanceof Error ? error.message : String(error)}`,
					0,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/**
	 * Create an Iceberg table within a namespace.
	 *
	 * @param namespace - Namespace parts, e.g. `["lakesync"]`
	 * @param name - Table name
	 * @param schema - Iceberg schema definition
	 * @param partitionSpec - Partition specification
	 */
	async createTable(
		namespace: string[],
		name: string,
		schema: IcebergSchema,
		partitionSpec: PartitionSpec,
	): Promise<Result<void, CatalogueError>> {
		const ns = encodeNamespace(namespace);
		const base = await this.apiBase();
		const url = `${base}/namespaces/${ns}/tables`;
		const location = `${this.warehouseUri}/${namespace.join("/")}/${name}`;
		const body = {
			name,
			schema,
			"partition-spec": partitionSpec,
			"stage-create": false,
			location,
			properties: {},
		};

		try {
			const response = await fetch(url, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(body),
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "");
				return Err(
					new CatalogueError(
						`Failed to create table ${namespace.join(".")}.${name}: ${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`,
						response.status,
					),
				);
			}

			return Ok(undefined);
		} catch (error) {
			return Err(
				new CatalogueError(
					`Network error creating table: ${error instanceof Error ? error.message : String(error)}`,
					0,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/**
	 * Load table metadata from the catalogue.
	 *
	 * @param namespace - Namespace parts, e.g. `["lakesync"]`
	 * @param name - Table name
	 * @returns Full table metadata including schemas, snapshots, and partition specs
	 */
	async loadTable(
		namespace: string[],
		name: string,
	): Promise<Result<TableMetadata, CatalogueError>> {
		const ns = encodeNamespace(namespace);
		const base = await this.apiBase();
		const url = `${base}/namespaces/${ns}/tables/${encodeURIComponent(name)}`;

		try {
			const response = await fetch(url, {
				method: "GET",
				headers: { Accept: "application/json" },
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "");
				return Err(
					new CatalogueError(
						`Failed to load table ${namespace.join(".")}.${name}: ${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`,
						response.status,
					),
				);
			}

			const data = (await response.json()) as TableMetadata;
			return Ok(data);
		} catch (error) {
			return Err(
				new CatalogueError(
					`Network error loading table: ${error instanceof Error ? error.message : String(error)}`,
					0,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/**
	 * Append data files to a table, creating a new snapshot.
	 *
	 * Uses the standard Iceberg REST v1 commit-table endpoint with
	 * `add-snapshot` and `set-snapshot-ref` metadata updates.
	 * First loads the current table metadata to determine the current state,
	 * then commits a new snapshot referencing the provided data files.
	 *
	 * @param namespace - Namespace parts, e.g. `["lakesync"]`
	 * @param table - Table name
	 * @param files - Data files to append
	 */
	async appendFiles(
		namespace: string[],
		table: string,
		files: DataFile[],
	): Promise<Result<void, CatalogueError>> {
		// Load the current table metadata to get schema and snapshot state
		const metadataResult = await this.loadTable(namespace, table);
		if (!metadataResult.ok) {
			return metadataResult;
		}

		const metadata = metadataResult.value;
		const currentSchemaId = metadata.metadata["current-schema-id"];

		const ns = encodeNamespace(namespace);
		const base = await this.apiBase();
		const url = `${base}/namespaces/${ns}/tables/${encodeURIComponent(table)}`;

		// Generate a unique snapshot ID
		const snapshotId = Date.now() * 1000 + Math.floor(Math.random() * 1000);
		const timestampMs = Date.now();

		// Compute summary from data files
		const totalRecords = files.reduce((sum, f) => sum + f["record-count"], 0);
		const totalSize = files.reduce((sum, f) => sum + f["file-size-in-bytes"], 0);

		const snapshot: Record<string, unknown> = {
			"snapshot-id": snapshotId,
			"timestamp-ms": timestampMs,
			summary: {
				operation: "append",
				"added-data-files": String(files.length),
				"added-records": String(totalRecords),
				"added-files-size": String(totalSize),
			},
			"schema-id": currentSchemaId,
		};

		// Include parent snapshot reference if one exists
		const currentSnapshotId = metadata.metadata["current-snapshot-id"];
		if (currentSnapshotId !== undefined) {
			snapshot["parent-snapshot-id"] = currentSnapshotId;
		}

		const commitBody = {
			requirements: [
				{
					type: "assert-current-schema-id",
					"current-schema-id": currentSchemaId,
				},
			],
			updates: [
				{
					action: "add-snapshot",
					snapshot,
				},
				{
					action: "set-snapshot-ref",
					"ref-name": "main",
					type: "branch",
					"snapshot-id": snapshotId,
				},
			],
		};

		try {
			const response = await fetch(url, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify(commitBody),
			});

			if (!response.ok) {
				const text = await response.text().catch(() => "");
				return Err(
					new CatalogueError(
						`Failed to append files to ${namespace.join(".")}.${table}: ${response.status} ${response.statusText}${text ? ` - ${text}` : ""}`,
						response.status,
					),
				);
			}

			return Ok(undefined);
		} catch (error) {
			return Err(
				new CatalogueError(
					`Network error appending files: ${error instanceof Error ? error.message : String(error)}`,
					0,
					error instanceof Error ? error : undefined,
				),
			);
		}
	}

	/**
	 * Get the current snapshot of a table, or null if no snapshots exist.
	 *
	 * @param namespace - Namespace parts, e.g. `["lakesync"]`
	 * @param table - Table name
	 * @returns The current snapshot, or `null` if the table has no snapshots
	 */
	async currentSnapshot(
		namespace: string[],
		table: string,
	): Promise<Result<Snapshot | null, CatalogueError>> {
		const metadataResult = await this.loadTable(namespace, table);
		if (!metadataResult.ok) {
			return metadataResult;
		}

		const metadata = metadataResult.value;
		const currentSnapshotId = metadata.metadata["current-snapshot-id"];
		const snapshots = metadata.metadata.snapshots ?? [];

		if (currentSnapshotId === undefined || snapshots.length === 0) {
			return Ok(null);
		}

		const snapshot = snapshots.find((s) => s["snapshot-id"] === currentSnapshotId);
		return Ok(snapshot ?? null);
	}
}
