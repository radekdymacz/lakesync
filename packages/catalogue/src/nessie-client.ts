import { Err, Ok } from "@lakesync/core";
import type { Result } from "@lakesync/core";
import {
	CatalogueError,
	type CatalogueConfig,
	type DataFile,
	type IcebergSchema,
	type PartitionSpec,
	type Snapshot,
	type TableMetadata,
} from "./types";

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
 */
export class NessieCatalogueClient {
	private readonly baseUri: string;
	private readonly warehouseUri: string;
	private readonly branch: string;

	constructor(config: CatalogueConfig) {
		this.baseUri = config.nessieUri.replace(/\/$/, "");
		this.warehouseUri = config.warehouseUri;
		this.branch = config.defaultBranch ?? "main";
	}

	/**
	 * Create a namespace (idempotent -- ignores 409 Conflict).
	 *
	 * @param namespace - Namespace parts, e.g. `["lakesync"]`
	 * @returns `Ok(void)` on success or if namespace already exists
	 */
	async createNamespace(
		namespace: string[],
	): Promise<Result<void, CatalogueError>> {
		const url = `${this.baseUri}/v1/namespaces`;
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
		const url = `${this.baseUri}/v1/namespaces`;

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
		const url = `${this.baseUri}/v1/namespaces/${ns}/tables`;
		const location = `${this.warehouseUri}/${namespace.join("/")}/${name}`;
		const body = {
			name,
			schema,
			"partition-spec": partitionSpec,
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
		const url = `${this.baseUri}/v1/namespaces/${ns}/tables/${encodeURIComponent(name)}`;

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
	 * Uses the Iceberg REST v1 commit-table endpoint to append data files.
	 * First loads the current table metadata to determine the current state,
	 * then commits the new files as an append operation.
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
		// First, load the current table metadata to get schema info
		const metadataResult = await this.loadTable(namespace, table);
		if (!metadataResult.ok) {
			return metadataResult;
		}

		const metadata = metadataResult.value;
		const currentSchemaId = metadata.metadata["current-schema-id"];

		const ns = encodeNamespace(namespace);
		const url = `${this.baseUri}/v1/namespaces/${ns}/tables/${encodeURIComponent(table)}`;

		const commitBody = {
			requirements: [
				{
					type: "assert-current-schema-id",
					"current-schema-id": currentSchemaId,
				},
			],
			updates: [
				{
					action: "append",
					"data-files": files,
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

		const snapshot = snapshots.find(
			(s) => s["snapshot-id"] === currentSnapshotId,
		);
		return Ok(snapshot ?? null);
	}
}
