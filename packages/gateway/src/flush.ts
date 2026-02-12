import {
	type DatabaseAdapter,
	isDatabaseAdapter,
	isMaterialisable,
	type LakeAdapter,
} from "@lakesync/adapter";
import {
	buildPartitionSpec,
	type DataFile,
	lakeSyncTableName,
	type NessieCatalogueClient,
	tableSchemaToIceberg,
} from "@lakesync/catalogue";
import {
	Err,
	FlushError,
	HLC,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
	toError,
} from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { bigintReplacer } from "./json";
import type { FlushEnvelope } from "./types";

/** Configuration for flush operations. */
export interface FlushConfig {
	gatewayId: string;
	flushFormat?: "json" | "parquet";
	tableSchema?: TableSchema;
	catalogue?: NessieCatalogueClient;
	/** Optional callback invoked when materialisation fails. Useful for metrics/alerting. */
	onMaterialisationFailure?: (table: string, deltaCount: number, error: Error) => void;
}

/** Dependencies injected into flush operations. */
export interface FlushDeps {
	adapter: LakeAdapter | DatabaseAdapter;
	config: FlushConfig;
	restoreEntries: (entries: RowDelta[]) => void;
	schemas?: ReadonlyArray<TableSchema>;
}

/** Find the min and max HLC in a non-empty array of deltas. */
export function hlcRange(entries: RowDelta[]): { min: HLCTimestamp; max: HLCTimestamp } {
	let min = entries[0]!.hlc;
	let max = entries[0]!.hlc;
	for (let i = 1; i < entries.length; i++) {
		const hlc = entries[i]!.hlc;
		if (HLC.compare(hlc, min) < 0) min = hlc;
		if (HLC.compare(hlc, max) > 0) max = hlc;
	}
	return { min, max };
}

/** Strategy for flushing deltas to a specific adapter type. */
export interface FlushStrategy {
	/** Flush entries to the target adapter. */
	flush(
		entries: RowDelta[],
		byteSize: number,
		deps: FlushDeps,
		keyPrefix?: string,
	): Promise<Result<void, FlushError>>;
}

/**
 * Notify the onMaterialisationFailure callback if configured.
 * Extracts unique table names from deltas for per-table reporting.
 */
function notifyMaterialisationFailure(
	entries: RowDelta[],
	error: Error,
	config: FlushConfig,
): void {
	if (!config.onMaterialisationFailure) return;
	const tables = new Set(entries.map((e) => e.table));
	for (const table of tables) {
		const count = entries.filter((e) => e.table === table).length;
		config.onMaterialisationFailure(table, count, error);
	}
}

/** Strategy for flushing deltas to a DatabaseAdapter (batch INSERT). */
class DatabaseFlushStrategy implements FlushStrategy {
	async flush(
		entries: RowDelta[],
		_byteSize: number,
		deps: FlushDeps,
	): Promise<Result<void, FlushError>> {
		const adapter = deps.adapter as DatabaseAdapter;
		try {
			const result = await adapter.insertDeltas(entries);
			if (!result.ok) {
				deps.restoreEntries(entries);
				return Err(new FlushError(`Database flush failed: ${result.error.message}`));
			}

			// Materialise after successful delta insertion (non-fatal)
			if (deps.schemas && deps.schemas.length > 0 && isMaterialisable(adapter)) {
				try {
					const matResult = await adapter.materialise(entries, deps.schemas);
					if (!matResult.ok) {
						const error = new Error(matResult.error.message);
						console.warn(
							`[lakesync] Materialisation failed (${entries.length} deltas): ${matResult.error.message}`,
						);
						notifyMaterialisationFailure(entries, error, deps.config);
					}
				} catch (error: unknown) {
					const err = error instanceof Error ? error : new Error(String(error));
					console.warn(
						`[lakesync] Materialisation error (${entries.length} deltas): ${err.message}`,
					);
					notifyMaterialisationFailure(entries, err, deps.config);
				}
			}

			return Ok(undefined);
		} catch (error: unknown) {
			deps.restoreEntries(entries);
			return Err(new FlushError(`Unexpected database flush failure: ${toError(error).message}`));
		}
	}
}

/** Strategy for flushing deltas to a LakeAdapter as JSON. */
class LakeJsonFlushStrategy implements FlushStrategy {
	async flush(
		entries: RowDelta[],
		byteSize: number,
		deps: FlushDeps,
		keyPrefix?: string,
	): Promise<Result<void, FlushError>> {
		const adapter = deps.adapter as LakeAdapter;
		try {
			const { min, max } = hlcRange(entries);
			const date = new Date().toISOString().split("T")[0];
			const prefix = keyPrefix ? `${keyPrefix}-` : "";

			const envelope: FlushEnvelope = {
				version: 1,
				gatewayId: deps.config.gatewayId,
				createdAt: new Date().toISOString(),
				hlcRange: { min, max },
				deltaCount: entries.length,
				byteSize,
				deltas: entries,
			};

			const objectKey = `deltas/${date}/${deps.config.gatewayId}/${prefix}${min.toString()}-${max.toString()}.json`;
			const data = new TextEncoder().encode(JSON.stringify(envelope, bigintReplacer));

			const result = await adapter.putObject(objectKey, data, "application/json");
			if (!result.ok) {
				deps.restoreEntries(entries);
				return Err(new FlushError(`Failed to write flush envelope: ${result.error.message}`));
			}

			if (deps.config.catalogue && deps.config.tableSchema) {
				await commitToCatalogue(
					objectKey,
					data.byteLength,
					entries.length,
					deps.config.catalogue,
					deps.config.tableSchema,
				);
			}

			return Ok(undefined);
		} catch (error: unknown) {
			deps.restoreEntries(entries);
			return Err(new FlushError(`Unexpected flush failure: ${toError(error).message}`));
		}
	}
}

/** Strategy for flushing deltas to a LakeAdapter as Parquet. */
class LakeParquetFlushStrategy implements FlushStrategy {
	async flush(
		entries: RowDelta[],
		_byteSize: number,
		deps: FlushDeps,
		keyPrefix?: string,
	): Promise<Result<void, FlushError>> {
		const adapter = deps.adapter as LakeAdapter;
		try {
			if (!deps.config.tableSchema) {
				deps.restoreEntries(entries);
				return Err(new FlushError("tableSchema required for Parquet flush"));
			}

			const parquetResult = await writeDeltasToParquet(entries, deps.config.tableSchema);
			if (!parquetResult.ok) {
				deps.restoreEntries(entries);
				return Err(parquetResult.error);
			}

			const { min, max } = hlcRange(entries);
			const date = new Date().toISOString().split("T")[0];
			const prefix = keyPrefix ? `${keyPrefix}-` : "";
			const objectKey = `deltas/${date}/${deps.config.gatewayId}/${prefix}${min.toString()}-${max.toString()}.parquet`;
			const data = parquetResult.value;

			const result = await adapter.putObject(objectKey, data, "application/vnd.apache.parquet");
			if (!result.ok) {
				deps.restoreEntries(entries);
				return Err(new FlushError(`Failed to write flush envelope: ${result.error.message}`));
			}

			if (deps.config.catalogue && deps.config.tableSchema) {
				await commitToCatalogue(
					objectKey,
					data.byteLength,
					entries.length,
					deps.config.catalogue,
					deps.config.tableSchema,
				);
			}

			return Ok(undefined);
		} catch (error: unknown) {
			deps.restoreEntries(entries);
			return Err(new FlushError(`Unexpected flush failure: ${toError(error).message}`));
		}
	}
}

// Module-level singleton instances (strategies are stateless).
const databaseStrategy: FlushStrategy = new DatabaseFlushStrategy();
const lakeJsonStrategy: FlushStrategy = new LakeJsonFlushStrategy();
const lakeParquetStrategy: FlushStrategy = new LakeParquetFlushStrategy();

/** Select the appropriate flush strategy based on adapter type and format. */
function selectFlushStrategy(adapter: LakeAdapter | DatabaseAdapter, format?: string): FlushStrategy {
	if (isDatabaseAdapter(adapter)) return databaseStrategy;
	if (format === "json") return lakeJsonStrategy;
	return lakeParquetStrategy;
}

/**
 * Flush a set of entries to the configured adapter.
 *
 * Unifies both full-buffer flush and per-table flush. The `keyPrefix`
 * parameter, when provided, is prepended to the HLC range in the object key
 * (e.g. "todos" for per-table flush).
 */
export async function flushEntries(
	entries: RowDelta[],
	byteSize: number,
	deps: FlushDeps,
	keyPrefix?: string,
): Promise<Result<void, FlushError>> {
	const strategy = selectFlushStrategy(deps.adapter, deps.config.flushFormat);
	return strategy.flush(entries, byteSize, deps, keyPrefix);
}

/**
 * Best-effort catalogue commit. Registers the flushed Parquet file
 * as an Iceberg snapshot via Nessie. Errors are logged but do not
 * fail the flush — the Parquet file is the source of truth.
 */
export async function commitToCatalogue(
	objectKey: string,
	fileSizeInBytes: number,
	recordCount: number,
	catalogue: NessieCatalogueClient,
	schema: TableSchema,
): Promise<void> {
	const { namespace, name } = lakeSyncTableName(schema.table);
	const icebergSchema = tableSchemaToIceberg(schema);
	const partitionSpec = buildPartitionSpec(icebergSchema);

	// Ensure namespace exists (idempotent)
	await catalogue.createNamespace(namespace);

	// Ensure table exists (idempotent — catch 409)
	const createResult = await catalogue.createTable(namespace, name, icebergSchema, partitionSpec);
	if (!createResult.ok && createResult.error.statusCode !== 409) {
		return;
	}

	// Build DataFile reference
	const dataFile: DataFile = {
		content: "data",
		"file-path": objectKey,
		"file-format": "PARQUET",
		"record-count": recordCount,
		"file-size-in-bytes": fileSizeInBytes,
	};

	// Append file to table snapshot
	const appendResult = await catalogue.appendFiles(namespace, name, [dataFile]);
	if (!appendResult.ok && appendResult.error.statusCode === 409) {
		// On 409 conflict, retry once with fresh metadata
		await catalogue.appendFiles(namespace, name, [dataFile]);
	}
}
