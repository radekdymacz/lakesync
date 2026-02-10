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
	// Database adapter path — batch INSERT deltas directly
	if (isDatabaseAdapter(deps.adapter)) {
		try {
			const result = await deps.adapter.insertDeltas(entries);
			if (!result.ok) {
				deps.restoreEntries(entries);
				return Err(new FlushError(`Database flush failed: ${result.error.message}`));
			}

			// Materialise after successful delta insertion (non-fatal)
			if (deps.schemas && deps.schemas.length > 0 && isMaterialisable(deps.adapter)) {
				try {
					const matResult = await deps.adapter.materialise(entries, deps.schemas);
					if (!matResult.ok) {
						console.warn(
							`[lakesync] Materialisation failed (${entries.length} deltas): ${matResult.error.message}`,
						);
					}
				} catch (error: unknown) {
					console.warn(
						`[lakesync] Materialisation error (${entries.length} deltas): ${error instanceof Error ? error.message : String(error)}`,
					);
				}
			}

			return Ok(undefined);
		} catch (error: unknown) {
			deps.restoreEntries(entries);
			return Err(new FlushError(`Unexpected database flush failure: ${toError(error).message}`));
		}
	}

	// Lake adapter path — write to object storage as Parquet or JSON
	try {
		const { min, max } = hlcRange(entries);
		const date = new Date().toISOString().split("T")[0];
		const prefix = keyPrefix ? `${keyPrefix}-` : "";
		let objectKey: string;
		let data: Uint8Array;
		let contentType: string;

		if (deps.config.flushFormat === "json") {
			const envelope: FlushEnvelope = {
				version: 1,
				gatewayId: deps.config.gatewayId,
				createdAt: new Date().toISOString(),
				hlcRange: { min, max },
				deltaCount: entries.length,
				byteSize,
				deltas: entries,
			};

			objectKey = `deltas/${date}/${deps.config.gatewayId}/${prefix}${min.toString()}-${max.toString()}.json`;
			data = new TextEncoder().encode(JSON.stringify(envelope, bigintReplacer));
			contentType = "application/json";
		} else {
			// Parquet path
			if (!deps.config.tableSchema) {
				deps.restoreEntries(entries);
				return Err(new FlushError("tableSchema required for Parquet flush"));
			}

			const parquetResult = await writeDeltasToParquet(entries, deps.config.tableSchema);
			if (!parquetResult.ok) {
				deps.restoreEntries(entries);
				return Err(parquetResult.error);
			}

			objectKey = `deltas/${date}/${deps.config.gatewayId}/${prefix}${min.toString()}-${max.toString()}.parquet`;
			data = parquetResult.value;
			contentType = "application/vnd.apache.parquet";
		}

		const result = await deps.adapter.putObject(objectKey, data, contentType);
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
