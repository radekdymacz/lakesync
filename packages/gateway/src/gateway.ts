import type { LakeAdapter } from "@lakesync/adapter";
import {
	type DataFile,
	buildPartitionSpec,
	lakeSyncTableName,
	tableSchemaToIceberg,
} from "@lakesync/catalogue";
import {
	type ClockDriftError,
	Err,
	FlushError,
	HLC,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	type SchemaError,
	resolveLWW,
	rowKey,
} from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { DeltaBuffer } from "./buffer";
import { bigintReplacer } from "./json";
import type { FlushEnvelope, GatewayConfig } from "./types";

/** SyncPush input message */
export interface SyncPush {
	/** Client that sent the push */
	clientId: string;
	/** Deltas to push */
	deltas: RowDelta[];
	/** Client's last-seen HLC */
	lastSeenHlc: HLCTimestamp;
}

/** SyncPull input message */
export interface SyncPull {
	/** Client that sent the pull */
	clientId: string;
	/** Return deltas with HLC strictly after this value */
	sinceHlc: HLCTimestamp;
	/** Maximum number of deltas to return */
	maxDeltas: number;
}

/** SyncResponse output */
export interface SyncResponse {
	/** Deltas matching the pull criteria */
	deltas: RowDelta[];
	/** Current server HLC */
	serverHlc: HLCTimestamp;
	/** Whether there are more deltas to fetch */
	hasMore: boolean;
}

/**
 * Sync gateway -- coordinates delta ingestion, conflict resolution, and flush.
 *
 * Phase 1: plain TypeScript class (Phase 2 wraps in Cloudflare Durable Object).
 */
export class SyncGateway {
	private hlc: HLC;
	private buffer: DeltaBuffer;
	private config: GatewayConfig;
	private adapter: LakeAdapter | null;
	private flushing = false;

	constructor(config: GatewayConfig, adapter?: LakeAdapter) {
		this.config = config;
		this.hlc = new HLC();
		this.buffer = new DeltaBuffer();
		this.adapter = adapter ?? null;
	}

	/**
	 * Handle an incoming push from a client.
	 *
	 * Validates HLC drift, resolves conflicts via LWW, and appends to the buffer.
	 *
	 * @param msg - The push message containing client deltas.
	 * @returns A `Result` with the new server HLC and accepted count,
	 *          or a `ClockDriftError` if the client clock is too far ahead.
	 */
	handlePush(
		msg: SyncPush,
	): Result<
		{ serverHlc: HLCTimestamp; accepted: number },
		ClockDriftError | SchemaError
	> {
		let accepted = 0;

		for (const delta of msg.deltas) {
			// Check for idempotent re-push
			if (this.buffer.hasDelta(delta.deltaId)) {
				accepted++;
				continue;
			}

			// Validate delta against the schema if a schema manager is configured
			if (this.config.schemaManager) {
				const schemaResult = this.config.schemaManager.validateDelta(delta);
				if (!schemaResult.ok) {
					return Err(schemaResult.error);
				}
			}

			// Validate HLC drift against server's physical clock
			const recvResult = this.hlc.recv(delta.hlc);
			if (!recvResult.ok) {
				return Err(recvResult.error);
			}

			// Check for conflict with existing state
			const key = rowKey(delta.table, delta.rowId);
			const existing = this.buffer.getRow(key);

			if (existing) {
				const resolved = resolveLWW(existing, delta);
				if (resolved.ok) {
					this.buffer.append(resolved.value);
				}
				// If resolution fails (should not happen with LWW on same row), skip
			} else {
				this.buffer.append(delta);
			}

			accepted++;
		}

		const serverHlc = this.hlc.now();
		return Ok({ serverHlc, accepted });
	}

	/**
	 * Handle a pull request from a client.
	 *
	 * Returns change events from the log since the given HLC.
	 *
	 * @param msg - The pull message specifying the cursor and limit.
	 * @returns A `Result` containing the matching deltas, server HLC, and pagination flag.
	 */
	handlePull(msg: SyncPull): Result<SyncResponse, never> {
		const { deltas, hasMore } = this.buffer.getEventsSince(msg.sinceHlc, msg.maxDeltas);
		const serverHlc = this.hlc.now();
		return Ok({ deltas, serverHlc, hasMore });
	}

	/**
	 * Flush the buffer to the lake adapter.
	 *
	 * Writes deltas as either a Parquet file (default) or a JSON
	 * {@link FlushEnvelope} to the adapter, depending on
	 * `config.flushFormat`. If the write fails, the buffer entries
	 * are restored so they can be retried.
	 *
	 * @returns A `Result` indicating success or a `FlushError`.
	 */
	async flush(): Promise<Result<void, FlushError>> {
		if (this.flushing) {
			return Err(new FlushError("Flush already in progress"));
		}
		if (this.buffer.logSize === 0) {
			return Ok(undefined);
		}
		if (!this.adapter) {
			return Err(new FlushError("No adapter configured"));
		}

		this.flushing = true;

		// Capture byte size before draining (avoids recomputing)
		const byteSize = this.buffer.byteSize;
		const entries = this.buffer.drain();

		// Find HLC range across all entries
		let min = entries[0]!.hlc;
		let max = entries[0]!.hlc;
		for (let i = 1; i < entries.length; i++) {
			const hlc = entries[i]!.hlc;
			if (HLC.compare(hlc, min) < 0) min = hlc;
			if (HLC.compare(hlc, max) > 0) max = hlc;
		}

		const date = new Date().toISOString().split("T")[0];
		let objectKey: string;
		let data: Uint8Array;
		let contentType: string;

		if (this.config.flushFormat === "json") {
			// Explicit JSON flush path
			const envelope: FlushEnvelope = {
				version: 1,
				gatewayId: this.config.gatewayId,
				createdAt: new Date().toISOString(),
				hlcRange: { min, max },
				deltaCount: entries.length,
				byteSize,
				deltas: entries,
			};

			objectKey = `deltas/${date}/${this.config.gatewayId}/${min.toString()}-${max.toString()}.json`;
			const jsonStr = JSON.stringify(envelope, bigintReplacer);
			data = new TextEncoder().encode(jsonStr);
			contentType = "application/json";
		} else {
			// Default: Parquet flush path
			if (!this.config.tableSchema) {
				for (const entry of entries) {
					this.buffer.append(entry);
				}
				this.flushing = false;
				return Err(new FlushError("tableSchema required for Parquet flush"));
			}

			const parquetResult = await writeDeltasToParquet(entries, this.config.tableSchema);
			if (!parquetResult.ok) {
				for (const entry of entries) {
					this.buffer.append(entry);
				}
				this.flushing = false;
				return Err(parquetResult.error);
			}

			objectKey = `deltas/${date}/${this.config.gatewayId}/${min.toString()}-${max.toString()}.parquet`;
			data = parquetResult.value;
			contentType = "application/vnd.apache.parquet";
		}

		const result = await this.adapter.putObject(objectKey, data, contentType);

		if (!result.ok) {
			// Flush failed — restore buffer entries so they can be retried
			for (const entry of entries) {
				this.buffer.append(entry);
			}
			this.flushing = false;
			return Err(new FlushError(`Failed to write flush envelope: ${result.error.message}`));
		}

		// After successful adapter write, commit to catalogue (best-effort)
		if (this.config.catalogue && this.config.tableSchema) {
			await this.commitToCatalogue(objectKey, data.byteLength, entries.length);
		}

		this.flushing = false;
		return Ok(undefined);
	}

	/**
	 * Best-effort catalogue commit. Registers the flushed Parquet file
	 * as an Iceberg snapshot via Nessie. Errors are logged but do not
	 * fail the flush — the Parquet file is the source of truth.
	 */
	private async commitToCatalogue(
		objectKey: string,
		fileSizeInBytes: number,
		recordCount: number,
	): Promise<void> {
		const catalogue = this.config.catalogue!;
		const schema = this.config.tableSchema!;

		const { namespace, name } = lakeSyncTableName(schema.table);
		const icebergSchema = tableSchemaToIceberg(schema);
		const partitionSpec = buildPartitionSpec(icebergSchema);

		// Ensure namespace exists (idempotent)
		await catalogue.createNamespace(namespace);

		// Ensure table exists (idempotent — catch 409)
		const createResult = await catalogue.createTable(namespace, name, icebergSchema, partitionSpec);
		if (!createResult.ok && createResult.error.statusCode !== 409) {
			console.warn(`Catalogue: failed to create table: ${createResult.error.message}`);
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
		if (!appendResult.ok) {
			// On 409 conflict, retry once with fresh metadata
			if (appendResult.error.statusCode === 409) {
				const retryResult = await catalogue.appendFiles(namespace, name, [dataFile]);
				if (!retryResult.ok) {
					console.warn(`Catalogue: retry append failed: ${retryResult.error.message}`);
				}
			} else {
				console.warn(`Catalogue: append failed: ${appendResult.error.message}`);
			}
		}
	}

	/** Check if the buffer should be flushed based on config thresholds. */
	shouldFlush(): boolean {
		return this.buffer.shouldFlush({
			maxBytes: this.config.maxBufferBytes,
			maxAgeMs: this.config.maxBufferAgeMs,
		});
	}

	/** Get buffer statistics for monitoring. */
	get bufferStats(): {
		logSize: number;
		indexSize: number;
		byteSize: number;
	} {
		return {
			logSize: this.buffer.logSize,
			indexSize: this.buffer.indexSize,
			byteSize: this.buffer.byteSize,
		};
	}
}
