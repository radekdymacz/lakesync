import { type DatabaseAdapter, isDatabaseAdapter, type LakeAdapter } from "@lakesync/adapter";
import {
	buildPartitionSpec,
	type DataFile,
	lakeSyncTableName,
	tableSchemaToIceberg,
} from "@lakesync/catalogue";
import {
	type AdapterError,
	AdapterNotFoundError,
	type ClockDriftError,
	Err,
	FlushError,
	filterDeltas,
	HLC,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
	resolveLWW,
	rowKey,
	type SchemaError,
	type SyncPull,
	type SyncPush,
	type SyncResponse,
	type SyncRulesContext,
	toError,
} from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { DeltaBuffer } from "./buffer";
import { bigintReplacer } from "./json";
import type { FlushEnvelope, GatewayConfig } from "./types";

export type { SyncPush, SyncPull, SyncResponse };

/**
 * Sync gateway -- coordinates delta ingestion, conflict resolution, and flush.
 *
 * Phase 1: plain TypeScript class (Phase 2 wraps in Cloudflare Durable Object).
 */
export class SyncGateway {
	private hlc: HLC;
	private buffer: DeltaBuffer;
	private config: GatewayConfig;
	private adapter: LakeAdapter | DatabaseAdapter | null;
	private flushing = false;

	constructor(config: GatewayConfig, adapter?: LakeAdapter | DatabaseAdapter) {
		this.config = config;
		this.hlc = new HLC();
		this.buffer = new DeltaBuffer();
		this.adapter = config.adapter ?? adapter ?? null;
	}

	/** Restore drained entries back to the buffer for retry. */
	private restoreEntries(entries: RowDelta[]): void {
		for (const entry of entries) {
			this.buffer.append(entry);
		}
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
	): Result<{ serverHlc: HLCTimestamp; accepted: number }, ClockDriftError | SchemaError> {
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
	 * When `msg.source` is set, pulls deltas from the named source adapter
	 * instead of the in-memory buffer. Otherwise, returns change events
	 * from the log since the given HLC. When a {@link SyncRulesContext} is
	 * provided, deltas are post-filtered by the client's bucket definitions
	 * and JWT claims. The buffer path over-fetches (3x the requested limit)
	 * and retries up to 5 times to fill the page.
	 *
	 * @param msg - The pull message specifying the cursor and limit.
	 * @param context - Optional sync rules context for row-level filtering.
	 * @returns A `Result` containing the matching deltas, server HLC, and pagination flag.
	 */
	handlePull(
		msg: SyncPull & { source: string },
		context?: SyncRulesContext,
	): Promise<Result<SyncResponse, AdapterNotFoundError | AdapterError>>;
	handlePull(msg: SyncPull, context?: SyncRulesContext): Result<SyncResponse, never>;
	handlePull(
		msg: SyncPull,
		context?: SyncRulesContext,
	):
		| Promise<Result<SyncResponse, AdapterNotFoundError | AdapterError>>
		| Result<SyncResponse, never> {
		if (msg.source) {
			return this.handleAdapterPull(msg, context);
		}

		return this.handleBufferPull(msg, context);
	}

	/** Pull from the in-memory buffer (original path). */
	private handleBufferPull(msg: SyncPull, context?: SyncRulesContext): Result<SyncResponse, never> {
		if (!context) {
			const { deltas, hasMore } = this.buffer.getEventsSince(msg.sinceHlc, msg.maxDeltas);
			const serverHlc = this.hlc.now();
			return Ok({ deltas, serverHlc, hasMore });
		}

		// Over-fetch and filter with bounded retry
		const maxRetries = 5;
		const overFetchMultiplier = 3;
		let cursor = msg.sinceHlc;
		const collected: RowDelta[] = [];

		for (let attempt = 0; attempt < maxRetries; attempt++) {
			const fetchLimit = msg.maxDeltas * overFetchMultiplier;
			const { deltas: raw, hasMore: rawHasMore } = this.buffer.getEventsSince(cursor, fetchLimit);

			if (raw.length === 0) {
				// No more data in buffer
				const serverHlc = this.hlc.now();
				return Ok({ deltas: collected, serverHlc, hasMore: false });
			}

			const filtered = filterDeltas(raw, context);
			collected.push(...filtered);

			if (collected.length >= msg.maxDeltas) {
				// Trim to exactly maxDeltas
				const trimmed = collected.slice(0, msg.maxDeltas);
				const serverHlc = this.hlc.now();
				return Ok({ deltas: trimmed, serverHlc, hasMore: true });
			}

			if (!rawHasMore) {
				// Exhausted the buffer
				const serverHlc = this.hlc.now();
				return Ok({ deltas: collected, serverHlc, hasMore: false });
			}

			// Advance cursor past the last examined delta
			cursor = raw[raw.length - 1]!.hlc;
		}

		// Exhausted retries — return what we have
		const serverHlc = this.hlc.now();
		const hasMore = collected.length >= msg.maxDeltas;
		const trimmed = collected.slice(0, msg.maxDeltas);
		return Ok({ deltas: trimmed, serverHlc, hasMore });
	}

	/** Pull from a named source adapter. */
	private async handleAdapterPull(
		msg: SyncPull,
		context?: SyncRulesContext,
	): Promise<Result<SyncResponse, AdapterNotFoundError | AdapterError>> {
		const adapter = this.config.sourceAdapters?.[msg.source!];
		if (!adapter) {
			return Err(new AdapterNotFoundError(`Source adapter "${msg.source}" not found`));
		}

		const queryResult = await adapter.queryDeltasSince(msg.sinceHlc);
		if (!queryResult.ok) {
			return Err(queryResult.error);
		}

		let deltas = queryResult.value;

		// Apply sync rules filtering if context is provided
		if (context) {
			deltas = filterDeltas(deltas, context);
		}

		// Paginate
		const hasMore = deltas.length > msg.maxDeltas;
		const sliced = deltas.slice(0, msg.maxDeltas);

		const serverHlc = this.hlc.now();
		return Ok({ deltas: sliced, serverHlc, hasMore });
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

		// Database adapter path — batch INSERT deltas directly
		if (isDatabaseAdapter(this.adapter)) {
			const entries = this.buffer.drain();
			if (entries.length === 0) {
				this.flushing = false;
				return Ok(undefined);
			}
			try {
				const result = await this.adapter.insertDeltas(entries);
				if (!result.ok) {
					this.restoreEntries(entries);
					this.flushing = false;
					return Err(new FlushError(`Database flush failed: ${result.error.message}`));
				}
				return Ok(undefined);
			} catch (error: unknown) {
				this.restoreEntries(entries);
				const message = toError(error).message;
				return Err(new FlushError(`Unexpected database flush failure: ${message}`));
			} finally {
				this.flushing = false;
			}
		}

		// Lake adapter path — write to object storage as Parquet or JSON
		// Capture byte size before draining (avoids recomputing)
		const byteSize = this.buffer.byteSize;
		const entries = this.buffer.drain();

		try {
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
					this.restoreEntries(entries);
					return Err(new FlushError("tableSchema required for Parquet flush"));
				}

				const parquetResult = await writeDeltasToParquet(entries, this.config.tableSchema);
				if (!parquetResult.ok) {
					this.restoreEntries(entries);
					return Err(parquetResult.error);
				}

				objectKey = `deltas/${date}/${this.config.gatewayId}/${min.toString()}-${max.toString()}.parquet`;
				data = parquetResult.value;
				contentType = "application/vnd.apache.parquet";
			}

			const result = await this.adapter.putObject(objectKey, data, contentType);

			if (!result.ok) {
				// Flush failed — restore buffer entries so they can be retried
				this.restoreEntries(entries);
				return Err(new FlushError(`Failed to write flush envelope: ${result.error.message}`));
			}

			// After successful adapter write, commit to catalogue (best-effort)
			if (this.config.catalogue && this.config.tableSchema) {
				await this.commitToCatalogue(objectKey, data.byteLength, entries.length);
			}

			return Ok(undefined);
		} catch (error: unknown) {
			// Unexpected throw (e.g. adapter threw instead of returning Err) — restore buffer
			this.restoreEntries(entries);
			const message = toError(error).message;
			return Err(new FlushError(`Unexpected flush failure: ${message}`));
		} finally {
			this.flushing = false;
		}
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
