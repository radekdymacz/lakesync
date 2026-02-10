import { type DatabaseAdapter, isDatabaseAdapter, type LakeAdapter } from "@lakesync/adapter";
import {
	buildPartitionSpec,
	type DataFile,
	lakeSyncTableName,
	tableSchemaToIceberg,
} from "@lakesync/catalogue";
import {
	type Action,
	type ActionDescriptor,
	type ActionDiscovery,
	type ActionExecutionError,
	type ActionHandler,
	type ActionPush,
	type ActionResponse,
	type ActionResult,
	type ActionValidationError,
	type AdapterError,
	AdapterNotFoundError,
	type AuthContext,
	BackpressureError,
	type ClockDriftError,
	Err,
	FlushError,
	filterDeltas,
	HLC,
	type HLCTimestamp,
	type IngestTarget,
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
	validateAction,
} from "@lakesync/core";
import { writeDeltasToParquet } from "@lakesync/parquet";
import { DeltaBuffer } from "./buffer";
import { bigintReplacer } from "./json";
import type { FlushEnvelope, GatewayConfig, HandlePushResult } from "./types";

export type { SyncPush, SyncPull, SyncResponse };

/** Find the min and max HLC in a non-empty array of deltas. */
function hlcRange(entries: RowDelta[]): { min: HLCTimestamp; max: HLCTimestamp } {
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
 * Sync gateway -- coordinates delta ingestion, conflict resolution, and flush.
 *
 * Phase 1: plain TypeScript class (Phase 2 wraps in Cloudflare Durable Object).
 */
export class SyncGateway implements IngestTarget {
	private hlc: HLC;
	private buffer: DeltaBuffer;
	private config: GatewayConfig;
	private adapter: LakeAdapter | DatabaseAdapter | null;
	private flushing = false;
	private actionHandlers: Map<string, ActionHandler> = new Map();
	private executedActions: Set<string> = new Set();
	private idempotencyMap: Map<
		string,
		ActionResult | { actionId: string; code: string; message: string; retryable: boolean }
	> = new Map();

	constructor(config: GatewayConfig, adapter?: LakeAdapter | DatabaseAdapter) {
		this.config = { sourceAdapters: {}, ...config };
		this.hlc = new HLC();
		this.buffer = new DeltaBuffer();
		this.adapter = this.config.adapter ?? adapter ?? null;

		// Register action handlers from config
		if (config.actionHandlers) {
			for (const [name, handler] of Object.entries(config.actionHandlers)) {
				this.actionHandlers.set(name, handler);
			}
		}
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
	): Result<HandlePushResult, ClockDriftError | SchemaError | BackpressureError> {
		// Backpressure — reject when buffer exceeds threshold to prevent OOM
		const backpressureLimit = this.config.maxBackpressureBytes ?? this.config.maxBufferBytes * 2;
		if (this.buffer.byteSize >= backpressureLimit) {
			return Err(
				new BackpressureError(
					`Buffer backpressure exceeded (${this.buffer.byteSize} >= ${backpressureLimit} bytes)`,
				),
			);
		}

		let accepted = 0;
		const ingested: RowDelta[] = [];

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
					ingested.push(resolved.value);
				}
				// If resolution fails (should not happen with LWW on same row), skip
			} else {
				this.buffer.append(delta);
				ingested.push(delta);
			}

			accepted++;
		}

		const serverHlc = this.hlc.now();
		return Ok({ serverHlc, accepted, deltas: ingested });
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
					return Err(new FlushError(`Database flush failed: ${result.error.message}`));
				}
				return Ok(undefined);
			} catch (error: unknown) {
				this.restoreEntries(entries);
				return Err(new FlushError(`Unexpected database flush failure: ${toError(error).message}`));
			} finally {
				this.flushing = false;
			}
		}

		// Lake adapter path — write to object storage as Parquet or JSON
		const byteSize = this.buffer.byteSize;
		const entries = this.buffer.drain();

		try {
			const { min, max } = hlcRange(entries);
			const date = new Date().toISOString().split("T")[0];
			let objectKey: string;
			let data: Uint8Array;
			let contentType: string;

			if (this.config.flushFormat === "json") {
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
				data = new TextEncoder().encode(JSON.stringify(envelope, bigintReplacer));
				contentType = "application/json";
			} else {
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
				this.restoreEntries(entries);
				return Err(new FlushError(`Failed to write flush envelope: ${result.error.message}`));
			}

			if (this.config.catalogue && this.config.tableSchema) {
				await this.commitToCatalogue(objectKey, data.byteLength, entries.length);
			}

			return Ok(undefined);
		} catch (error: unknown) {
			this.restoreEntries(entries);
			return Err(new FlushError(`Unexpected flush failure: ${toError(error).message}`));
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

	/**
	 * Register a named source adapter for adapter-sourced pulls.
	 *
	 * @param name - Unique source name (used as the `source` parameter in pull requests).
	 * @param adapter - The database adapter to register.
	 */
	registerSource(name: string, adapter: DatabaseAdapter): void {
		this.config.sourceAdapters![name] = adapter;
	}

	/**
	 * Unregister a named source adapter.
	 *
	 * @param name - The source name to remove.
	 */
	unregisterSource(name: string): void {
		delete this.config.sourceAdapters![name];
	}

	/**
	 * List all registered source adapter names.
	 *
	 * @returns Array of registered source adapter names.
	 */
	listSources(): string[] {
		return Object.keys(this.config.sourceAdapters!);
	}

	/** Get per-table buffer statistics. */
	get tableStats(): Array<{ table: string; byteSize: number; deltaCount: number }> {
		return this.buffer.tableStats();
	}

	/**
	 * Flush a single table's deltas from the buffer.
	 *
	 * Drains only the specified table's deltas and flushes them,
	 * leaving other tables in the buffer.
	 */
	async flushTable(table: string): Promise<Result<void, FlushError>> {
		if (this.flushing) {
			return Err(new FlushError("Flush already in progress"));
		}
		if (!this.adapter) {
			return Err(new FlushError("No adapter configured"));
		}

		const entries = this.buffer.drainTable(table);
		if (entries.length === 0) {
			return Ok(undefined);
		}

		this.flushing = true;

		// Database adapter path
		if (isDatabaseAdapter(this.adapter)) {
			try {
				const result = await this.adapter.insertDeltas(entries);
				if (!result.ok) {
					this.restoreEntries(entries);
					return Err(new FlushError(`Table flush failed: ${result.error.message}`));
				}
				return Ok(undefined);
			} catch (error: unknown) {
				this.restoreEntries(entries);
				return Err(new FlushError(`Unexpected table flush failure: ${toError(error).message}`));
			} finally {
				this.flushing = false;
			}
		}

		// Lake adapter path
		try {
			const { min, max } = hlcRange(entries);
			const date = new Date().toISOString().split("T")[0];
			let objectKey: string;
			let data: Uint8Array;
			let contentType: string;

			if (this.config.flushFormat === "json" || !this.config.tableSchema) {
				const envelope: FlushEnvelope = {
					version: 1,
					gatewayId: this.config.gatewayId,
					createdAt: new Date().toISOString(),
					hlcRange: { min, max },
					deltaCount: entries.length,
					byteSize: 0,
					deltas: entries,
				};
				data = new TextEncoder().encode(JSON.stringify(envelope, bigintReplacer));
				objectKey = `deltas/${date}/${this.config.gatewayId}/${table}-${min.toString()}-${max.toString()}.json`;
				contentType = "application/json";
			} else {
				const parquetResult = await writeDeltasToParquet(entries, this.config.tableSchema);
				if (!parquetResult.ok) {
					this.restoreEntries(entries);
					return Err(parquetResult.error);
				}
				data = parquetResult.value;
				objectKey = `deltas/${date}/${this.config.gatewayId}/${table}-${min.toString()}-${max.toString()}.parquet`;
				contentType = "application/vnd.apache.parquet";
			}

			const result = await this.adapter.putObject(objectKey, data, contentType);
			if (!result.ok) {
				this.restoreEntries(entries);
				return Err(new FlushError(`Failed to write table flush: ${result.error.message}`));
			}
			return Ok(undefined);
		} catch (error: unknown) {
			this.restoreEntries(entries);
			return Err(new FlushError(`Unexpected table flush failure: ${toError(error).message}`));
		} finally {
			this.flushing = false;
		}
	}

	/**
	 * Get tables that exceed the per-table budget.
	 */
	getTablesExceedingBudget(): string[] {
		const budget = this.config.perTableBudgetBytes;
		if (!budget) return [];
		return this.buffer
			.tableStats()
			.filter((s) => s.byteSize >= budget)
			.map((s) => s.table);
	}

	/** Check if the buffer should be flushed based on config thresholds. */
	shouldFlush(): boolean {
		let effectiveMaxBytes = this.config.maxBufferBytes;

		// Reduce threshold for wide-column deltas
		const adaptive = this.config.adaptiveBufferConfig;
		if (adaptive && this.buffer.averageDeltaBytes > adaptive.wideColumnThreshold) {
			effectiveMaxBytes = Math.floor(effectiveMaxBytes * adaptive.reductionFactor);
		}

		return this.buffer.shouldFlush({
			maxBytes: effectiveMaxBytes,
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

	// -----------------------------------------------------------------------
	// Action handling
	// -----------------------------------------------------------------------

	/**
	 * Handle an incoming action push from a client.
	 *
	 * Iterates over actions, dispatches each to the registered ActionHandler
	 * by connector name. Supports idempotency via actionId deduplication and
	 * idempotencyKey mapping.
	 *
	 * @param msg - The action push containing one or more actions.
	 * @param context - Optional auth context for permission checks.
	 * @returns A `Result` containing results for each action.
	 */
	async handleAction(
		msg: ActionPush,
		context?: AuthContext,
	): Promise<Result<ActionResponse, ActionValidationError>> {
		const results: Array<
			ActionResult | { actionId: string; code: string; message: string; retryable: boolean }
		> = [];

		for (const action of msg.actions) {
			// Structural validation
			const validation = validateAction(action);
			if (!validation.ok) {
				return Err(validation.error);
			}

			// Idempotency — check actionId
			if (this.executedActions.has(action.actionId)) {
				const cached = this.idempotencyMap.get(action.actionId);
				if (cached) {
					results.push(cached);
					continue;
				}
				// Already executed but no cached result — skip
				continue;
			}

			// Idempotency — check idempotencyKey
			if (action.idempotencyKey) {
				const cached = this.idempotencyMap.get(`idem:${action.idempotencyKey}`);
				if (cached) {
					results.push(cached);
					continue;
				}
			}

			// Resolve handler
			const handler = this.actionHandlers.get(action.connector);
			if (!handler) {
				const errorResult = {
					actionId: action.actionId,
					code: "ACTION_NOT_SUPPORTED",
					message: `No action handler registered for connector "${action.connector}"`,
					retryable: false,
				};
				results.push(errorResult);
				this.cacheActionResult(action, errorResult);
				continue;
			}

			// Check action type is supported
			const supported = handler.supportedActions.some((d) => d.actionType === action.actionType);
			if (!supported) {
				const errorResult = {
					actionId: action.actionId,
					code: "ACTION_NOT_SUPPORTED",
					message: `Action type "${action.actionType}" not supported by connector "${action.connector}"`,
					retryable: false,
				};
				results.push(errorResult);
				this.cacheActionResult(action, errorResult);
				continue;
			}

			// Execute
			const execResult = await handler.executeAction(action, context);
			if (execResult.ok) {
				results.push(execResult.value);
				this.cacheActionResult(action, execResult.value);
			} else {
				const err = execResult.error;
				const errorResult = {
					actionId: action.actionId,
					code: err.code,
					message: err.message,
					retryable: "retryable" in err ? (err as ActionExecutionError).retryable : false,
				};
				results.push(errorResult);
				// Only cache non-retryable errors — retryable errors should be retried
				if (!errorResult.retryable) {
					this.cacheActionResult(action, errorResult);
				}
			}
		}

		const serverHlc = this.hlc.now();
		return Ok({ results, serverHlc });
	}

	/** Cache an action result for idempotency deduplication. */
	private cacheActionResult(
		action: Action,
		result: ActionResult | { actionId: string; code: string; message: string; retryable: boolean },
	): void {
		this.executedActions.add(action.actionId);
		this.idempotencyMap.set(action.actionId, result);
		if (action.idempotencyKey) {
			this.idempotencyMap.set(`idem:${action.idempotencyKey}`, result);
		}
	}

	/**
	 * Register a named action handler.
	 *
	 * @param name - Connector name (matches `Action.connector`).
	 * @param handler - The action handler to register.
	 */
	registerActionHandler(name: string, handler: ActionHandler): void {
		this.actionHandlers.set(name, handler);
	}

	/**
	 * Unregister a named action handler.
	 *
	 * @param name - The connector name to remove.
	 */
	unregisterActionHandler(name: string): void {
		this.actionHandlers.delete(name);
	}

	/**
	 * List all registered action handler names.
	 *
	 * @returns Array of registered connector names.
	 */
	listActionHandlers(): string[] {
		return [...this.actionHandlers.keys()];
	}

	/**
	 * Describe all registered action handlers and their supported actions.
	 *
	 * Returns a map of connector name to its {@link ActionDescriptor} array,
	 * enabling frontend discovery of available actions.
	 *
	 * @returns An {@link ActionDiscovery} object listing connectors and their actions.
	 */
	describeActions(): ActionDiscovery {
		const connectors: Record<string, ActionDescriptor[]> = {};
		for (const [name, handler] of this.actionHandlers) {
			connectors[name] = handler.supportedActions;
		}
		return { connectors };
	}
}
