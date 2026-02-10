import { type DatabaseAdapter, isDatabaseAdapter, type LakeAdapter } from "@lakesync/adapter";
import {
	type ActionDiscovery,
	type ActionHandler,
	type ActionPush,
	type ActionResponse,
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
} from "@lakesync/core";
import { ActionDispatcher } from "./action-dispatcher";
import { DeltaBuffer } from "./buffer";
import { flushEntries } from "./flush";
import type { GatewayConfig, HandlePushResult } from "./types";

export type { SyncPush, SyncPull, SyncResponse };

/**
 * Sync gateway -- coordinates delta ingestion, conflict resolution, and flush.
 *
 * Thin facade composing ActionDispatcher, DeltaBuffer, and flushEntries.
 */
export class SyncGateway implements IngestTarget {
	private hlc: HLC;
	readonly buffer: DeltaBuffer;
	readonly actions: ActionDispatcher;
	private config: GatewayConfig;
	private adapter: LakeAdapter | DatabaseAdapter | null;
	private flushing = false;

	constructor(config: GatewayConfig, adapter?: LakeAdapter | DatabaseAdapter) {
		this.config = { sourceAdapters: {}, ...config };
		this.hlc = new HLC();
		this.buffer = new DeltaBuffer();
		this.adapter = this.config.adapter ?? adapter ?? null;
		this.actions = new ActionDispatcher(config.actionHandlers);
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

	// -----------------------------------------------------------------------
	// Flush — delegates to flush module
	// -----------------------------------------------------------------------

	/**
	 * Flush the buffer to the configured adapter.
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

		// Database adapter path — drain after flushing flag is set
		if (isDatabaseAdapter(this.adapter)) {
			const entries = this.buffer.drain();
			if (entries.length === 0) {
				this.flushing = false;
				return Ok(undefined);
			}

			try {
				return await flushEntries(entries, 0, {
					adapter: this.adapter,
					config: {
						gatewayId: this.config.gatewayId,
						flushFormat: this.config.flushFormat,
						tableSchema: this.config.tableSchema,
						catalogue: this.config.catalogue,
					},
					restoreEntries: (e) => this.restoreEntries(e),
				});
			} finally {
				this.flushing = false;
			}
		}

		// Lake adapter path
		const byteSize = this.buffer.byteSize;
		const entries = this.buffer.drain();

		try {
			return await flushEntries(entries, byteSize, {
				adapter: this.adapter,
				config: {
					gatewayId: this.config.gatewayId,
					flushFormat: this.config.flushFormat,
					tableSchema: this.config.tableSchema,
					catalogue: this.config.catalogue,
				},
				restoreEntries: (e) => this.restoreEntries(e),
			});
		} finally {
			this.flushing = false;
		}
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

		try {
			return await flushEntries(
				entries,
				0,
				{
					adapter: this.adapter,
					config: {
						gatewayId: this.config.gatewayId,
						flushFormat: this.config.flushFormat,
						tableSchema: this.config.tableSchema,
						catalogue: this.config.catalogue,
					},
					restoreEntries: (e) => this.restoreEntries(e),
				},
				table,
			);
		} finally {
			this.flushing = false;
		}
	}

	// -----------------------------------------------------------------------
	// Actions — delegates to ActionDispatcher
	// -----------------------------------------------------------------------

	/** Handle an incoming action push from a client. */
	async handleAction(
		msg: ActionPush,
		context?: AuthContext,
	): Promise<Result<ActionResponse, ActionValidationError>> {
		return this.actions.dispatch(msg, () => this.hlc.now(), context);
	}

	/** Register a named action handler. */
	registerActionHandler(name: string, handler: ActionHandler): void {
		this.actions.registerHandler(name, handler);
	}

	/** Unregister a named action handler. */
	unregisterActionHandler(name: string): void {
		this.actions.unregisterHandler(name);
	}

	/** List all registered action handler names. */
	listActionHandlers(): string[] {
		return this.actions.listHandlers();
	}

	/** Describe all registered action handlers and their supported actions. */
	describeActions(): ActionDiscovery {
		return this.actions.describe();
	}

	// -----------------------------------------------------------------------
	// Source adapters
	// -----------------------------------------------------------------------

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

	// -----------------------------------------------------------------------
	// Buffer queries
	// -----------------------------------------------------------------------

	/** Get per-table buffer statistics. */
	get tableStats(): Array<{ table: string; byteSize: number; deltaCount: number }> {
		return this.buffer.tableStats();
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
}
