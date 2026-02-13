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
	type DatabaseAdapter,
	Err,
	type FlushError,
	filterDeltas,
	HLC,
	type IngestTarget,
	type LakeAdapter,
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
import { FlushCoordinator } from "./flush-coordinator";
import { buildFlushQueue, type FlushQueue } from "./flush-queue";
import { SourceRegistry } from "./source-registry";
import type { GatewayConfig, HandlePushResult } from "./types";
import { ValidationPipeline } from "./validation-pipeline";

export type { SyncPush, SyncPull, SyncResponse };

/**
 * Sync gateway -- coordinates delta ingestion, conflict resolution, and flush.
 *
 * Thin facade composing DeltaBuffer, ActionDispatcher, SourceRegistry,
 * and FlushCoordinator. Public methods delegate to composed modules.
 */
export class SyncGateway implements IngestTarget {
	private hlc: HLC;
	readonly buffer: DeltaBuffer;
	readonly actions: ActionDispatcher;
	private config: GatewayConfig;
	private adapter: LakeAdapter | DatabaseAdapter | null;
	private readonly sources: SourceRegistry;
	private readonly flushCoordinator: FlushCoordinator;
	private readonly flushQueue: FlushQueue | undefined;
	private readonly validationPipeline: ValidationPipeline;

	constructor(config: GatewayConfig, adapter?: LakeAdapter | DatabaseAdapter) {
		this.config = { sourceAdapters: {}, ...config };
		this.hlc = new HLC();
		this.buffer = new DeltaBuffer();
		this.adapter = this.config.adapter ?? adapter ?? null;
		this.actions = new ActionDispatcher(config.actionHandlers);
		this.sources = new SourceRegistry(this.config.sourceAdapters);
		this.flushCoordinator = new FlushCoordinator();

		// Build validation pipeline from config
		this.validationPipeline = new ValidationPipeline();
		if (this.config.schemaManager) {
			const sm = this.config.schemaManager;
			this.validationPipeline.add((delta) => sm.validateDelta(delta));
		}

		this.flushQueue = buildFlushQueue(config, this.adapter);
	}

	// -----------------------------------------------------------------------
	// Push — pipeline of validation steps then buffer append
	// -----------------------------------------------------------------------

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
		// Step 1: Check backpressure
		const bpResult = this.checkBackpressure();
		if (!bpResult.ok) return bpResult;

		// Phase 1 — Validate all deltas (no side effects)
		for (const delta of msg.deltas) {
			if (this.buffer.hasDelta(delta.deltaId)) continue;

			const validationResult = this.validationPipeline.validate(delta);
			if (!validationResult.ok) return Err(validationResult.error);
		}

		// Phase 2 — Apply all validated deltas
		let accepted = 0;
		const ingested: RowDelta[] = [];

		for (const delta of msg.deltas) {
			if (this.buffer.hasDelta(delta.deltaId)) {
				accepted++;
				continue;
			}

			const recvResult = this.hlc.recv(delta.hlc);
			if (!recvResult.ok) return Err(recvResult.error);

			const key = rowKey(delta.table, delta.rowId);
			const existing = this.buffer.getRow(key);

			if (existing) {
				const resolved = resolveLWW(existing, delta);
				if (resolved.ok) {
					this.buffer.append(resolved.value);
					ingested.push(resolved.value);
				}
			} else {
				this.buffer.append(delta);
				ingested.push(delta);
			}

			accepted++;
		}

		const serverHlc = this.hlc.now();
		return Ok({ serverHlc, accepted, deltas: ingested });
	}

	/** Check buffer backpressure. */
	private checkBackpressure(): Result<void, BackpressureError> {
		const backpressureLimit = this.config.maxBackpressureBytes ?? this.config.maxBufferBytes * 2;
		if (this.buffer.byteSize >= backpressureLimit) {
			return Err(
				new BackpressureError(
					`Buffer backpressure exceeded (${this.buffer.byteSize} >= ${backpressureLimit} bytes)`,
				),
			);
		}
		return Ok(undefined);
	}

	// -----------------------------------------------------------------------
	// Pull — delegates to buffer or source registry
	// -----------------------------------------------------------------------

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
				const serverHlc = this.hlc.now();
				return Ok({ deltas: collected, serverHlc, hasMore: false });
			}

			const filtered = filterDeltas(raw, context);
			collected.push(...filtered);

			if (collected.length >= msg.maxDeltas) {
				const trimmed = collected.slice(0, msg.maxDeltas);
				const serverHlc = this.hlc.now();
				return Ok({ deltas: trimmed, serverHlc, hasMore: true });
			}

			if (!rawHasMore) {
				const serverHlc = this.hlc.now();
				return Ok({ deltas: collected, serverHlc, hasMore: false });
			}

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
		const adapter = this.sources.get(msg.source!);
		if (!adapter) {
			return Err(new AdapterNotFoundError(`Source adapter "${msg.source}" not found`));
		}

		const queryResult = await adapter.queryDeltasSince(msg.sinceHlc);
		if (!queryResult.ok) {
			return Err(queryResult.error);
		}

		let deltas = queryResult.value;

		if (context) {
			deltas = filterDeltas(deltas, context);
		}

		const hasMore = deltas.length > msg.maxDeltas;
		const sliced = deltas.slice(0, msg.maxDeltas);

		const serverHlc = this.hlc.now();
		return Ok({ deltas: sliced, serverHlc, hasMore });
	}

	// -----------------------------------------------------------------------
	// Flush — delegates to FlushCoordinator
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
		return this.flushCoordinator.flush(this.buffer, this.adapter, {
			config: {
				gatewayId: this.config.gatewayId,
				flushFormat: this.config.flushFormat,
				tableSchema: this.config.tableSchema,
				catalogue: this.config.catalogue,
			},
			schemas: this.config.schemas,
			flushQueue: this.flushQueue,
		});
	}

	/**
	 * Flush a single table's deltas from the buffer.
	 *
	 * Drains only the specified table's deltas and flushes them,
	 * leaving other tables in the buffer.
	 */
	async flushTable(table: string): Promise<Result<void, FlushError>> {
		return this.flushCoordinator.flushTable(table, this.buffer, this.adapter, {
			config: {
				gatewayId: this.config.gatewayId,
				flushFormat: this.config.flushFormat,
				tableSchema: this.config.tableSchema,
				catalogue: this.config.catalogue,
			},
			schemas: this.config.schemas,
			flushQueue: this.flushQueue,
		});
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
	// Source adapters — delegates to SourceRegistry
	// -----------------------------------------------------------------------

	/**
	 * Register a named source adapter for adapter-sourced pulls.
	 *
	 * @param name - Unique source name (used as the `source` parameter in pull requests).
	 * @param adapter - The database adapter to register.
	 */
	registerSource(name: string, adapter: DatabaseAdapter): void {
		this.sources.register(name, adapter);
	}

	/**
	 * Unregister a named source adapter.
	 *
	 * @param name - The source name to remove.
	 */
	unregisterSource(name: string): void {
		this.sources.unregister(name);
	}

	/**
	 * List all registered source adapter names.
	 *
	 * @returns Array of registered source adapter names.
	 */
	listSources(): string[] {
		return this.sources.list();
	}

	// -----------------------------------------------------------------------
	// Rehydration — restore persisted deltas without push validation
	// -----------------------------------------------------------------------

	/** Rehydrate the buffer with persisted deltas (bypasses push validation). */
	rehydrate(deltas: ReadonlyArray<RowDelta>): void {
		for (const delta of deltas) {
			this.buffer.append(delta);
		}
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
