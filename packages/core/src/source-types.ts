import type { RowDelta, TableSchema } from "./delta";
import type { AdapterError, Result } from "./result";

/**
 * Cursor state for resuming a source from where it left off.
 *
 * The shape is source-specific (e.g. `{ lsn: "0/16B3748" }` for Postgres CDC).
 * Consumers persist this opaquely and pass it back via {@link Source.setCursor}.
 */
export type SourceCursor = Record<string, unknown>;

/**
 * Callback invoked when a source produces deltas.
 *
 * The source waits for the returned promise to resolve before continuing,
 * providing natural backpressure.
 */
export type OnDeltas = (deltas: RowDelta[]) => Promise<void>;

/**
 * A Source produces {@link RowDelta}s from an external system.
 *
 * Sources are the entry point for data into the sync engine. Any readable
 * system (Postgres WAL, Kafka topic, API poller, file watcher) can implement
 * this interface to feed deltas into a gateway.
 *
 * Lifecycle:
 * 1. Optionally call {@link setCursor} to resume from a previous position.
 * 2. Call {@link start} with an {@link OnDeltas} callback.
 * 3. The source calls `onDeltas` whenever new changes are available.
 * 4. Call {@link stop} to tear down. Idempotent.
 */
export interface Source {
	/** Human-readable name for logging and metrics. */
	readonly name: string;

	/**
	 * Start producing deltas. The source calls `onDeltas` whenever new
	 * changes are available. Resolves when the source is ready to produce.
	 */
	start(onDeltas: OnDeltas): Promise<Result<void, AdapterError>>;

	/** Stop producing deltas and release resources. Idempotent. */
	stop(): Promise<void>;

	/** Get current cursor position for persistence and resumption. */
	getCursor(): SourceCursor;

	/** Set cursor position (called before {@link start} for resumption). */
	setCursor(cursor: SourceCursor): void;

	/** Discover available tables and their schemas (optional). */
	discoverSchemas?(): Promise<Result<TableSchema[], AdapterError>>;
}
