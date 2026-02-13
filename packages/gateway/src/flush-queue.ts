import {
	type FlushQueueError,
	type Materialisable,
	Ok,
	type Result,
	type RowDelta,
	type TableSchema,
} from "@lakesync/core";
import { processMaterialisation } from "./materialisation-processor";

/** Context passed alongside deltas when publishing to the flush queue. */
export interface FlushContext {
	/** Gateway that produced the flush. */
	gatewayId: string;
	/** Table schemas for materialisation. */
	schemas: ReadonlyArray<TableSchema>;
}

/**
 * Producer-only queue interface sitting between flush and materialisation.
 *
 * Consumer wiring is platform-specific — not part of this interface.
 */
export interface FlushQueue {
	/** Publish flushed deltas for downstream materialisation. */
	publish(
		entries: ReadonlyArray<RowDelta>,
		context: FlushContext,
	): Promise<Result<void, FlushQueueError>>;
}

/**
 * Type guard to check if an object implements the `FlushQueue` interface.
 *
 * Uses duck-typing (same pattern as `isMaterialisable`).
 */
export function isFlushQueue(value: unknown): value is FlushQueue {
	return (
		value !== null &&
		typeof value === "object" &&
		"publish" in value &&
		typeof (value as FlushQueue).publish === "function"
	);
}

/**
 * In-memory flush queue that calls `processMaterialisation()` inline.
 *
 * This is the default when no `flushQueue` is provided — behaviour is
 * identical to the previous synchronous materialisation path.
 */
export class MemoryFlushQueue implements FlushQueue {
	private readonly materialisers: ReadonlyArray<Materialisable>;
	private readonly onFailure?: (table: string, deltaCount: number, error: Error) => void;

	constructor(
		materialisers: ReadonlyArray<Materialisable>,
		onFailure?: (table: string, deltaCount: number, error: Error) => void,
	) {
		this.materialisers = materialisers;
		this.onFailure = onFailure;
	}

	async publish(
		entries: ReadonlyArray<RowDelta>,
		context: FlushContext,
	): Promise<Result<void, FlushQueueError>> {
		await processMaterialisation(entries, context.schemas, {
			materialisers: this.materialisers,
			onFailure: this.onFailure,
		});
		return Ok(undefined);
	}
}
