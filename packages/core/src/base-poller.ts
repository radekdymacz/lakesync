// ---------------------------------------------------------------------------
// BaseSourcePoller — shared lifecycle and push logic for source connectors
// ---------------------------------------------------------------------------

import type { RowDelta } from "./delta/types";
import { HLC } from "./hlc/hlc";
import { ChunkedPusher, type PushTarget } from "./polling/chunked-pusher";
import { PressureManager } from "./polling/pressure-manager";
import { PollingScheduler } from "./polling/scheduler";
import type { FlushError } from "./result/errors";
import type { Result } from "./result/result";

// Re-export PushTarget from its canonical location
export type { PushTarget } from "./polling/chunked-pusher";

/**
 * Extended push target that supports flush and buffer inspection.
 * Implemented by SyncGateway so pollers can trigger flushes to relieve memory pressure.
 */
export interface IngestTarget extends PushTarget {
	flush(): Promise<Result<void, FlushError>>;
	shouldFlush(): boolean;
	readonly bufferStats: { logSize: number; indexSize: number; byteSize: number };
}

/** Type guard: returns true if the target supports flush/shouldFlush/bufferStats. */
export function isIngestTarget(target: PushTarget): target is IngestTarget {
	return (
		typeof (target as IngestTarget).flush === "function" &&
		typeof (target as IngestTarget).shouldFlush === "function" &&
		"bufferStats" in target
	);
}

/** Memory configuration for the streaming accumulator. */
export interface PollerMemoryConfig {
	/** Number of deltas per push chunk (default 500). */
	chunkSize?: number;
	/** Approximate memory budget in bytes — triggers flush at 70% (default: no limit). */
	memoryBudgetBytes?: number;
	/** Proportion of memoryBudgetBytes at which to trigger a flush (default 0.7). */
	flushThreshold?: number;
}

const DEFAULT_CHUNK_SIZE = 500;

/**
 * Base class for source pollers that poll an external API and push deltas
 * to a SyncGateway.
 *
 * Composes {@link PollingScheduler} (lifecycle), {@link ChunkedPusher}
 * (chunked push with backpressure), and {@link PressureManager}
 * (memory-budget flush decisions).
 */
export abstract class BaseSourcePoller {
	protected readonly gateway: PushTarget;
	protected readonly hlc: HLC;
	protected readonly clientId: string;

	private readonly scheduler: PollingScheduler;
	private readonly pusher: ChunkedPusher;

	constructor(config: {
		name: string;
		intervalMs: number;
		gateway: PushTarget;
		memory?: PollerMemoryConfig;
	}) {
		this.gateway = config.gateway;
		this.hlc = new HLC();
		this.clientId = `ingest:${config.name}`;

		// PressureManager is only created when the target supports flush —
		// decided once at construction, no runtime type checks in the push path
		const pressure = isIngestTarget(config.gateway)
			? new PressureManager({
					target: config.gateway,
					memoryBudgetBytes: config.memory?.memoryBudgetBytes,
					flushThreshold: config.memory?.flushThreshold,
				})
			: null;

		this.pusher = new ChunkedPusher({
			target: config.gateway,
			clientId: this.clientId,
			chunkSize: config.memory?.chunkSize ?? DEFAULT_CHUNK_SIZE,
			pressure,
		});

		this.scheduler = new PollingScheduler(() => this.poll(), config.intervalMs);
	}

	/** Start the polling loop. */
	start(): void {
		this.scheduler.start();
	}

	/** Stop the polling loop. */
	stop(): void {
		this.scheduler.stop();
	}

	/** Whether the poller is currently running. */
	get isRunning(): boolean {
		return this.scheduler.isRunning;
	}

	/** Execute a single poll cycle. Subclasses implement their specific polling logic. */
	abstract poll(): Promise<void>;

	/** Export cursor state as a JSON-serialisable object for external persistence. */
	abstract getCursorState(): Record<string, unknown>;

	/** Restore cursor state from a previously exported snapshot. */
	abstract setCursorState(state: Record<string, unknown>): void;

	/**
	 * Execute a single poll cycle without the timer loop.
	 * Convenience for serverless consumers who trigger polls manually.
	 */
	async pollOnce(): Promise<void> {
		return this.scheduler.pollOnce();
	}

	/** Push collected deltas to the gateway (single-shot, backward compat). */
	protected pushDeltas(deltas: RowDelta[]): void {
		this.pusher.pushImmediate(deltas);
	}

	/**
	 * Accumulate a single delta. When `chunkSize` is reached, the pending
	 * deltas are automatically pushed (and flushed if needed).
	 */
	protected async accumulateDelta(delta: RowDelta): Promise<void> {
		await this.pusher.accumulate(delta);
	}

	/** Flush any remaining accumulated deltas. Call at the end of `poll()`. */
	protected async flushAccumulator(): Promise<void> {
		await this.pusher.flush();
	}
}
