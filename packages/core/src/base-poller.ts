// ---------------------------------------------------------------------------
// BaseSourcePoller — shared lifecycle and push logic for source connectors
// ---------------------------------------------------------------------------

import type { RowDelta, SyncPush } from "./delta/types";
import { HLC } from "./hlc/hlc";
import type { HLCTimestamp } from "./hlc/types";
import type { BackpressureError, FlushError } from "./result/errors";
import type { Result } from "./result/result";

/** Minimal interface for a push target (avoids depending on @lakesync/gateway). */
export interface PushTarget {
	handlePush(push: SyncPush): unknown;
}

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
const DEFAULT_FLUSH_THRESHOLD = 0.7;

/**
 * Base class for source pollers that poll an external API and push deltas
 * to a SyncGateway. Handles lifecycle (start/stop/schedule), and push.
 */
export abstract class BaseSourcePoller {
	protected readonly gateway: PushTarget;
	protected readonly hlc: HLC;
	protected readonly clientId: string;
	private readonly intervalMs: number;
	private timer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	private readonly chunkSize: number;
	private readonly memoryBudgetBytes: number | undefined;
	private readonly flushThreshold: number;
	private pendingDeltas: RowDelta[] = [];

	constructor(config: {
		name: string;
		intervalMs: number;
		gateway: PushTarget;
		memory?: PollerMemoryConfig;
	}) {
		this.gateway = config.gateway;
		this.hlc = new HLC();
		this.clientId = `ingest:${config.name}`;
		this.intervalMs = config.intervalMs;
		this.chunkSize = config.memory?.chunkSize ?? DEFAULT_CHUNK_SIZE;
		this.memoryBudgetBytes = config.memory?.memoryBudgetBytes;
		this.flushThreshold = config.memory?.flushThreshold ?? DEFAULT_FLUSH_THRESHOLD;
	}

	/** Start the polling loop. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.schedulePoll();
	}

	/** Stop the polling loop. */
	stop(): void {
		this.running = false;
		if (this.timer) {
			clearTimeout(this.timer);
			this.timer = null;
		}
	}

	/** Whether the poller is currently running. */
	get isRunning(): boolean {
		return this.running;
	}

	/** Execute a single poll cycle. Subclasses implement their specific polling logic. */
	abstract poll(): Promise<void>;

	/** Push collected deltas to the gateway (single-shot, backward compat). */
	protected pushDeltas(deltas: RowDelta[]): void {
		if (deltas.length === 0) return;
		const push: SyncPush = {
			clientId: this.clientId,
			deltas,
			lastSeenHlc: 0n as HLCTimestamp,
		};
		this.gateway.handlePush(push);
	}

	/**
	 * Accumulate a single delta. When `chunkSize` is reached, the pending
	 * deltas are automatically pushed (and flushed if needed).
	 */
	protected async accumulateDelta(delta: RowDelta): Promise<void> {
		this.pendingDeltas.push(delta);
		if (this.pendingDeltas.length >= this.chunkSize) {
			await this.pushPendingChunk();
		}
	}

	/** Flush any remaining accumulated deltas. Call at the end of `poll()`. */
	protected async flushAccumulator(): Promise<void> {
		if (this.pendingDeltas.length > 0) {
			await this.pushPendingChunk();
		}
	}

	/**
	 * Push a chunk of pending deltas. If the gateway is an IngestTarget,
	 * checks memory pressure and flushes before/after push when needed.
	 * On backpressure, flushes once and retries.
	 */
	private async pushPendingChunk(): Promise<void> {
		const chunk = this.pendingDeltas;
		this.pendingDeltas = [];
		await this.pushChunkWithFlush(chunk);
	}

	private async pushChunkWithFlush(chunk: RowDelta[]): Promise<void> {
		if (chunk.length === 0) return;

		const target = this.gateway;

		// Pre-push: flush if IngestTarget signals pressure
		if (isIngestTarget(target)) {
			if (this.shouldFlushTarget(target)) {
				await target.flush();
			}
		}

		const push: SyncPush = {
			clientId: this.clientId,
			deltas: chunk,
			lastSeenHlc: 0n as HLCTimestamp,
		};

		const result = target.handlePush(push) as Result<unknown, BackpressureError> | undefined;

		// If handlePush returned a Result with backpressure, flush and retry once
		if (result && typeof result === "object" && "ok" in result && !result.ok) {
			if (isIngestTarget(target)) {
				await target.flush();
				target.handlePush(push);
			}
		}
	}

	private shouldFlushTarget(target: IngestTarget): boolean {
		if (target.shouldFlush()) return true;
		if (this.memoryBudgetBytes != null) {
			const threshold = Math.floor(this.memoryBudgetBytes * this.flushThreshold);
			if (target.bufferStats.byteSize >= threshold) return true;
		}
		return false;
	}

	private schedulePoll(): void {
		if (!this.running) return;
		this.timer = setTimeout(async () => {
			try {
				await this.poll();
			} catch {
				// Swallow errors — a failed poll must never crash the server
			}
			this.schedulePoll();
		}, this.intervalMs);
	}
}
