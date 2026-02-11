// ---------------------------------------------------------------------------
// PressureManager — monitors buffer pressure and triggers flushes
// ---------------------------------------------------------------------------

import type { FlushError } from "../result/errors";
import type { Result } from "../result/result";

/**
 * Target that supports flush and buffer inspection.
 * Implemented by SyncGateway so pollers can trigger flushes to relieve memory pressure.
 */
export interface FlushableTarget {
	flush(): Promise<Result<void, FlushError>>;
	shouldFlush(): boolean;
	readonly bufferStats: { logSize: number; indexSize: number; byteSize: number };
}

/**
 * Monitors buffer pressure on a {@link FlushableTarget} and triggers
 * flushes when thresholds are exceeded. Created at construction time
 * only when the target supports flushing — no runtime type checks needed.
 */
export class PressureManager {
	private readonly target: FlushableTarget;
	private readonly memoryBudgetBytes: number | undefined;
	private readonly flushThreshold: number;

	constructor(config: {
		target: FlushableTarget;
		memoryBudgetBytes?: number;
		flushThreshold?: number;
	}) {
		this.target = config.target;
		this.memoryBudgetBytes = config.memoryBudgetBytes;
		this.flushThreshold = config.flushThreshold ?? 0.7;
	}

	/** Check buffer pressure and flush if thresholds are exceeded. */
	async checkAndFlush(): Promise<void> {
		if (this.shouldFlush()) {
			await this.target.flush();
		}
	}

	/** Force a flush regardless of current pressure. */
	async forceFlush(): Promise<void> {
		await this.target.flush();
	}

	private shouldFlush(): boolean {
		if (this.target.shouldFlush()) return true;
		if (this.memoryBudgetBytes != null) {
			const threshold = Math.floor(this.memoryBudgetBytes * this.flushThreshold);
			if (this.target.bufferStats.byteSize >= threshold) return true;
		}
		return false;
	}
}
