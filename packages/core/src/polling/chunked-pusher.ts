// ---------------------------------------------------------------------------
// ChunkedPusher — chunks deltas and pushes to a target with backpressure retry
// ---------------------------------------------------------------------------

import type { RowDelta, SyncPush } from "../delta/types";
import type { HLCTimestamp } from "../hlc/types";
import type { BackpressureError } from "../result/errors";
import type { Result } from "../result/result";
import type { PressureManager } from "./pressure-manager";

/** Minimal interface for a push target (avoids depending on @lakesync/gateway). */
export interface PushTarget {
	handlePush(push: SyncPush): unknown;
}

/**
 * Manages chunked pushing of deltas to a {@link PushTarget}.
 * Handles backpressure retry (flush + retry once) when a
 * {@link PressureManager} is present.
 *
 * No runtime type checks — the PressureManager is either provided
 * at construction (target supports flush) or null.
 */
export class ChunkedPusher {
	private readonly target: PushTarget;
	private readonly clientId: string;
	private readonly chunkSize: number;
	private readonly pressure: PressureManager | null;
	private pendingDeltas: RowDelta[] = [];

	constructor(config: {
		target: PushTarget;
		clientId: string;
		chunkSize: number;
		pressure: PressureManager | null;
	}) {
		this.target = config.target;
		this.clientId = config.clientId;
		this.chunkSize = config.chunkSize;
		this.pressure = config.pressure;
	}

	/**
	 * Accumulate a single delta. When `chunkSize` is reached, the pending
	 * deltas are automatically pushed (and flushed if needed).
	 */
	async accumulate(delta: RowDelta): Promise<void> {
		this.pendingDeltas.push(delta);
		if (this.pendingDeltas.length >= this.chunkSize) {
			await this.pushPendingChunk();
		}
	}

	/** Flush any remaining accumulated deltas. */
	async flush(): Promise<void> {
		if (this.pendingDeltas.length > 0) {
			await this.pushPendingChunk();
		}
	}

	/** Push deltas directly (single-shot, backward compat). */
	pushImmediate(deltas: RowDelta[]): void {
		if (deltas.length === 0) return;
		const push: SyncPush = {
			clientId: this.clientId,
			deltas,
			lastSeenHlc: 0n as HLCTimestamp,
		};
		this.target.handlePush(push);
	}

	private async pushPendingChunk(): Promise<void> {
		const chunk = this.pendingDeltas;
		this.pendingDeltas = [];
		await this.pushChunkWithFlush(chunk);
	}

	private async pushChunkWithFlush(chunk: RowDelta[]): Promise<void> {
		if (chunk.length === 0) return;

		// Pre-push: flush if pressure manager signals pressure
		if (this.pressure) {
			await this.pressure.checkAndFlush();
		}

		const push: SyncPush = {
			clientId: this.clientId,
			deltas: chunk,
			lastSeenHlc: 0n as HLCTimestamp,
		};

		const result = this.target.handlePush(push) as Result<unknown, BackpressureError> | undefined;

		// If handlePush returned a Result with backpressure, flush and retry once
		if (result && typeof result === "object" && "ok" in result && !result.ok) {
			if (this.pressure) {
				await this.pressure.forceFlush();
				this.target.handlePush(push);
			}
		}
	}
}
