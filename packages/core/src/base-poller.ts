// ---------------------------------------------------------------------------
// BaseSourcePoller — shared lifecycle and push logic for source connectors
// ---------------------------------------------------------------------------

import type { RowDelta, SyncPush } from "./delta/types";
import { HLC } from "./hlc/hlc";
import type { HLCTimestamp } from "./hlc/types";

/** Minimal interface for a push target (avoids depending on @lakesync/gateway). */
export interface PushTarget {
	handlePush(push: SyncPush): unknown;
}

/**
 * Base class for source pollers that poll an external API and push deltas
 * to a SyncGateway. Handles lifecycle (start/stop), scheduling, and push.
 */
export abstract class BaseSourcePoller {
	protected readonly gateway: PushTarget;
	protected readonly hlc: HLC;
	protected readonly clientId: string;
	private readonly intervalMs: number;
	private timer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	constructor(config: {
		name: string;
		intervalMs: number;
		gateway: PushTarget;
	}) {
		this.gateway = config.gateway;
		this.hlc = new HLC();
		this.clientId = `ingest:${config.name}`;
		this.intervalMs = config.intervalMs;
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

	/** Push collected deltas to the gateway. */
	protected pushDeltas(deltas: RowDelta[]): void {
		if (deltas.length === 0) return;
		const push: SyncPush = {
			clientId: this.clientId,
			deltas,
			lastSeenHlc: 0n as HLCTimestamp,
		};
		this.gateway.handlePush(push);
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
