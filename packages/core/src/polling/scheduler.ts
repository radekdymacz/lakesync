// ---------------------------------------------------------------------------
// PollingScheduler — pure lifecycle management for periodic polling
// ---------------------------------------------------------------------------

/**
 * Manages the start/stop lifecycle and timer scheduling for a poll function.
 * Has no knowledge of deltas, gateways, or sync protocol.
 */
export class PollingScheduler {
	private readonly pollFn: () => Promise<void>;
	private readonly intervalMs: number;
	private timer: ReturnType<typeof setTimeout> | null = null;
	private running = false;

	constructor(pollFn: () => Promise<void>, intervalMs: number) {
		this.pollFn = pollFn;
		this.intervalMs = intervalMs;
	}

	/** Start the polling loop. No-op if already running. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.schedule();
	}

	/** Stop the polling loop. */
	stop(): void {
		this.running = false;
		if (this.timer) {
			clearTimeout(this.timer);
			this.timer = null;
		}
	}

	/** Whether the scheduler is currently running. */
	get isRunning(): boolean {
		return this.running;
	}

	/** Execute a single poll cycle without the timer loop. */
	async pollOnce(): Promise<void> {
		return this.pollFn();
	}

	private schedule(): void {
		if (!this.running) return;
		this.timer = setTimeout(async () => {
			try {
				await this.pollFn();
			} catch {
				// Swallow errors — a failed poll must never crash the server
			}
			this.schedule();
		}, this.intervalMs);
	}
}
