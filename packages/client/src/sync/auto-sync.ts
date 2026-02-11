/**
 * Manages the auto-sync lifecycle: periodic interval timer and
 * visibility change handler.
 *
 * Extracted from SyncCoordinator to isolate timer/visibility concerns
 * from sync orchestration logic.
 */
export class AutoSyncScheduler {
	private intervalId: ReturnType<typeof setInterval> | null = null;
	private visibilityHandler: (() => void) | null = null;
	private readonly syncFn: () => Promise<void>;
	private readonly intervalMs: number;

	constructor(syncFn: () => Promise<void>, intervalMs: number) {
		this.syncFn = syncFn;
		this.intervalMs = intervalMs;
	}

	/** Whether auto-sync is currently running. */
	get isRunning(): boolean {
		return this.intervalId !== null;
	}

	/** Start periodic syncing and visibility-change-triggered sync. */
	start(): void {
		if (this.intervalId !== null) return;

		this.intervalId = setInterval(() => {
			void this.syncFn();
		}, this.intervalMs);

		this.setupVisibilitySync();
	}

	/** Stop periodic syncing and remove the visibility listener. */
	stop(): void {
		if (this.intervalId !== null) {
			clearInterval(this.intervalId);
			this.intervalId = null;
		}
		if (this.visibilityHandler) {
			if (typeof document !== "undefined") {
				document.removeEventListener("visibilitychange", this.visibilityHandler);
			}
			this.visibilityHandler = null;
		}
	}

	/** Register a visibility change listener to sync when the tab becomes visible. */
	private setupVisibilitySync(): void {
		this.visibilityHandler = () => {
			if (typeof document !== "undefined" && document.visibilityState === "visible") {
				void this.syncFn();
			}
		};
		if (typeof document !== "undefined") {
			document.addEventListener("visibilitychange", this.visibilityHandler);
		}
	}
}
