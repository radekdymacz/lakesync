/**
 * Manages online/offline lifecycle by listening to browser events.
 *
 * Extracted from SyncCoordinator to isolate network status detection
 * from sync orchestration logic. Guards all browser API access with
 * typeof checks for Node/SSR safety.
 */
export class OnlineManager {
	private _online = true;
	private onlineHandler: (() => void) | null = null;
	private offlineHandler: (() => void) | null = null;

	/** Callback invoked when the browser transitions to online. */
	onOnline: (() => void) | null = null;

	/** Callback invoked when the browser transitions to offline. */
	onOffline: (() => void) | null = null;

	/** Whether the client believes it is online. */
	get isOnline(): boolean {
		return this._online;
	}

	/**
	 * Register window online/offline event listeners.
	 * Reads initial status from `navigator.onLine` when available.
	 */
	start(): void {
		if (typeof window === "undefined") return;

		if (typeof navigator !== "undefined" && typeof navigator.onLine === "boolean") {
			this._online = navigator.onLine;
		}

		this.onlineHandler = () => {
			this._online = true;
			this.onOnline?.();
		};
		this.offlineHandler = () => {
			this._online = false;
			this.onOffline?.();
		};
		window.addEventListener("online", this.onlineHandler);
		window.addEventListener("offline", this.offlineHandler);
	}

	/** Remove online/offline listeners. */
	stop(): void {
		if (typeof window === "undefined") return;
		if (this.onlineHandler) {
			window.removeEventListener("online", this.onlineHandler);
			this.onlineHandler = null;
		}
		if (this.offlineHandler) {
			window.removeEventListener("offline", this.offlineHandler);
			this.offlineHandler = null;
		}
	}
}
