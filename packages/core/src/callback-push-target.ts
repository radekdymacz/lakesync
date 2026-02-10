// ---------------------------------------------------------------------------
// CallbackPushTarget — a PushTarget that delegates to a user-provided callback
// ---------------------------------------------------------------------------

import type { PushTarget } from "./base-poller";
import type { SyncPush } from "./delta/types";

/**
 * A simple PushTarget implementation that forwards every push to a
 * user-supplied callback. Useful for testing, logging, or lightweight
 * integrations where a full gateway is not required.
 */
export class CallbackPushTarget implements PushTarget {
	private readonly onPush: (push: SyncPush) => void | Promise<void>;

	constructor(onPush: (push: SyncPush) => void | Promise<void>) {
		this.onPush = onPush;
	}

	handlePush(push: SyncPush): void {
		this.onPush(push);
	}
}
