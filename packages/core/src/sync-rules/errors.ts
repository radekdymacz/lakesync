import { LakeSyncError } from "../result/errors";

/** Sync rule configuration or evaluation error */
export class SyncRuleError extends LakeSyncError {
	constructor(message: string, cause?: Error) {
		super(message, "SYNC_RULE_ERROR", cause);
	}
}
