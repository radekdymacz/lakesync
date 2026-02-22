import type { PullResult, PushResult } from "./engine";

/**
 * Context provided to sync strategies with access to sync operations.
 *
 * Each method performs a single sync operation. The strategy decides
 * ordering and which operations to perform.
 */
export interface SyncContext {
	/** Whether this is the first sync (lastSyncedHlc === 0). */
	readonly isFirstSync: boolean;
	/** Current sync mode. */
	readonly syncMode: "full" | "pushOnly" | "pullOnly";
	/** Perform initial sync via checkpoint download. */
	initialSync(): Promise<void>;
	/** Pull remote deltas from the gateway. */
	pull(): Promise<PullResult>;
	/** Push local deltas to the gateway. */
	push(): Promise<PushResult>;
	/** Process pending actions from the action queue. */
	processActions(): Promise<void>;
}

/**
 * Strategy that determines the ordering of sync operations.
 *
 * Decouples "what to sync" from "in what order".
 */
export interface SyncStrategy {
	/** Execute a sync cycle using the provided context. */
	execute(ctx: SyncContext): Promise<void>;
}

/**
 * Default strategy: pull before push.
 *
 * On first sync, performs initial sync (checkpoint download).
 * Then pulls remote deltas, pushes local deltas, and processes actions.
 */
export class PullFirstStrategy implements SyncStrategy {
	async execute(ctx: SyncContext): Promise<void> {
		if (ctx.syncMode !== "pushOnly") {
			if (ctx.isFirstSync) {
				await ctx.initialSync();
			}
			await ctx.pull();
		}
		if (ctx.syncMode !== "pullOnly") {
			await ctx.push();
		}
		await ctx.processActions();
	}
}

/**
 * Push-first strategy for offline-first apps.
 *
 * Pushes local deltas first, then pulls remote deltas.
 * Useful when local changes should be sent before receiving updates.
 */
export class PushFirstStrategy implements SyncStrategy {
	async execute(ctx: SyncContext): Promise<void> {
		if (ctx.syncMode !== "pullOnly") {
			await ctx.push();
		}
		if (ctx.syncMode !== "pushOnly") {
			if (ctx.isFirstSync) {
				await ctx.initialSync();
			}
			await ctx.pull();
		}
		await ctx.processActions();
	}
}
