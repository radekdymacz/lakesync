import type {
	Action,
	ActionDiscovery,
	ActionErrorResult,
	ActionResult,
	ConnectorDescriptor,
	HLC,
	LakeSyncError,
	Result,
} from "@lakesync/core";
import { isActionError } from "@lakesync/core";
import type { ActionQueue } from "../queue/action-types";
import type { TransportWithCapabilities } from "./transport";

/** Callback for action completion events. */
export type ActionCompleteCallback = (
	actionId: string,
	result: ActionResult | ActionErrorResult,
) => void;

/**
 * Manages the action queue: enqueue, process, dead-letter, and discovery.
 *
 * Extracted from SyncCoordinator to isolate action concerns from
 * push/pull sync orchestration.
 */
export class ActionProcessor {
	private readonly actionQueue: ActionQueue;
	private readonly transport: TransportWithCapabilities;
	private readonly clientId: string;
	private readonly hlc: HLC;
	private readonly maxRetries: number;
	private onComplete: ActionCompleteCallback | null = null;

	constructor(config: {
		actionQueue: ActionQueue;
		transport: TransportWithCapabilities;
		clientId: string;
		hlc: HLC;
		maxRetries: number;
	}) {
		this.actionQueue = config.actionQueue;
		this.transport = config.transport;
		this.clientId = config.clientId;
		this.hlc = config.hlc;
		this.maxRetries = config.maxRetries;
	}

	/** Register a callback for action completion events. */
	setOnComplete(cb: ActionCompleteCallback): void {
		this.onComplete = cb;
	}

	/**
	 * Submit an action for execution.
	 *
	 * Pushes the action to the ActionQueue and triggers immediate processing.
	 */
	async enqueue(params: {
		connector: string;
		actionType: string;
		params: Record<string, unknown>;
		idempotencyKey?: string;
	}): Promise<void> {
		const hlc = this.hlc.now();
		const { generateActionId } = await import("@lakesync/core");
		const actionId = await generateActionId({
			clientId: this.clientId,
			hlc,
			connector: params.connector,
			actionType: params.actionType,
			params: params.params,
		});

		const action: Action = {
			actionId,
			clientId: this.clientId,
			hlc,
			connector: params.connector,
			actionType: params.actionType,
			params: params.params,
			idempotencyKey: params.idempotencyKey,
		};

		await this.actionQueue.push(action);
		void this.processQueue();
	}

	/**
	 * Process pending actions from the action queue.
	 *
	 * Peeks at pending entries, sends them to the gateway via
	 * `transport.executeAction()`, and acks/nacks based on the result.
	 * Dead-letters entries after `maxRetries` failures.
	 */
	async processQueue(): Promise<void> {
		if (!this.transport.executeAction) return;

		const peekResult = await this.actionQueue.peek(100);
		if (!peekResult.ok || peekResult.value.length === 0) return;

		// Dead-letter entries that exceeded max retries
		const deadLettered = peekResult.value.filter((e) => e.retryCount >= this.maxRetries);
		const entries = peekResult.value.filter((e) => e.retryCount < this.maxRetries);

		if (deadLettered.length > 0) {
			console.warn(
				`[ActionProcessor] Dead-lettering ${deadLettered.length} actions after ${this.maxRetries} retries`,
			);
			await this.actionQueue.ack(deadLettered.map((e) => e.id));
			for (const entry of deadLettered) {
				this.onComplete?.(entry.action.actionId, {
					actionId: entry.action.actionId,
					code: "DEAD_LETTERED",
					message: `Action dead-lettered after ${this.maxRetries} retries`,
					retryable: false,
				});
			}
		}

		if (entries.length === 0) return;

		const ids = entries.map((e) => e.id);
		await this.actionQueue.markSending(ids);

		const transportResult = await this.transport.executeAction({
			clientId: this.clientId,
			actions: entries.map((e) => e.action),
		});

		if (transportResult.ok) {
			await this.actionQueue.ack(ids);

			for (const result of transportResult.value.results) {
				this.onComplete?.(result.actionId, result);
			}

			// Check if any results were retryable errors
			const retryableIds: string[] = [];
			for (let i = 0; i < transportResult.value.results.length; i++) {
				const result = transportResult.value.results[i]!;
				if (isActionError(result) && result.retryable) {
					retryableIds.push(ids[i]!);
				}
			}
		} else {
			// Transport-level failure — nack all for retry
			await this.actionQueue.nack(ids);
		}
	}

	/**
	 * Discover available connectors and their supported action types.
	 *
	 * Delegates to the transport's `describeActions()` method. Returns
	 * empty connectors when the transport does not support discovery.
	 */
	async describeActions(): Promise<Result<ActionDiscovery, LakeSyncError>> {
		if (!this.transport.describeActions) {
			return { ok: true, value: { connectors: {} } };
		}
		return this.transport.describeActions();
	}

	/**
	 * List available connector types and their configuration schemas.
	 *
	 * Delegates to the transport's `listConnectorTypes()` method. Returns
	 * an empty array when the transport does not support it.
	 */
	async listConnectorTypes(): Promise<Result<ConnectorDescriptor[], LakeSyncError>> {
		if (!this.transport.listConnectorTypes) {
			return { ok: true, value: [] };
		}
		return this.transport.listConnectorTypes();
	}
}
