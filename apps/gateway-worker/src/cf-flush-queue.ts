import {
	Err,
	FlushQueueError,
	type LakeAdapter,
	Ok,
	type Result,
	type RowDelta,
	toError,
} from "@lakesync/core";
import { bigintReplacer, type FlushContext, type FlushQueue } from "@lakesync/gateway";

/** Lightweight reference message published to the CF Queue. */
export interface MaterialiseJobMessage {
	/** R2 object key containing the full payload. */
	objectKey: string;
	/** Gateway that produced the flush. */
	gatewayId: string;
	/** Number of deltas in this batch (for observability). */
	deltaCount: number;
}

/**
 * Flush queue backed by Cloudflare Queues + R2 (claim-check pattern).
 *
 * CF Queue messages are limited to 128 KB, so the full payload is written
 * to R2 first. A lightweight reference message is then published to the
 * queue for the consumer to pick up.
 */
export class CloudflareFlushQueue implements FlushQueue {
	private readonly adapter: LakeAdapter;
	private readonly queue: Queue<MaterialiseJobMessage>;

	constructor(adapter: LakeAdapter, queue: Queue<MaterialiseJobMessage>) {
		this.adapter = adapter;
		this.queue = queue;
	}

	async publish(
		entries: ReadonlyArray<RowDelta>,
		context: FlushContext,
	): Promise<Result<void, FlushQueueError>> {
		if (entries.length === 0) return Ok(undefined);

		try {
			// Step 1: Write full payload to R2
			const payload = JSON.stringify({ entries, schemas: context.schemas }, bigintReplacer);
			const data = new TextEncoder().encode(payload);
			const timestamp = Date.now();
			const uuid = crypto.randomUUID();
			const objectKey = `materialise-jobs/${context.gatewayId}/${timestamp}-${uuid}.json`;

			const putResult = await this.adapter.putObject(objectKey, data, "application/json");
			if (!putResult.ok) {
				return Err(
					new FlushQueueError(`Failed to write materialise job to R2: ${putResult.error.message}`),
				);
			}

			// Step 2: Publish lightweight reference to queue
			await this.queue.send({
				objectKey,
				gatewayId: context.gatewayId,
				deltaCount: entries.length,
			});

			return Ok(undefined);
		} catch (error: unknown) {
			return Err(
				new FlushQueueError(`CloudflareFlushQueue publish failed: ${toError(error).message}`),
			);
		}
	}
}
