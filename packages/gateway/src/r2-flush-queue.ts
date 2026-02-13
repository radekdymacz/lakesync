import {
	Err,
	FlushQueueError,
	type LakeAdapter,
	Ok,
	type Result,
	type RowDelta,
	toError,
} from "@lakesync/core";
import type { FlushContext, FlushQueue } from "./flush-queue";
import { bigintReplacer } from "./json";

/**
 * Flush queue backed by any `LakeAdapter` (S3, R2, MinIO).
 *
 * Writes serialised delta batches to object storage under
 * `materialise-jobs/{gatewayId}/{timestamp}-{uuid}.json`. A separate
 * polling consumer lists this prefix, processes each batch, then
 * deletes the object. No queue service required.
 */
export class R2FlushQueue implements FlushQueue {
	private readonly adapter: LakeAdapter;

	constructor(adapter: LakeAdapter) {
		this.adapter = adapter;
	}

	async publish(
		entries: ReadonlyArray<RowDelta>,
		context: FlushContext,
	): Promise<Result<void, FlushQueueError>> {
		if (entries.length === 0) return Ok(undefined);

		try {
			const payload = JSON.stringify({ entries, schemas: context.schemas }, bigintReplacer);
			const data = new TextEncoder().encode(payload);
			const timestamp = Date.now();
			// Generate a random hex suffix for uniqueness (no crypto.randomUUID dependency)
			const rand = Array.from({ length: 16 }, () =>
				Math.floor(Math.random() * 256)
					.toString(16)
					.padStart(2, "0"),
			).join("");
			const objectKey = `materialise-jobs/${context.gatewayId}/${timestamp}-${rand}.json`;

			const result = await this.adapter.putObject(objectKey, data, "application/json");
			if (!result.ok) {
				return Err(new FlushQueueError(`Failed to write materialise job: ${result.error.message}`));
			}

			return Ok(undefined);
		} catch (error: unknown) {
			return Err(new FlushQueueError(`R2FlushQueue publish failed: ${toError(error).message}`));
		}
	}
}
