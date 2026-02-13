import type { LakeAdapter, Materialisable, RowDelta, TableSchema } from "@lakesync/core";
import { bigintReviver, processMaterialisation } from "@lakesync/gateway";
import type { MaterialiseJobMessage } from "./cf-flush-queue";

/** Configuration for the materialise queue consumer. */
export interface MaterialiseConsumerConfig {
	/** R2/S3 adapter to fetch payloads from. */
	adapter: LakeAdapter;
	/** Materialisation targets. */
	materialisers: ReadonlyArray<Materialisable>;
	/** Optional failure callback (per-table). */
	onFailure?: (table: string, deltaCount: number, error: Error) => void;
}

/**
 * Handle a batch of materialise job messages from a CF Queue.
 *
 * For each message:
 * 1. Fetch the full payload from R2
 * 2. Deserialise with `bigintReviver`
 * 3. Call `processMaterialisation()`
 * 4. Delete the R2 object on success
 * 5. `message.retry()` on failure
 */
export async function handleMaterialiseQueue(
	batch: MessageBatch<MaterialiseJobMessage>,
	config: MaterialiseConsumerConfig,
): Promise<void> {
	for (const message of batch.messages) {
		const { objectKey } = message.body;

		try {
			// Fetch payload from R2
			const getResult = await config.adapter.getObject(objectKey);
			if (!getResult.ok) {
				console.warn(
					`[lakesync] Failed to fetch materialise job ${objectKey}: ${getResult.error.message}`,
				);
				message.retry();
				continue;
			}

			// Deserialise
			const text = new TextDecoder().decode(getResult.value);
			const parsed = JSON.parse(text, bigintReviver) as {
				entries: RowDelta[];
				schemas: TableSchema[];
			};

			// Process materialisation
			await processMaterialisation(parsed.entries, parsed.schemas, {
				materialisers: config.materialisers,
				onFailure: config.onFailure,
			});

			// Success — delete the R2 object and ack the message
			await config.adapter.deleteObject(objectKey);
			message.ack();
		} catch (error: unknown) {
			const err = error instanceof Error ? error : new Error(String(error));
			console.warn(`[lakesync] Materialise consumer error for ${objectKey}: ${err.message}`);
			message.retry();
		}
	}
}
