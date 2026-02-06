import {
	HLC,
	type HLCTimestamp,
	type RowDelta,
	rowKey,
	Ok,
	Err,
	type Result,
	type ClockDriftError,
	FlushError,
	resolveLWW,
} from '@lakesync/core';
import type { LakeAdapter } from '@lakesync/adapter';
import { DeltaBuffer } from './buffer';
import type { GatewayConfig, FlushEnvelope } from './types';

/** SyncPush input message */
export interface SyncPush {
	/** Client that sent the push */
	clientId: string;
	/** Deltas to push */
	deltas: RowDelta[];
	/** Client's last-seen HLC */
	lastSeenHlc: HLCTimestamp;
}

/** SyncPull input message */
export interface SyncPull {
	/** Client that sent the pull */
	clientId: string;
	/** Return deltas with HLC strictly after this value */
	sinceHlc: HLCTimestamp;
	/** Maximum number of deltas to return */
	maxDeltas: number;
}

/** SyncResponse output */
export interface SyncResponse {
	/** Deltas matching the pull criteria */
	deltas: RowDelta[];
	/** Current server HLC */
	serverHlc: HLCTimestamp;
	/** Whether there are more deltas to fetch */
	hasMore: boolean;
}

/**
 * Sync gateway -- coordinates delta ingestion, conflict resolution, and flush.
 *
 * Phase 1: plain TypeScript class (Phase 2 wraps in Cloudflare Durable Object).
 */
export class SyncGateway {
	private hlc: HLC;
	private buffer: DeltaBuffer;
	private config: GatewayConfig;
	private adapter: LakeAdapter | null;
	private flushing = false;

	constructor(config: GatewayConfig, adapter?: LakeAdapter) {
		this.config = config;
		this.hlc = new HLC();
		this.buffer = new DeltaBuffer();
		this.adapter = adapter ?? null;
	}

	/**
	 * Handle an incoming push from a client.
	 *
	 * Validates HLC drift, resolves conflicts via LWW, and appends to the buffer.
	 *
	 * @param msg - The push message containing client deltas.
	 * @returns A `Result` with the new server HLC and accepted count,
	 *          or a `ClockDriftError` if the client clock is too far ahead.
	 */
	handlePush(
		msg: SyncPush,
	): Result<{ serverHlc: HLCTimestamp; accepted: number }, ClockDriftError> {
		let accepted = 0;

		for (const delta of msg.deltas) {
			// Check for idempotent re-push
			if (this.buffer.hasDelta(delta.deltaId)) {
				accepted++;
				continue;
			}

			// Validate HLC drift against server's physical clock
			const recvResult = this.hlc.recv(delta.hlc);
			if (!recvResult.ok) {
				return Err(recvResult.error);
			}

			// Check for conflict with existing state
			const key = rowKey(delta.table, delta.rowId);
			const existing = this.buffer.getRow(key);

			if (existing) {
				const resolved = resolveLWW(existing, delta);
				if (resolved.ok) {
					this.buffer.append(resolved.value);
				}
				// If resolution fails (should not happen with LWW on same row), skip
			} else {
				this.buffer.append(delta);
			}

			accepted++;
		}

		const serverHlc = this.hlc.now();
		return Ok({ serverHlc, accepted });
	}

	/**
	 * Handle a pull request from a client.
	 *
	 * Returns change events from the log since the given HLC.
	 *
	 * @param msg - The pull message specifying the cursor and limit.
	 * @returns A `Result` containing the matching deltas, server HLC, and pagination flag.
	 */
	handlePull(msg: SyncPull): Result<SyncResponse, never> {
		const { deltas, hasMore } = this.buffer.getEventsSince(
			msg.sinceHlc,
			msg.maxDeltas,
		);
		const serverHlc = this.hlc.now();
		return Ok({ deltas, serverHlc, hasMore });
	}

	/**
	 * Flush the buffer to the lake adapter.
	 *
	 * Writes a {@link FlushEnvelope} JSON file to the adapter. If the write
	 * fails, the buffer entries are restored so they can be retried.
	 *
	 * @returns A `Result` indicating success or a `FlushError`.
	 */
	async flush(): Promise<Result<void, FlushError>> {
		if (this.flushing) {
			return Err(new FlushError('Flush already in progress'));
		}
		if (this.buffer.logSize === 0) {
			return Ok(undefined);
		}
		if (!this.adapter) {
			return Err(new FlushError('No adapter configured'));
		}

		this.flushing = true;

		// Drain buffer before computing envelope
		const entries = this.buffer.drain();

		// Find HLC range
		let min = entries[0]!.hlc;
		let max = entries[0]!.hlc;
		for (const delta of entries) {
			if (HLC.compare(delta.hlc, min) < 0) min = delta.hlc;
			if (HLC.compare(delta.hlc, max) > 0) max = delta.hlc;
		}

		// Compute byte size from the entries (BigInt-safe serialisation)
		let byteSize = 0;
		for (const delta of entries) {
			byteSize += JSON.stringify(delta, (_key, value) =>
				typeof value === 'bigint' ? value.toString() : (value as unknown),
			).length;
		}

		const envelope: FlushEnvelope = {
			version: 1,
			gatewayId: this.config.gatewayId,
			createdAt: new Date().toISOString(),
			hlcRange: { min, max },
			deltaCount: entries.length,
			byteSize,
			deltas: entries,
		};

		// Object key: deltas/{YYYY-MM-DD}/{gatewayId}/{minHlc}-{maxHlc}.json
		const date = new Date().toISOString().split('T')[0];
		const objectKey = `deltas/${date}/${this.config.gatewayId}/${min.toString()}-${max.toString()}.json`;

		// Serialise BigInt values for JSON
		const jsonStr = JSON.stringify(envelope, (_key, value) =>
			typeof value === 'bigint' ? value.toString() : (value as unknown),
		);
		const data = new TextEncoder().encode(jsonStr);

		const result = await this.adapter.putObject(
			objectKey,
			data,
			'application/json',
		);

		if (!result.ok) {
			// Flush failed -- restore the buffer entries so they can be retried
			for (const entry of entries) {
				this.buffer.append(entry);
			}
			this.flushing = false;
			return Err(
				new FlushError(
					`Failed to write flush envelope: ${result.error.message}`,
				),
			);
		}

		this.flushing = false;
		return Ok(undefined);
	}

	/** Check if the buffer should be flushed based on config thresholds. */
	shouldFlush(): boolean {
		return this.buffer.shouldFlush({
			maxBytes: this.config.maxBufferBytes,
			maxAgeMs: this.config.maxBufferAgeMs,
		});
	}

	/** Get buffer statistics for monitoring. */
	get bufferStats(): {
		logSize: number;
		indexSize: number;
		byteSize: number;
	} {
		return {
			logSize: this.buffer.logSize,
			indexSize: this.buffer.indexSize,
			byteSize: this.buffer.byteSize,
		};
	}
}
