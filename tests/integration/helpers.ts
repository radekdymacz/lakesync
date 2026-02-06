import type { LakeAdapter, ObjectInfo } from "@lakesync/adapter";
import {
	AdapterError,
	type ColumnDelta,
	type DeltaOp,
	Err,
	HLC,
	type HLCTimestamp,
	Ok,
	type Result,
	type RowDelta,
} from "@lakesync/core";
import { type GatewayConfig, SyncGateway } from "@lakesync/gateway";

/** Create a gateway with sensible test defaults. */
export function createTestGateway(
	adapter?: LakeAdapter,
	overrides?: Partial<GatewayConfig>,
): SyncGateway {
	return new SyncGateway(
		{
			gatewayId: "test-gateway",
			maxBufferBytes: 100 * 1024 * 1024, // 100 MiB
			maxBufferAgeMs: 60_000, // 1 minute
			flushFormat: "json" as const,
			...overrides,
		},
		adapter,
	);
}

/** Create a test HLC with an injectable clock. */
export function createTestHLC(startTime = 1_000_000): {
	hlc: HLC;
	advance: (ms: number) => void;
} {
	let now = startTime;
	const hlc = new HLC(() => now);
	return {
		hlc,
		advance: (ms: number) => {
			now += ms;
		},
	};
}

/** Build a RowDelta for testing. */
export function makeDelta(opts: {
	table?: string;
	rowId?: string;
	clientId?: string;
	hlc: HLCTimestamp;
	columns?: ColumnDelta[];
	op?: DeltaOp;
	deltaId?: string;
}): RowDelta {
	return {
		op: opts.op ?? "UPDATE",
		table: opts.table ?? "todos",
		rowId: opts.rowId ?? "row-1",
		clientId: opts.clientId ?? "client-a",
		columns: opts.columns ?? [{ column: "title", value: "Test" }],
		hlc: opts.hlc,
		deltaId: opts.deltaId ?? `delta-${Math.random().toString(36).slice(2)}`,
	};
}

/**
 * In-memory mock adapter that satisfies the {@link LakeAdapter} interface
 * using proper `Result` types from `@lakesync/core`.
 */
export function createMockAdapter(): LakeAdapter & {
	stored: Map<string, Uint8Array>;
} {
	const stored = new Map<string, Uint8Array>();

	return {
		stored,

		async putObject(
			path: string,
			data: Uint8Array,
			_contentType?: string,
		): Promise<Result<void, AdapterError>> {
			stored.set(path, data);
			return Ok(undefined);
		},

		async getObject(path: string): Promise<Result<Uint8Array, AdapterError>> {
			const data = stored.get(path);
			if (data) return Ok(data);
			return Err(new AdapterError(`Object not found: ${path}`));
		},

		async headObject(
			path: string,
		): Promise<Result<{ size: number; lastModified: Date }, AdapterError>> {
			const data = stored.get(path);
			if (data) return Ok({ size: data.length, lastModified: new Date() });
			return Err(new AdapterError(`Object not found: ${path}`));
		},

		async listObjects(prefix: string): Promise<Result<ObjectInfo[], AdapterError>> {
			const results: ObjectInfo[] = [...stored.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({
					key,
					size: data.length,
					lastModified: new Date(),
				}));
			return Ok(results);
		},

		async deleteObject(path: string): Promise<Result<void, AdapterError>> {
			stored.delete(path);
			return Ok(undefined);
		},

		async deleteObjects(paths: string[]): Promise<Result<void, AdapterError>> {
			for (const p of paths) stored.delete(p);
			return Ok(undefined);
		},
	};
}
