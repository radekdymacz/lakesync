import type { DeltaOp, HLCTimestamp, RowDelta } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import type { SyncPullPayload, SyncPushPayload, SyncResponsePayload } from "../codec";
import {
	decodeRowDelta,
	decodeSyncPull,
	decodeSyncPush,
	decodeSyncResponse,
	encodeRowDelta,
	encodeSyncPull,
	encodeSyncPush,
	encodeSyncResponse,
} from "../codec";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const clock = new HLC(() => 1_700_000_000_000);

/** Create a test RowDelta with sensible defaults. */
function makeRowDelta(overrides?: Partial<RowDelta>): RowDelta {
	return {
		op: "INSERT" as DeltaOp,
		table: "users",
		rowId: "row-001",
		clientId: "client-abc",
		columns: [
			{ column: "name", value: "Alice" },
			{ column: "age", value: 30 },
			{ column: "active", value: true },
		],
		hlc: clock.now(),
		deltaId: "delta-001",
		...overrides,
	};
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("codec", () => {
	it("SyncPush roundtrip with multiple deltas", () => {
		const deltas: RowDelta[] = [
			makeRowDelta({ deltaId: "d-1", rowId: "r-1", op: "INSERT" }),
			makeRowDelta({
				deltaId: "d-2",
				rowId: "r-2",
				op: "UPDATE",
				columns: [{ column: "email", value: "alice@example.com" }],
			}),
			makeRowDelta({
				deltaId: "d-3",
				rowId: "r-3",
				op: "DELETE",
				columns: [],
			}),
		];

		const push: SyncPushPayload = {
			clientId: "client-xyz",
			deltas,
			lastSeenHlc: clock.now(),
		};

		const encoded = encodeSyncPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.clientId).toBe(push.clientId);
		expect(decoded.value.lastSeenHlc).toBe(push.lastSeenHlc);
		expect(decoded.value.deltas).toHaveLength(3);

		for (let i = 0; i < deltas.length; i++) {
			const original = deltas[i]!;
			const roundtripped = decoded.value.deltas[i]!;
			expect(roundtripped.op).toBe(original.op);
			expect(roundtripped.table).toBe(original.table);
			expect(roundtripped.rowId).toBe(original.rowId);
			expect(roundtripped.clientId).toBe(original.clientId);
			expect(roundtripped.deltaId).toBe(original.deltaId);
			expect(roundtripped.hlc).toBe(original.hlc);
			expect(roundtripped.columns).toEqual(original.columns);
		}
	});

	it("SyncPull roundtrip", () => {
		const pull: SyncPullPayload = {
			clientId: "client-pull-test",
			sinceHlc: HLC.encode(1_700_000_000_000, 42),
			maxDeltas: 500,
		};

		const encoded = encodeSyncPull(pull);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPull(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.clientId).toBe(pull.clientId);
		expect(decoded.value.sinceHlc).toBe(pull.sinceHlc);
		expect(decoded.value.maxDeltas).toBe(pull.maxDeltas);
	});

	it("SyncResponse roundtrip with has_more = true", () => {
		const response: SyncResponsePayload = {
			deltas: [
				makeRowDelta({ deltaId: "resp-1" }),
				makeRowDelta({ deltaId: "resp-2", op: "UPDATE" }),
			],
			serverHlc: HLC.encode(1_700_000_001_000, 7),
			hasMore: true,
		};

		const encoded = encodeSyncResponse(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncResponse(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.hasMore).toBe(true);
		expect(decoded.value.serverHlc).toBe(response.serverHlc);
		expect(decoded.value.deltas).toHaveLength(2);
		expect(decoded.value.deltas[0]?.deltaId).toBe("resp-1");
		expect(decoded.value.deltas[1]?.deltaId).toBe("resp-2");
	});

	it("HLC preserved through fixed64 (bigint exact equality)", () => {
		// Test with several HLC values including edge cases
		const hlcValues: HLCTimestamp[] = [
			HLC.encode(0, 0),
			HLC.encode(1, 0),
			HLC.encode(1_700_000_000_000, 0),
			HLC.encode(1_700_000_000_000, 65535),
			HLC.encode(281474976710655, 65535), // max 48-bit wall + max 16-bit counter
		];

		for (const hlc of hlcValues) {
			const delta = makeRowDelta({ hlc, deltaId: `hlc-test-${hlc}` });
			const encoded = encodeRowDelta(delta);
			expect(encoded.ok).toBe(true);
			if (!encoded.ok) continue;

			const decoded = decodeRowDelta(encoded.value);
			expect(decoded.ok).toBe(true);
			if (!decoded.ok) continue;

			// Exact bigint equality — no precision loss
			expect(decoded.value.hlc).toBe(hlc);
			expect(typeof decoded.value.hlc).toBe("bigint");
		}
	});

	it("empty deltas array", () => {
		const push: SyncPushPayload = {
			clientId: "empty-client",
			deltas: [],
			lastSeenHlc: HLC.encode(1_700_000_000_000, 0),
		};

		const encoded = encodeSyncPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(0);
		expect(decoded.value.clientId).toBe("empty-client");

		// Also test SyncResponse with empty deltas
		const response: SyncResponsePayload = {
			deltas: [],
			serverHlc: HLC.encode(1_700_000_000_000, 0),
			hasMore: false,
		};

		const encodedResp = encodeSyncResponse(response);
		expect(encodedResp.ok).toBe(true);
		if (!encodedResp.ok) return;

		const decodedResp = decodeSyncResponse(encodedResp.value);
		expect(decodedResp.ok).toBe(true);
		if (!decodedResp.ok) return;

		expect(decodedResp.value.deltas).toHaveLength(0);
		expect(decodedResp.value.hasMore).toBe(false);
	});

	it("large payload (1000 deltas) no corruption", () => {
		const deltas: RowDelta[] = [];
		const largeClock = new HLC(() => 1_700_000_000_000);

		for (let i = 0; i < 1000; i++) {
			deltas.push(
				makeRowDelta({
					deltaId: `bulk-${i}`,
					rowId: `row-${i}`,
					hlc: largeClock.now(),
					columns: [
						{ column: "index", value: i },
						{ column: "data", value: `payload-${i}` },
						{ column: "nested", value: { key: i, arr: [1, 2, 3] } },
					],
				}),
			);
		}

		const push: SyncPushPayload = {
			clientId: "bulk-client",
			deltas,
			lastSeenHlc: largeClock.now(),
		};

		const encoded = encodeSyncPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(1000);

		// Spot-check first, middle, and last deltas
		for (const idx of [0, 499, 999]) {
			const original = deltas[idx]!;
			const roundtripped = decoded.value.deltas[idx]!;
			expect(roundtripped.deltaId).toBe(original.deltaId);
			expect(roundtripped.rowId).toBe(original.rowId);
			expect(roundtripped.hlc).toBe(original.hlc);
			expect(roundtripped.columns).toEqual(original.columns);
		}
	});

	it("HLCTimestamp -> proto -> HLCTimestamp exact roundtrip", () => {
		// Verify that the branded HLCTimestamp survives the full encode/decode cycle
		// and remains usable with the HLC utility functions
		const wall = 1_700_123_456_789;
		const counter = 12345;
		const original = HLC.encode(wall, counter);

		const delta = makeRowDelta({ hlc: original, deltaId: "hlc-roundtrip" });

		const push: SyncPushPayload = {
			clientId: "hlc-test-client",
			deltas: [delta],
			lastSeenHlc: original,
		};

		const encoded = encodeSyncPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		const recoveredHlc = decoded.value.deltas[0]!.hlc;
		const recoveredLastSeen = decoded.value.lastSeenHlc;

		// Exact bigint equality
		expect(recoveredHlc).toBe(original);
		expect(recoveredLastSeen).toBe(original);

		// Verify the decoded HLC can be round-tripped through HLC.decode
		const components = HLC.decode(recoveredHlc);
		expect(components.wall).toBe(wall);
		expect(components.counter).toBe(counter);

		// Verify it can be compared with HLC.compare
		expect(HLC.compare(recoveredHlc, original)).toBe(0);

		// Verify the lastSeenHlc also decodes correctly
		const lastSeenComponents = HLC.decode(recoveredLastSeen);
		expect(lastSeenComponents.wall).toBe(wall);
		expect(lastSeenComponents.counter).toBe(counter);
	});
});
