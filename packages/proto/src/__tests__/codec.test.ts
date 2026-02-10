import type { DeltaOp, HLCTimestamp, RowDelta } from "@lakesync/core";
import { HLC } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import type { SyncPullPayload, SyncPushPayload, SyncResponsePayload } from "../codec";
import {
	decodeBroadcastFrame,
	decodeRowDelta,
	decodeSyncPull,
	decodeSyncPush,
	decodeSyncResponse,
	encodeBroadcastFrame,
	encodeRowDelta,
	encodeSyncPull,
	encodeSyncPush,
	encodeSyncResponse,
	TAG_BROADCAST,
	TAG_SYNC_PULL,
	TAG_SYNC_PUSH,
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

	it("broadcast frame roundtrip", () => {
		const response: SyncResponsePayload = {
			deltas: [makeRowDelta({ deltaId: "bc-1" }), makeRowDelta({ deltaId: "bc-2", op: "UPDATE" })],
			serverHlc: HLC.encode(1_700_000_001_000, 3),
			hasMore: false,
		};

		const encoded = encodeBroadcastFrame(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		// First byte should be TAG_BROADCAST
		expect(encoded.value[0]).toBe(TAG_BROADCAST);

		const decoded = decodeBroadcastFrame(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(2);
		expect(decoded.value.deltas[0]?.deltaId).toBe("bc-1");
		expect(decoded.value.deltas[1]?.deltaId).toBe("bc-2");
		expect(decoded.value.serverHlc).toBe(response.serverHlc);
		expect(decoded.value.hasMore).toBe(false);
	});

	it("broadcast frame rejects wrong tag", () => {
		// Create a valid SyncResponse and manually set wrong tag
		const response: SyncResponsePayload = {
			deltas: [],
			serverHlc: HLC.encode(1_700_000_000_000, 0),
			hasMore: false,
		};

		const encoded = encodeSyncResponse(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		// Prepend TAG_SYNC_PUSH instead of TAG_BROADCAST
		const badFrame = new Uint8Array(1 + encoded.value.length);
		badFrame[0] = TAG_SYNC_PUSH;
		badFrame.set(encoded.value, 1);

		const decoded = decodeBroadcastFrame(badFrame);
		expect(decoded.ok).toBe(false);
		if (!decoded.ok) {
			expect(decoded.error.message).toContain("Expected broadcast tag 0x03");
		}
	});

	it("broadcast frame rejects too-short input", () => {
		const decoded = decodeBroadcastFrame(new Uint8Array([0x03]));
		expect(decoded.ok).toBe(false);
		if (!decoded.ok) {
			expect(decoded.error.message).toContain("too short");
		}
	});

	it("tag constants have correct values", () => {
		expect(TAG_SYNC_PUSH).toBe(0x01);
		expect(TAG_SYNC_PULL).toBe(0x02);
		expect(TAG_BROADCAST).toBe(0x03);
	});
});

// ---------------------------------------------------------------------------
// Error handling tests
// ---------------------------------------------------------------------------

describe("codec error handling", () => {
	it("decodeSyncPush with corrupt bytes decodes without throwing (protobuf is lenient)", () => {
		// Protobuf is intentionally tolerant of unknown fields — random bytes
		// may decode to default values rather than erroring. The important thing
		// is that the codec never throws; it always returns a Result.
		const corrupt = new Uint8Array(64).map((_, i) => (i * 37 + 13) % 256);
		const result = decodeSyncPush(corrupt);
		expect(typeof result.ok).toBe("boolean");
	});

	it("decodeSyncPull with corrupt bytes returns error", () => {
		const corrupt = new Uint8Array(64).map((_, i) => (i * 41 + 7) % 256);
		const result = decodeSyncPull(corrupt);
		expect(result.ok).toBe(false);
	});

	it("decodeSyncResponse with corrupt bytes returns error", () => {
		const corrupt = new Uint8Array(64).map((_, i) => (i * 53 + 19) % 256);
		const result = decodeSyncResponse(corrupt);
		expect(result.ok).toBe(false);
	});

	it("decodeRowDelta with corrupt bytes returns error", () => {
		const corrupt = new Uint8Array(64).map((_, i) => (i * 59 + 23) % 256);
		const result = decodeRowDelta(corrupt);
		expect(result.ok).toBe(false);
	});

	it("decodeBroadcastFrame with only tag byte (no payload) returns error", () => {
		const result = decodeBroadcastFrame(new Uint8Array([TAG_BROADCAST]));
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("too short");
		}
	});

	it("decodeSyncPush with empty Uint8Array returns ok (protobuf default)", () => {
		// Empty bytes are valid protobuf — all fields take default values
		const result = decodeSyncPush(new Uint8Array([]));
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("");
			expect(result.value.deltas).toHaveLength(0);
		}
	});

	it("decodeSyncPull with empty Uint8Array returns ok (protobuf default)", () => {
		const result = decodeSyncPull(new Uint8Array([]));
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("");
			expect(result.value.maxDeltas).toBe(0);
		}
	});

	it("decodeSyncResponse with empty Uint8Array returns ok (protobuf default)", () => {
		const result = decodeSyncResponse(new Uint8Array([]));
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.deltas).toHaveLength(0);
			expect(result.value.hasMore).toBe(false);
		}
	});
});

// ---------------------------------------------------------------------------
// Edge case tests
// ---------------------------------------------------------------------------

describe("codec edge cases", () => {
	it("SyncPush roundtrip with empty string identifiers", () => {
		const delta = makeRowDelta({
			clientId: "",
			table: "",
			rowId: "",
			deltaId: "",
		});
		const push: SyncPushPayload = {
			clientId: "",
			deltas: [delta],
			lastSeenHlc: HLC.encode(0, 0),
		};

		const encoded = encodeSyncPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.clientId).toBe("");
		expect(decoded.value.deltas[0]!.table).toBe("");
		expect(decoded.value.deltas[0]!.rowId).toBe("");
		expect(decoded.value.deltas[0]!.clientId).toBe("");
		expect(decoded.value.deltas[0]!.deltaId).toBe("");
	});

	it("column values with unicode and emoji strings", () => {
		const delta = makeRowDelta({
			deltaId: "unicode-test",
			columns: [
				{ column: "greeting", value: "こんにちは世界" },
				{ column: "emoji", value: "🎉🚀💡" },
				{ column: "mixed", value: "Hello 你好 مرحبا 🌍" },
				{ column: "zalgo", value: "H̷̗̀ë̸̞l̶̜̈l̷̰̐o̵̞̎" },
			],
		});

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.columns).toEqual(delta.columns);
	});

	it("column values with deeply nested JSON objects", () => {
		const nested = {
			a: { b: { c: { d: { e: { f: { g: "deep" } } } } } },
			meta: { tags: [{ id: 1, labels: { en: "hello", fr: "bonjour" } }] },
		};
		const delta = makeRowDelta({
			deltaId: "nested-test",
			columns: [{ column: "data", value: nested }],
		});

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.columns[0]!.value).toEqual(nested);
	});

	it("column values with arrays", () => {
		const delta = makeRowDelta({
			deltaId: "array-test",
			columns: [
				{ column: "tags", value: ["a", "b", "c"] },
				{ column: "numbers", value: [1, 2, 3, 4, 5] },
				{ column: "mixed", value: [1, "two", true, null, { key: "val" }] },
				{
					column: "nested",
					value: [
						[1, 2],
						[3, 4],
					],
				},
			],
		});

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.columns).toEqual(delta.columns);
	});

	it("column values with null", () => {
		const delta = makeRowDelta({
			deltaId: "null-test",
			columns: [
				{ column: "nullable", value: null },
				{ column: "name", value: "Alice" },
			],
		});

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.columns[0]!.value).toBeNull();
		expect(decoded.value.columns[1]!.value).toBe("Alice");
	});

	it("column values with very long strings (10KB+)", () => {
		const longString = "x".repeat(10_240);
		const delta = makeRowDelta({
			deltaId: "long-string-test",
			columns: [{ column: "content", value: longString }],
		});

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.columns[0]!.value).toBe(longString);
	});

	it("SyncPull with maxDeltas=0", () => {
		const pull: SyncPullPayload = {
			clientId: "zero-max",
			sinceHlc: HLC.encode(1_700_000_000_000, 0),
			maxDeltas: 0,
		};

		const encoded = encodeSyncPull(pull);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPull(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.maxDeltas).toBe(0);
	});

	it("SyncPull with maxDeltas at uint32 max (4294967295)", () => {
		const pull: SyncPullPayload = {
			clientId: "max-deltas",
			sinceHlc: HLC.encode(1_700_000_000_000, 0),
			maxDeltas: 4294967295,
		};

		const encoded = encodeSyncPull(pull);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPull(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.maxDeltas).toBe(4294967295);
	});

	it("single delta roundtrip in push", () => {
		const delta = makeRowDelta({ deltaId: "single-1" });
		const push: SyncPushPayload = {
			clientId: "single-client",
			deltas: [delta],
			lastSeenHlc: clock.now(),
		};

		const encoded = encodeSyncPush(push);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncPush(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(1);
		expect(decoded.value.deltas[0]!.deltaId).toBe("single-1");
		expect(decoded.value.deltas[0]!.columns).toEqual(delta.columns);
	});

	it("single delta roundtrip in response", () => {
		const delta = makeRowDelta({ deltaId: "single-resp-1" });
		const response: SyncResponsePayload = {
			deltas: [delta],
			serverHlc: clock.now(),
			hasMore: false,
		};

		const encoded = encodeSyncResponse(response);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeSyncResponse(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.deltas).toHaveLength(1);
		expect(decoded.value.deltas[0]!.deltaId).toBe("single-resp-1");
	});
});

// ---------------------------------------------------------------------------
// HLC boundary tests
// ---------------------------------------------------------------------------

describe("codec HLC boundaries", () => {
	it("HLC with counter=0 (just wall time)", () => {
		const hlc = HLC.encode(1_700_000_000_000, 0);
		const delta = makeRowDelta({ hlc, deltaId: "hlc-counter-zero" });

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.hlc).toBe(hlc);
		const parts = HLC.decode(decoded.value.hlc);
		expect(parts.wall).toBe(1_700_000_000_000);
		expect(parts.counter).toBe(0);
	});

	it("HLC with counter=65535 (max 16-bit)", () => {
		const hlc = HLC.encode(1_700_000_000_000, 65535);
		const delta = makeRowDelta({ hlc, deltaId: "hlc-counter-max" });

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.hlc).toBe(hlc);
		const parts = HLC.decode(decoded.value.hlc);
		expect(parts.wall).toBe(1_700_000_000_000);
		expect(parts.counter).toBe(65535);
	});

	it("HLC with max 48-bit wall time", () => {
		const maxWall = 281474976710655; // 2^48 - 1
		const hlc = HLC.encode(maxWall, 65535);
		const delta = makeRowDelta({ hlc, deltaId: "hlc-max-wall" });

		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.hlc).toBe(hlc);
		const parts = HLC.decode(decoded.value.hlc);
		expect(parts.wall).toBe(maxWall);
		expect(parts.counter).toBe(65535);
	});
});

// ---------------------------------------------------------------------------
// Determinism tests
// ---------------------------------------------------------------------------

describe("codec determinism", () => {
	it("same payload encoded twice produces identical bytes", () => {
		const delta = makeRowDelta({ deltaId: "det-1", hlc: HLC.encode(1_700_000_000_000, 42) });
		const push: SyncPushPayload = {
			clientId: "det-client",
			deltas: [delta],
			lastSeenHlc: HLC.encode(1_700_000_000_000, 42),
		};

		const encoded1 = encodeSyncPush(push);
		const encoded2 = encodeSyncPush(push);
		expect(encoded1.ok).toBe(true);
		expect(encoded2.ok).toBe(true);
		if (!encoded1.ok || !encoded2.ok) return;

		expect(encoded1.value).toEqual(encoded2.value);
	});
});

// ---------------------------------------------------------------------------
// DeltaOp tests
// ---------------------------------------------------------------------------

describe("codec DeltaOp roundtrips", () => {
	it("INSERT op roundtrips correctly", () => {
		const delta = makeRowDelta({ op: "INSERT", deltaId: "op-insert" });
		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.op).toBe("INSERT");
	});

	it("UPDATE op roundtrips correctly", () => {
		const delta = makeRowDelta({ op: "UPDATE", deltaId: "op-update" });
		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.op).toBe("UPDATE");
	});

	it("DELETE op roundtrips correctly", () => {
		const delta = makeRowDelta({
			op: "DELETE",
			deltaId: "op-delete",
			columns: [],
		});
		const encoded = encodeRowDelta(delta);
		expect(encoded.ok).toBe(true);
		if (!encoded.ok) return;

		const decoded = decodeRowDelta(encoded.value);
		expect(decoded.ok).toBe(true);
		if (!decoded.ok) return;

		expect(decoded.value.op).toBe("DELETE");
	});
});
