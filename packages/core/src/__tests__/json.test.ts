import { describe, expect, it } from "vitest";
import { bigintReplacer, bigintReviver } from "../json";

describe("bigintReplacer", () => {
	it("converts BigInt to string", () => {
		const result = JSON.stringify({ value: 42n }, bigintReplacer);
		expect(result).toBe('{"value":"42"}');
	});

	it("leaves numbers unchanged", () => {
		const result = JSON.stringify({ value: 42 }, bigintReplacer);
		expect(result).toBe('{"value":42}');
	});

	it("leaves strings unchanged", () => {
		const result = JSON.stringify({ value: "hello" }, bigintReplacer);
		expect(result).toBe('{"value":"hello"}');
	});

	it("handles nested objects with BigInt values", () => {
		const obj = { outer: { inner: 100n } };
		const result = JSON.parse(JSON.stringify(obj, bigintReplacer));
		expect(result.outer.inner).toBe("100");
	});

	it("handles arrays with BigInt values", () => {
		const result = JSON.parse(JSON.stringify([1n, 2n, 3n], bigintReplacer));
		expect(result).toEqual(["1", "2", "3"]);
	});

	it("handles null values gracefully", () => {
		const result = JSON.stringify({ a: null }, bigintReplacer);
		expect(result).toBe('{"a":null}');
	});

	it("handles zero BigInt", () => {
		const result = JSON.parse(JSON.stringify({ hlc: 0n }, bigintReplacer));
		expect(result.hlc).toBe("0");
	});

	it("handles negative BigInt", () => {
		const result = JSON.parse(JSON.stringify({ val: -99n }, bigintReplacer));
		expect(result.val).toBe("-99");
	});
});

describe("bigintReviver", () => {
	it("restores field ending in 'hlc' to BigInt", () => {
		const result = JSON.parse('{"hlc":"42"}', bigintReviver);
		expect(result.hlc).toBe(42n);
	});

	it("restores field ending in 'Hlc' (camelCase) to BigInt", () => {
		const result = JSON.parse('{"serverHlc":"100"}', bigintReviver);
		expect(result.serverHlc).toBe(100n);
	});

	it("restores 'serverHlc' field to BigInt", () => {
		const result = JSON.parse('{"serverHlc":"999"}', bigintReviver);
		expect(result.serverHlc).toBe(999n);
	});

	it("restores 'lastSyncedHlc' field to BigInt", () => {
		const result = JSON.parse('{"lastSyncedHlc":"12345"}', bigintReviver);
		expect(result.lastSyncedHlc).toBe(12345n);
	});

	it("leaves non-HLC string fields unchanged", () => {
		const result = JSON.parse('{"name":"alice"}', bigintReviver);
		expect(result.name).toBe("alice");
	});

	it("leaves number fields unchanged", () => {
		const result = JSON.parse('{"count":7}', bigintReviver);
		expect(result.count).toBe(7);
	});

	it('handles "0" as BigInt for HLC fields', () => {
		const result = JSON.parse('{"hlc":"0"}', bigintReviver);
		expect(result.hlc).toBe(0n);
	});

	it("handles nested objects with HLC fields", () => {
		const json = '{"data":{"lastSyncedHlc":"500"}}';
		const result = JSON.parse(json, bigintReviver);
		expect(result.data.lastSyncedHlc).toBe(500n);
	});
});

describe("bigintReplacer + bigintReviver round-trip", () => {
	it("round-trips an object with BigInt values", () => {
		const original = { hlc: 42n, serverHlc: 100n };
		const json = JSON.stringify(original, bigintReplacer);
		const restored = JSON.parse(json, bigintReviver);
		expect(restored.hlc).toBe(42n);
		expect(restored.serverHlc).toBe(100n);
	});

	it("round-trips multiple HLC fields", () => {
		const original = { hlc: 1n, serverHlc: 2n, lastSyncedHlc: 3n };
		const json = JSON.stringify(original, bigintReplacer);
		const restored = JSON.parse(json, bigintReviver);
		expect(restored.hlc).toBe(1n);
		expect(restored.serverHlc).toBe(2n);
		expect(restored.lastSyncedHlc).toBe(3n);
	});

	it("preserves mixed HLC and non-HLC fields", () => {
		const original = { hlc: 77n, name: "test", count: 5 };
		const json = JSON.stringify(original, bigintReplacer);
		const restored = JSON.parse(json, bigintReviver);
		expect(restored.hlc).toBe(77n);
		expect(restored.name).toBe("test");
		expect(restored.count).toBe(5);
	});
});
