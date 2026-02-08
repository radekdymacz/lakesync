import { describe, expect, it } from "vitest";
import { isDatabaseAdapter } from "../db-types";

describe("isDatabaseAdapter", () => {
	const fakeDatabaseAdapter = {
		insertDeltas: async () => ({ ok: true as const, value: undefined }),
		queryDeltasSince: async () => ({ ok: true as const, value: [] }),
		getLatestState: async () => ({ ok: true as const, value: null }),
		ensureSchema: async () => ({ ok: true as const, value: undefined }),
		close: async () => {},
	};

	it("returns true for an object with all DatabaseAdapter methods", () => {
		expect(isDatabaseAdapter(fakeDatabaseAdapter)).toBe(true);
	});

	it("returns false for a LakeAdapter-like object", () => {
		const lakeAdapter = {
			putObject: async () => ({ ok: true as const, value: undefined }),
			getObject: async () => ({ ok: true as const, value: new Uint8Array() }),
			headObject: async () => ({ ok: true as const, value: { size: 0, lastModified: new Date() } }),
			listObjects: async () => ({ ok: true as const, value: [] }),
			deleteObject: async () => ({ ok: true as const, value: undefined }),
			deleteObjects: async () => ({ ok: true as const, value: undefined }),
		};
		expect(isDatabaseAdapter(lakeAdapter)).toBe(false);
	});

	it("returns false for null, undefined, and primitives", () => {
		expect(isDatabaseAdapter(null)).toBe(false);
		expect(isDatabaseAdapter(undefined)).toBe(false);
		expect(isDatabaseAdapter(42)).toBe(false);
		expect(isDatabaseAdapter("string")).toBe(false);
		expect(isDatabaseAdapter(true)).toBe(false);
	});

	it("returns false for a partial object missing queryDeltasSince", () => {
		const partial = {
			insertDeltas: async () => ({ ok: true as const, value: undefined }),
		};
		expect(isDatabaseAdapter(partial)).toBe(false);
	});

	it("returns false when insertDeltas is not a function", () => {
		const notFunc = {
			insertDeltas: "not-a-function",
			queryDeltasSince: async () => ({ ok: true as const, value: [] }),
		};
		expect(isDatabaseAdapter(notFunc)).toBe(false);
	});
});
