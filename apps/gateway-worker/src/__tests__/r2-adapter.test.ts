import { describe, expect, it } from "vitest";
import { R2Adapter } from "../r2-adapter";

/**
 * Minimal mock R2ObjectBody that satisfies the subset of the interface
 * used by R2Adapter.getObject (arrayBuffer).
 */
function createMockR2ObjectBody(data: Uint8Array): {
	arrayBuffer: () => Promise<ArrayBuffer>;
} {
	return {
		arrayBuffer: async () =>
			data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength) as ArrayBuffer,
	};
}

/**
 * Minimal mock R2Object returned by head(), with size and uploaded fields.
 */
function createMockR2Head(size: number, uploaded: Date): { size: number; uploaded: Date } {
	return { size, uploaded };
}

/**
 * Create a minimal mock R2Bucket for testing R2Adapter methods.
 *
 * Stores objects in an in-memory Map and exposes put/get/head/list/delete
 * matching the shape R2Adapter calls.
 */
function createMockR2Bucket(): R2Bucket {
	const store = new Map<string, Uint8Array>();
	const timestamps = new Map<string, Date>();

	return {
		put: async (key: string, value: unknown) => {
			const data = value instanceof Uint8Array ? value : new Uint8Array(value as ArrayBuffer);
			store.set(key, data);
			timestamps.set(key, new Date());
			return null as unknown as R2Object;
		},

		get: async (key: string) => {
			const data = store.get(key);
			if (!data) return null;
			return createMockR2ObjectBody(data) as unknown as R2ObjectBody;
		},

		head: async (key: string) => {
			const data = store.get(key);
			if (!data) return null;
			return createMockR2Head(
				data.length,
				timestamps.get(key) ?? new Date(),
			) as unknown as R2Object;
		},

		list: async (options?: R2ListOptions) => {
			const prefix = options?.prefix ?? "";
			const objects = [...store.entries()]
				.filter(([k]) => k.startsWith(prefix))
				.map(([key, data]) => ({
					key,
					size: data.length,
					uploaded: timestamps.get(key) ?? new Date(),
				}));
			return {
				objects,
				truncated: false,
				delimitedPrefixes: [],
			} as unknown as R2Objects;
		},

		delete: async (keys: string | string[]) => {
			const keyList = Array.isArray(keys) ? keys : [keys];
			for (const k of keyList) {
				store.delete(k);
				timestamps.delete(k);
			}
		},

		createMultipartUpload: (() => {
			throw new Error("Not implemented in mock");
		}) as unknown as R2Bucket["createMultipartUpload"],
		resumeMultipartUpload: (() => {
			throw new Error("Not implemented in mock");
		}) as unknown as R2Bucket["resumeMultipartUpload"],
	};
}

/**
 * Create a mock R2Bucket that throws on every operation,
 * used to verify error wrapping.
 */
function createThrowingR2Bucket(): R2Bucket {
	const thrower = () => {
		throw new Error("R2 service unavailable");
	};
	return {
		put: thrower,
		get: thrower,
		head: thrower,
		list: thrower,
		delete: thrower,
		createMultipartUpload: thrower,
		resumeMultipartUpload: thrower,
	} as unknown as R2Bucket;
}

describe("R2Adapter", () => {
	// ── putObject ─────────────────────────────────────────────────────

	it("putObject stores data and returns Ok", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);
		const data = new TextEncoder().encode("hello world");

		const result = await adapter.putObject("test/file.txt", data, "text/plain");
		expect(result.ok).toBe(true);
	});

	// ── getObject ─────────────────────────────────────────────────────

	it("getObject returns Ok with data for existing object", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);
		const original = new TextEncoder().encode("test payload");

		await adapter.putObject("data/obj.bin", original);
		const result = await adapter.getObject("data/obj.bin");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(new TextDecoder().decode(result.value)).toBe("test payload");
		}
	});

	it("getObject returns Err(AdapterError) when object does not exist", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.getObject("missing/object.bin");

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("not found");
		}
	});

	// ── headObject ────────────────────────────────────────────────────

	it("headObject returns size and lastModified for existing object", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await adapter.putObject("meta/test.bin", data);
		const result = await adapter.headObject("meta/test.bin");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.size).toBe(5);
			expect(result.value.lastModified).toBeInstanceOf(Date);
		}
	});

	it("headObject returns Err(AdapterError) when object does not exist", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.headObject("missing/file.bin");

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("not found");
		}
	});

	// ── listObjects ───────────────────────────────────────────────────

	it("listObjects returns matching ObjectInfo entries", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);

		await adapter.putObject("data/a.bin", new Uint8Array([1]));
		await adapter.putObject("data/b.bin", new Uint8Array([2, 3]));
		await adapter.putObject("other/c.bin", new Uint8Array([4, 5, 6]));

		const result = await adapter.listObjects("data/");

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toHaveLength(2);
			const keys = result.value.map((o) => o.key);
			expect(keys).toContain("data/a.bin");
			expect(keys).toContain("data/b.bin");
		}
	});

	// ── deleteObject ──────────────────────────────────────────────────

	it("deleteObject removes the object and returns Ok", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);

		await adapter.putObject("del/file.bin", new Uint8Array([1]));
		const delResult = await adapter.deleteObject("del/file.bin");
		expect(delResult.ok).toBe(true);

		// Confirm the object is gone
		const getResult = await adapter.getObject("del/file.bin");
		expect(getResult.ok).toBe(false);
	});

	// ── deleteObjects ─────────────────────────────────────────────────

	it("deleteObjects with empty array returns Ok immediately", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.deleteObjects([]);
		expect(result.ok).toBe(true);
	});

	it("deleteObjects removes multiple objects", async () => {
		const bucket = createMockR2Bucket();
		const adapter = new R2Adapter(bucket);

		await adapter.putObject("batch/a.bin", new Uint8Array([1]));
		await adapter.putObject("batch/b.bin", new Uint8Array([2]));

		const result = await adapter.deleteObjects(["batch/a.bin", "batch/b.bin"]);
		expect(result.ok).toBe(true);

		// Confirm both are gone
		const getA = await adapter.getObject("batch/a.bin");
		const getB = await adapter.getObject("batch/b.bin");
		expect(getA.ok).toBe(false);
		expect(getB.ok).toBe(false);
	});

	// ── Error wrapping ────────────────────────────────────────────────

	it("wraps R2 errors into Err(AdapterError) on get", async () => {
		const bucket = createThrowingR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.getObject("any/key");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to get object");
		}
	});

	it("wraps R2 errors into Err(AdapterError) on put", async () => {
		const bucket = createThrowingR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.putObject("any/key", new Uint8Array([1]));
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to put object");
		}
	});

	it("wraps R2 errors into Err(AdapterError) on head", async () => {
		const bucket = createThrowingR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.headObject("any/key");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to head object");
		}
	});

	it("wraps R2 errors into Err(AdapterError) on list", async () => {
		const bucket = createThrowingR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.listObjects("prefix/");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to list objects");
		}
	});

	it("wraps R2 errors into Err(AdapterError) on delete", async () => {
		const bucket = createThrowingR2Bucket();
		const adapter = new R2Adapter(bucket);

		const result = await adapter.deleteObject("any/key");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.message).toContain("Failed to delete object");
		}
	});
});
