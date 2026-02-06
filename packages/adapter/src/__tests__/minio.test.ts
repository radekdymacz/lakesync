import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { AdapterError } from "@lakesync/core";
import { MinIOAdapter } from "../minio";
import type { AdapterConfig } from "../types";

const config: AdapterConfig = {
	endpoint: "http://localhost:9000",
	bucket: "lakesync-dev",
	region: "us-east-1",
	credentials: {
		accessKeyId: "lakesync",
		secretAccessKey: "lakesync123",
	},
};

/** Check whether MinIO is reachable before running integration tests */
const minioAvailable = await (async () => {
	try {
		const r = await fetch("http://localhost:9000/minio/health/live", {
			signal: AbortSignal.timeout(2000),
		});
		return r.ok;
	} catch {
		return false;
	}
})();

describe.skipIf(!minioAvailable)("MinIOAdapter (integration)", () => {
	let adapter: MinIOAdapter;
	const prefix = `test-${Date.now()}/`;

	beforeAll(() => {
		adapter = new MinIOAdapter(config);
	});

	afterAll(async () => {
		// Clean up all test objects created during the run
		const listResult = await adapter.listObjects(prefix);
		if (listResult.ok && listResult.value.length > 0) {
			await adapter.deleteObjects(listResult.value.map((o) => o.key));
		}
	});

	it("putObject + getObject roundtrip preserves bytes", async () => {
		const key = `${prefix}roundtrip.bin`;
		const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);

		const putResult = await adapter.putObject(key, data, "application/octet-stream");
		expect(putResult.ok).toBe(true);

		const getResult = await adapter.getObject(key);
		expect(getResult.ok).toBe(true);
		if (getResult.ok) {
			expect(getResult.value).toEqual(data);
		}
	});

	it("putObject overwrites existing key with new data", async () => {
		const key = `${prefix}overwrite.bin`;
		const first = new Uint8Array([10, 20]);
		const second = new Uint8Array([30, 40, 50]);

		await adapter.putObject(key, first);
		const overwriteResult = await adapter.putObject(key, second);
		expect(overwriteResult.ok).toBe(true);

		const getResult = await adapter.getObject(key);
		expect(getResult.ok).toBe(true);
		if (getResult.ok) {
			expect(getResult.value).toEqual(second);
		}
	});

	it("getObject returns Err for missing key", async () => {
		const key = `${prefix}does-not-exist-${Date.now()}.bin`;
		const result = await adapter.getObject(key);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(AdapterError);
		}
	});

	it("headObject returns correct size and a valid date", async () => {
		const key = `${prefix}head-test.bin`;
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await adapter.putObject(key, data);
		const headResult = await adapter.headObject(key);
		expect(headResult.ok).toBe(true);
		if (headResult.ok) {
			expect(headResult.value.size).toBe(5);
			expect(headResult.value.lastModified).toBeInstanceOf(Date);
			expect(headResult.value.lastModified.getTime()).toBeGreaterThan(0);
		}
	});

	it("listObjects filters by prefix correctly", async () => {
		const subPrefix = `${prefix}list-test/`;
		const otherPrefix = `${prefix}other/`;

		await Promise.all([
			adapter.putObject(`${subPrefix}a.bin`, new Uint8Array([1])),
			adapter.putObject(`${subPrefix}b.bin`, new Uint8Array([2])),
			adapter.putObject(`${otherPrefix}c.bin`, new Uint8Array([3])),
		]);

		const listResult = await adapter.listObjects(subPrefix);
		expect(listResult.ok).toBe(true);
		if (listResult.ok) {
			const keys = listResult.value.map((o) => o.key);
			expect(keys).toHaveLength(2);
			expect(keys).toContain(`${subPrefix}a.bin`);
			expect(keys).toContain(`${subPrefix}b.bin`);
			expect(keys).not.toContain(`${otherPrefix}c.bin`);
		}
	});

	it("deleteObject makes subsequent getObject return Err", async () => {
		const key = `${prefix}delete-single.bin`;
		await adapter.putObject(key, new Uint8Array([99]));

		const deleteResult = await adapter.deleteObject(key);
		expect(deleteResult.ok).toBe(true);

		const getResult = await adapter.getObject(key);
		expect(getResult.ok).toBe(false);
		if (!getResult.ok) {
			expect(getResult.error).toBeInstanceOf(AdapterError);
		}
	});

	it("deleteObjects removes multiple objects in a batch", async () => {
		const keys = [
			`${prefix}batch-del-1.bin`,
			`${prefix}batch-del-2.bin`,
			`${prefix}batch-del-3.bin`,
		];

		await Promise.all(keys.map((k) => adapter.putObject(k, new Uint8Array([1]))));

		const deleteResult = await adapter.deleteObjects(keys);
		expect(deleteResult.ok).toBe(true);

		// Verify all are gone
		const results = await Promise.all(keys.map((k) => adapter.getObject(k)));
		for (const result of results) {
			expect(result.ok).toBe(false);
		}
	});

	it("returns AdapterError when connection fails", async () => {
		const badAdapter = new MinIOAdapter({
			...config,
			endpoint: "http://localhost:19999", // unreachable port
		});

		const result = await badAdapter.putObject(
			"unreachable.bin",
			new Uint8Array([1]),
		);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(AdapterError);
			expect(result.error.code).toBe("ADAPTER_ERROR");
		}
	});
});
