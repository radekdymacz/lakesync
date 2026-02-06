import "fake-indexeddb/auto";
import { describe, expect, it } from "vitest";
import { deleteSnapshot, loadSnapshot, saveSnapshot } from "../idb-persistence";

describe("idb-persistence", () => {
	let testCounter = 0;

	/** Generate a unique DB name per test to avoid shared state */
	function uniqueName(prefix = "test-db"): string {
		return `${prefix}-${Date.now()}-${++testCounter}`;
	}

	it("load from empty DB returns null", async () => {
		const name = uniqueName();
		const result = await loadSnapshot(name);
		expect(result).toBeNull();
	});

	it("save then load returns the same Uint8Array", async () => {
		const name = uniqueName();
		const data = new Uint8Array([1, 2, 3, 4, 5]);

		await saveSnapshot(name, data);
		const loaded = await loadSnapshot(name);

		expect(loaded).toBeInstanceOf(Uint8Array);
		expect(loaded).toEqual(data);
	});

	it("save, delete, then load returns null", async () => {
		const name = uniqueName();
		const data = new Uint8Array([10, 20, 30]);

		await saveSnapshot(name, data);
		await deleteSnapshot(name);
		const loaded = await loadSnapshot(name);

		expect(loaded).toBeNull();
	});

	it("multiple databases are isolated", async () => {
		const nameA = uniqueName("db-a");
		const nameB = uniqueName("db-b");
		const data = new Uint8Array([42, 43, 44]);

		await saveSnapshot(nameA, data);
		const loadedB = await loadSnapshot(nameB);

		expect(loadedB).toBeNull();

		// Verify the original is still retrievable
		const loadedA = await loadSnapshot(nameA);
		expect(loadedA).toEqual(data);
	});

	it("overwrite: save twice with different data, load returns latest", async () => {
		const name = uniqueName();
		const first = new Uint8Array([1, 1, 1]);
		const second = new Uint8Array([2, 2, 2, 2]);

		await saveSnapshot(name, first);
		await saveSnapshot(name, second);
		const loaded = await loadSnapshot(name);

		expect(loaded).toEqual(second);
		expect(loaded).not.toEqual(first);
	});
});
