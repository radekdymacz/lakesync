import { describe, expect, it } from "vitest";
import {
	AdapterError,
	ClockDriftError,
	ConflictError,
	Err,
	FlushError,
	flatMapResult,
	fromPromise,
	LakeSyncError,
	mapResult,
	Ok,
	SchemaError,
	unwrapOrThrow,
} from "../../result";

describe("Result", () => {
	it("Ok/Err have correct discriminants", () => {
		const ok = Ok(42);
		const err = Err(new LakeSyncError("fail", "TEST"));

		expect(ok.ok).toBe(true);
		if (ok.ok) expect(ok.value).toBe(42);
		expect(err.ok).toBe(false);
		if (!err.ok) expect(err.error).toBeInstanceOf(LakeSyncError);
	});

	it("mapResult transforms Ok, passes through Err", () => {
		const ok = Ok(10);
		const err = Err(new LakeSyncError("fail", "TEST"));

		const mapped = mapResult(ok, (v) => v * 2);
		expect(mapped.ok).toBe(true);
		if (mapped.ok) expect(mapped.value).toBe(20);

		const mappedErr = mapResult(err, (v: number) => v * 2);
		expect(mappedErr.ok).toBe(false);
	});

	it("flatMapResult chains correctly", () => {
		const ok = Ok(10);
		const chained = flatMapResult(ok, (v) => Ok(v.toString()));
		expect(chained.ok).toBe(true);
		if (chained.ok) expect(chained.value).toBe("10");

		const err = Err(new LakeSyncError("fail", "TEST"));
		const chainedErr = flatMapResult(err, (v: number) => Ok(v.toString()));
		expect(chainedErr.ok).toBe(false);
	});

	it("unwrapOrThrow returns value on Ok, throws on Err", () => {
		expect(unwrapOrThrow(Ok(42))).toBe(42);
		expect(() => unwrapOrThrow(Err(new Error("boom")))).toThrow("boom");
	});

	it("fromPromise wraps resolve to Ok, reject to Err", async () => {
		const ok = await fromPromise(Promise.resolve(42));
		expect(ok.ok).toBe(true);
		if (ok.ok) expect(ok.value).toBe(42);

		const err = await fromPromise(Promise.reject(new Error("fail")));
		expect(err.ok).toBe(false);
		if (!err.ok) expect(err.error.message).toBe("fail");
	});

	it("all errors are instanceof LakeSyncError", () => {
		expect(new ClockDriftError("drift")).toBeInstanceOf(LakeSyncError);
		expect(new ConflictError("conflict")).toBeInstanceOf(LakeSyncError);
		expect(new FlushError("flush")).toBeInstanceOf(LakeSyncError);
		expect(new SchemaError("schema")).toBeInstanceOf(LakeSyncError);
		expect(new AdapterError("adapter")).toBeInstanceOf(LakeSyncError);
	});

	it("error codes are correct strings", () => {
		expect(new ClockDriftError("").code).toBe("CLOCK_DRIFT");
		expect(new ConflictError("").code).toBe("CONFLICT");
		expect(new FlushError("").code).toBe("FLUSH_FAILED");
		expect(new SchemaError("").code).toBe("SCHEMA_MISMATCH");
		expect(new AdapterError("").code).toBe("ADAPTER_ERROR");
	});
});
