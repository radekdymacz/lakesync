import { LakeSyncError } from "@lakesync/core";
import { describe, expect, it } from "vitest";
import { ControlPlaneError, wrapControlPlane } from "../errors";

describe("ControlPlaneError", () => {
	it("extends LakeSyncError", () => {
		const err = new ControlPlaneError("test", "NOT_FOUND");
		expect(err).toBeInstanceOf(LakeSyncError);
		expect(err).toBeInstanceOf(Error);
	});

	it("preserves code", () => {
		const err = new ControlPlaneError("not found", "NOT_FOUND");
		expect(err.code).toBe("NOT_FOUND");
		expect(err.message).toBe("not found");
	});

	it("preserves cause", () => {
		const cause = new Error("pg connection failed");
		const err = new ControlPlaneError("internal", "INTERNAL", cause);
		expect(err.cause).toBe(cause);
	});

	it("has correct name", () => {
		const err = new ControlPlaneError("test", "DUPLICATE");
		expect(err.name).toBe("ControlPlaneError");
	});
});

describe("wrapControlPlane", () => {
	it("returns Ok on success", async () => {
		const result = await wrapControlPlane(async () => 42, "should not appear");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value).toBe(42);
		}
	});

	it("wraps generic errors into ControlPlaneError", async () => {
		const result = await wrapControlPlane(async () => {
			throw new Error("pg error");
		}, "operation failed");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(ControlPlaneError);
			expect(result.error.message).toBe("operation failed");
			expect(result.error.code).toBe("INTERNAL");
			expect(result.error.cause).toBeInstanceOf(Error);
		}
	});

	it("passes through ControlPlaneError unchanged", async () => {
		const original = new ControlPlaneError("already exists", "DUPLICATE");
		const result = await wrapControlPlane(async () => {
			throw original;
		}, "should not appear");
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBe(original);
			expect(result.error.code).toBe("DUPLICATE");
		}
	});

	it("uses custom error code when specified", async () => {
		const result = await wrapControlPlane(
			async () => {
				throw new Error("bad input");
			},
			"validation failed",
			"INVALID_INPUT",
		);
		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error.code).toBe("INVALID_INPUT");
		}
	});
});
