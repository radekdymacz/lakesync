import { verifyToken } from "@lakesync/core";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

describe("token create command", () => {
	const originalEnv = process.env;
	let mockStdout: string[];
	let mockStderr: string[];

	beforeEach(() => {
		mockStdout = [];
		mockStderr = [];
		process.env = { ...originalEnv };

		vi.spyOn(process.stdout, "write").mockImplementation((data) => {
			mockStdout.push(String(data));
			return true;
		});
		vi.spyOn(process.stderr, "write").mockImplementation((data) => {
			mockStderr.push(String(data));
			return true;
		});
	});

	afterEach(() => {
		process.env = originalEnv;
		vi.restoreAllMocks();
	});

	it("generates a valid JWT that can be verified", async () => {
		const { tokenCreate } = await import("../commands/token");

		await tokenCreate({
			secret: "test-secret-32-bytes-long-at-min",
			gateway: "test-gw",
			client: "test-client",
			role: "admin",
			ttl: "300",
		});

		expect(mockStdout.length).toBe(1);
		const token = mockStdout[0]!.trim();

		// Verify the token we generated
		const result = await verifyToken(token, "test-secret-32-bytes-long-at-min");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("test-client");
			expect(result.value.gatewayId).toBe("test-gw");
			expect(result.value.role).toBe("admin");
		}
	});

	it("uses default role and client ID", async () => {
		const { tokenCreate } = await import("../commands/token");

		await tokenCreate({
			secret: "test-secret-for-defaults",
			gateway: "default-gw",
		});

		const token = mockStdout[0]!.trim();
		const result = await verifyToken(token, "test-secret-for-defaults");
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.clientId).toBe("cli-user");
			expect(result.value.role).toBe("client");
		}
	});

	it("reads secret from environment variable", async () => {
		const { tokenCreate } = await import("../commands/token");
		process.env.LAKESYNC_JWT_SECRET = "env-secret";

		await tokenCreate({ gateway: "env-gw" });

		const token = mockStdout[0]!.trim();
		const result = await verifyToken(token, "env-secret");
		expect(result.ok).toBe(true);
	});
});
