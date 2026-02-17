import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { keysCreate, keysList, keysRevoke } from "../commands/keys";

// Mock config to avoid touching real ~/.lakesync
vi.mock("../config", () => ({
	loadConfig: () => ({
		gatewayUrl: "http://localhost:3000",
		token: "test-token",
		orgId: "org-1",
	}),
}));

describe("keys commands", () => {
	let mockStdout: string[];
	let mockStderr: string[];
	let mockFetch: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		mockStdout = [];
		mockStderr = [];

		vi.spyOn(process.stdout, "write").mockImplementation((data) => {
			mockStdout.push(String(data));
			return true;
		});
		vi.spyOn(process.stderr, "write").mockImplementation((data) => {
			mockStderr.push(String(data));
			return true;
		});

		mockFetch = vi.fn();
		vi.stubGlobal("fetch", mockFetch);
	});

	afterEach(() => {
		vi.restoreAllMocks();
		vi.unstubAllGlobals();
	});

	describe("keysList", () => {
		it("prints a table of API keys", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => [
					{
						id: "key-1",
						name: "CI Key",
						keyPrefix: "lk_abc",
						role: "admin",
						gatewayId: "gw-1",
						createdAt: "2024-01-01T00:00:00Z",
					},
					{
						id: "key-2",
						name: "Dev Key",
						keyPrefix: "lk_xyz",
						role: "client",
						createdAt: "2024-01-02T00:00:00Z",
					},
				],
			});

			await keysList({});

			const output = mockStdout.join("");
			expect(output).toContain("key-1");
			expect(output).toContain("CI Key");
			expect(output).toContain("lk_abc");
			expect(output).toContain("admin");
			expect(output).toContain("key-2");
			expect(output).toContain("Dev Key");
			expect(output).toContain("client");
		});

		it("prints message when no keys found", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => [],
			});

			await keysList({});

			const output = mockStdout.join("");
			expect(output).toContain("No API keys found");
		});

		it("calls correct endpoint with org ID", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => [],
			});

			await keysList({ org: "custom-org" });

			expect(mockFetch).toHaveBeenCalledWith(
				"http://localhost:3000/v1/orgs/custom-org/api-keys",
				expect.objectContaining({
					method: "GET",
					headers: expect.objectContaining({
						Authorization: "Bearer test-token",
					}),
				}),
			);
		});
	});

	describe("keysCreate", () => {
		it("creates a key and prints raw key", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => ({
					id: "key-new",
					name: "New Key",
					keyPrefix: "lk_new",
					role: "admin",
					rawKey: "lk_new_full_secret_key_value",
				}),
			});

			await keysCreate({ name: "New Key", role: "admin" });

			const output = mockStdout.join("");
			expect(output).toContain("key-new");
			expect(output).toContain("New Key");
			expect(output).toContain("lk_new");
			expect(output).toContain("admin");
			expect(output).toContain("lk_new_full_secret_key_value");
			expect(output).toContain("shown once");
		});

		it("sends correct POST body with gateway scope", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => ({
					id: "key-x",
					name: "test",
					keyPrefix: "lk_x",
					role: "client",
					rawKey: "lk_x_secret",
				}),
			});

			await keysCreate({ name: "test", org: "org-2", gateway: "gw-1" });

			expect(mockFetch).toHaveBeenCalledWith(
				"http://localhost:3000/v1/api-keys",
				expect.objectContaining({
					method: "POST",
					body: expect.stringContaining('"gatewayId":"gw-1"'),
				}),
			);
		});

		it("exits if --name is missing", async () => {
			const mockExit = vi.spyOn(process, "exit").mockImplementation(() => {
				throw new Error("process.exit");
			});

			await expect(keysCreate({})).rejects.toThrow("process.exit");

			const stderr = mockStderr.join("");
			expect(stderr).toContain("--name is required");
			mockExit.mockRestore();
		});

		it("exits if --role is invalid", async () => {
			const mockExit = vi.spyOn(process, "exit").mockImplementation(() => {
				throw new Error("process.exit");
			});

			await expect(keysCreate({ name: "test", role: "superuser" })).rejects.toThrow("process.exit");

			const stderr = mockStderr.join("");
			expect(stderr).toContain("--role must be");
			mockExit.mockRestore();
		});
	});

	describe("keysRevoke", () => {
		it("revokes a key by ID", async () => {
			mockFetch.mockResolvedValue({ ok: true, json: async () => ({}) });

			await keysRevoke({ id: "key-del" });

			expect(mockFetch).toHaveBeenCalledWith(
				"http://localhost:3000/v1/api-keys/key-del",
				expect.objectContaining({ method: "DELETE" }),
			);

			const output = mockStdout.join("");
			expect(output).toContain("Revoked API key: key-del");
		});

		it("exits if --id is missing", async () => {
			const mockExit = vi.spyOn(process, "exit").mockImplementation(() => {
				throw new Error("process.exit");
			});

			await expect(keysRevoke({})).rejects.toThrow("process.exit");

			const stderr = mockStderr.join("");
			expect(stderr).toContain("--id is required");
			mockExit.mockRestore();
		});
	});
});
