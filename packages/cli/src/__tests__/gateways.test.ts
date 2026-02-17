import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { gatewaysCreate, gatewaysDelete, gatewaysList } from "../commands/gateways";

// Mock config to avoid touching real ~/.lakesync
vi.mock("../config", () => ({
	loadConfig: () => ({
		gatewayUrl: "http://localhost:3000",
		token: "test-token",
		orgId: "org-1",
	}),
}));

describe("gateways commands", () => {
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

	describe("gatewaysList", () => {
		it("prints a table of gateways", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => [
					{ id: "gw-1", name: "Production", status: "active", region: "us-east-1" },
					{ id: "gw-2", name: "Staging", status: "active" },
				],
			});

			await gatewaysList({});

			const output = mockStdout.join("");
			expect(output).toContain("gw-1");
			expect(output).toContain("Production");
			expect(output).toContain("active");
			expect(output).toContain("us-east-1");
			expect(output).toContain("gw-2");
			expect(output).toContain("Staging");
		});

		it("prints message when no gateways found", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => [],
			});

			await gatewaysList({});

			const output = mockStdout.join("");
			expect(output).toContain("No gateways found");
		});

		it("calls correct endpoint with org ID", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => [],
			});

			await gatewaysList({ org: "custom-org" });

			expect(mockFetch).toHaveBeenCalledWith(
				"http://localhost:3000/v1/orgs/custom-org/gateways",
				expect.objectContaining({
					method: "GET",
					headers: expect.objectContaining({
						Authorization: "Bearer test-token",
					}),
				}),
			);
		});
	});

	describe("gatewaysCreate", () => {
		it("creates a gateway and prints details", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => ({
					id: "gw-new",
					name: "My Gateway",
					status: "active",
					region: "eu-west-1",
				}),
			});

			await gatewaysCreate({ name: "My Gateway", region: "eu-west-1" });

			const output = mockStdout.join("");
			expect(output).toContain("gw-new");
			expect(output).toContain("My Gateway");
			expect(output).toContain("active");
			expect(output).toContain("eu-west-1");
		});

		it("sends correct POST body", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				json: async () => ({ id: "gw-x", name: "test", status: "active" }),
			});

			await gatewaysCreate({ name: "test", org: "org-2" });

			expect(mockFetch).toHaveBeenCalledWith(
				"http://localhost:3000/v1/gateways",
				expect.objectContaining({
					method: "POST",
					body: expect.stringContaining('"orgId":"org-2"'),
				}),
			);
		});

		it("exits if --name is missing", async () => {
			const mockExit = vi.spyOn(process, "exit").mockImplementation(() => {
				throw new Error("process.exit");
			});

			await expect(gatewaysCreate({})).rejects.toThrow("process.exit");

			const stderr = mockStderr.join("");
			expect(stderr).toContain("--name is required");
			mockExit.mockRestore();
		});
	});

	describe("gatewaysDelete", () => {
		it("deletes a gateway by ID", async () => {
			mockFetch.mockResolvedValue({ ok: true, json: async () => ({}) });

			await gatewaysDelete({ id: "gw-del" });

			expect(mockFetch).toHaveBeenCalledWith(
				"http://localhost:3000/v1/gateways/gw-del",
				expect.objectContaining({ method: "DELETE" }),
			);

			const output = mockStdout.join("");
			expect(output).toContain("Deleted gateway: gw-del");
		});

		it("exits if --id is missing", async () => {
			const mockExit = vi.spyOn(process, "exit").mockImplementation(() => {
				throw new Error("process.exit");
			});

			await expect(gatewaysDelete({})).rejects.toThrow("process.exit");

			const stderr = mockStderr.join("");
			expect(stderr).toContain("--id is required");
			mockExit.mockRestore();
		});
	});
});
