import { existsSync, mkdirSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Create test dir from the real tmpdir BEFORE mocking
const testDir = join(tmpdir(), `lakesync-cli-test-${Date.now()}`);

vi.mock("node:os", async (importOriginal) => {
	const actual = await importOriginal<typeof import("node:os")>();
	return {
		...actual,
		homedir: () => testDir,
	};
});

// Import after mocking
const { loadConfig, saveConfig } = await import("../config");

describe("config", () => {
	beforeEach(() => {
		mkdirSync(testDir, { recursive: true });
	});

	afterEach(() => {
		rmSync(testDir, { recursive: true, force: true });
	});

	it("returns empty config when no file exists", () => {
		const config = loadConfig();
		expect(config).toEqual({});
	});

	it("saves and loads config", () => {
		saveConfig({
			gatewayUrl: "http://localhost:3000",
			gatewayId: "my-gw",
			token: "test-token",
		});

		const config = loadConfig();
		expect(config.gatewayUrl).toBe("http://localhost:3000");
		expect(config.gatewayId).toBe("my-gw");
		expect(config.token).toBe("test-token");
	});

	it("creates .lakesync directory if it does not exist", () => {
		const configDir = join(testDir, ".lakesync");
		expect(existsSync(configDir)).toBe(false);

		saveConfig({ gatewayId: "test" });

		expect(existsSync(configDir)).toBe(true);
		const raw = readFileSync(join(configDir, "config.json"), "utf-8");
		const parsed = JSON.parse(raw);
		expect(parsed.gatewayId).toBe("test");
	});

	it("overwrites existing config", () => {
		saveConfig({ gatewayId: "first" });
		saveConfig({ gatewayId: "second" });

		const config = loadConfig();
		expect(config.gatewayId).toBe("second");
	});
});
