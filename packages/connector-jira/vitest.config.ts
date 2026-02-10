import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

/** Parse a .env file into a key-value record (ignores comments and blank lines). */
function loadDotenv(filePath: string): Record<string, string> {
	try {
		const content = readFileSync(filePath, "utf-8");
		const env: Record<string, string> = {};
		for (const line of content.split("\n")) {
			const trimmed = line.trim();
			if (!trimmed || trimmed.startsWith("#")) continue;
			const eqIndex = trimmed.indexOf("=");
			if (eqIndex === -1) continue;
			env[trimmed.slice(0, eqIndex)] = trimmed.slice(eqIndex + 1);
		}
		return env;
	} catch {
		return {};
	}
}

export default defineConfig({
	test: {
		globals: true,
		env: loadDotenv(resolve(__dirname, ".env")),
	},
});
