import { resolve } from "node:path";
import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		globals: true,
		include: ["**/__tests__/**/*.test.ts", "**/__tests__/**/*.test.tsx"],
		passWithNoTests: true,
	},
	resolve: {
		alias: {
			"@": resolve(__dirname, "."),
		},
	},
});
