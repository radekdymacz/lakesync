import path from "node:path";
import { defineConfig } from "vitest/config";

const packages = path.resolve(__dirname, "packages");

export default defineConfig({
	resolve: {
		alias: {
			"@lakesync/core": path.join(packages, "core/src/index.ts"),
			"@lakesync/client": path.join(packages, "client/src/index.ts"),
			"@lakesync/gateway": path.join(packages, "gateway/src/index.ts"),
			"@lakesync/adapter": path.join(packages, "adapter/src/index.ts"),
			"@lakesync/proto": path.join(packages, "proto/src/index.ts"),
		},
	},
	test: {
		globals: true,
		include: ["tests/integration/**/*.test.ts"],
		testTimeout: 30_000,
		projects: [
			{
				resolve: {
					alias: {
						"@lakesync/core": path.join(packages, "core/src/index.ts"),
						"@lakesync/client": path.join(packages, "client/src/index.ts"),
						"@lakesync/gateway": path.join(packages, "gateway/src/index.ts"),
						"@lakesync/adapter": path.join(packages, "adapter/src/index.ts"),
						"@lakesync/proto": path.join(packages, "proto/src/index.ts"),
					},
				},
				test: {
					name: "integration",
					globals: true,
					include: ["tests/integration/**/*.test.ts"],
					testTimeout: 30_000,
				},
			},
		],
	},
});
