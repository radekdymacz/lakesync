import path from "node:path";
import { defineConfig } from "vitest/config";

const packages = path.resolve(__dirname, "../../packages");

export default defineConfig({
	resolve: {
		alias: {
			"@lakesync/core": path.join(packages, "core/src/index.ts"),
			"@lakesync/client": path.join(packages, "client/src/index.ts"),
			"@lakesync/gateway": path.join(packages, "gateway/src/index.ts"),
			"@lakesync/adapter": path.join(packages, "adapter/src/index.ts"),
			"@lakesync/proto": path.join(packages, "proto/src/index.ts"),
			"@lakesync/parquet": path.join(packages, "parquet/src/index.ts"),
			"@lakesync/catalogue": path.join(packages, "catalogue/src/index.ts"),
			"@lakesync/compactor": path.join(packages, "compactor/src/index.ts"),
		},
	},
	test: {
		globals: true,
		include: ["tests/hardening/**/*.test.ts"],
		testTimeout: 120_000,
		projects: [
			{
				resolve: {
					alias: {
						"@lakesync/core": path.join(packages, "core/src/index.ts"),
						"@lakesync/client": path.join(packages, "client/src/index.ts"),
						"@lakesync/gateway": path.join(packages, "gateway/src/index.ts"),
						"@lakesync/adapter": path.join(packages, "adapter/src/index.ts"),
						"@lakesync/proto": path.join(packages, "proto/src/index.ts"),
						"@lakesync/parquet": path.join(packages, "parquet/src/index.ts"),
						"@lakesync/catalogue": path.join(packages, "catalogue/src/index.ts"),
						"@lakesync/compactor": path.join(packages, "compactor/src/index.ts"),
					},
				},
				test: {
					name: "hardening",
					globals: true,
					include: ["tests/hardening/**/*.test.ts"],
					testTimeout: 120_000,
				},
			},
		],
	},
});
