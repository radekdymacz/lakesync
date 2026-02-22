import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
	testDir: "./e2e",
	fullyParallel: true,
	forbidOnly: !!process.env.CI,
	retries: process.env.CI ? 2 : 0,
	workers: process.env.CI ? 1 : undefined,
	reporter: [["list"], ["html", { open: "never" }]],
	timeout: 30_000,

	use: {
		baseURL: "http://localhost:3002",
		trace: "on-first-retry",
		screenshot: "only-on-failure",
	},

	projects: [
		{
			name: "chromium",
			use: { ...devices["Desktop Chrome"] },
		},
	],

	webServer: {
		command: "npx next dev --port 3002",
		port: 3002,
		timeout: 120_000,
		reuseExistingServer: !process.env.CI,
		env: {
			NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY: "",
			CLERK_SECRET_KEY: "",
		},
	},
});
