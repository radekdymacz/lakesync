import { expect, test } from "@playwright/test";

test.describe("Docs", () => {
	test("loads docs page", async ({ page }) => {
		await page.goto("/docs");
		await expect(page).toHaveTitle(/LakeSync/);
	});

	test("has documentation content", async ({ page }) => {
		await page.goto("/docs");
		// Fumadocs renders a sidebar with navigation
		await expect(page.locator("main")).toBeVisible();
	});
});
