import { expect, test } from "@playwright/test";

test.describe("Sidebar navigation", () => {
	test("navigates from dashboard to gateways", async ({ page }) => {
		await page.goto("/dashboard");
		await page.getByRole("link", { name: /gateways/i }).click();
		await expect(page).toHaveURL(/\/gateways/);
		await expect(page.getByRole("heading", { name: "Gateways" })).toBeVisible();
	});

	test("navigates from dashboard to API keys", async ({ page }) => {
		await page.goto("/dashboard");
		await page.getByRole("link", { name: /api keys/i }).click();
		await expect(page).toHaveURL(/\/api-keys/);
		await expect(page.getByRole("heading", { name: "API Keys" })).toBeVisible();
	});

	test("navigates from dashboard to usage", async ({ page }) => {
		await page.goto("/dashboard");
		await page.getByRole("link", { name: /usage/i }).click();
		await expect(page).toHaveURL(/\/usage/);
		await expect(page.getByRole("heading", { name: "Usage" })).toBeVisible();
	});

	test("navigates back to dashboard via overview link", async ({ page }) => {
		await page.goto("/gateways");
		await page.getByRole("link", { name: /overview/i }).click();
		await expect(page).toHaveURL(/\/dashboard/);
	});
});
