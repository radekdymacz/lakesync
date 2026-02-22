import { expect, test } from "@playwright/test";

test.describe("Dashboard", () => {
	test("loads dashboard page", async ({ page }) => {
		await page.goto("/dashboard");
		await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
	});

	test("shows stats cards", async ({ page }) => {
		await page.goto("/dashboard");
		await expect(page.getByText(/Active Gateways/i)).toBeVisible();
		await expect(page.getByText(/Deltas This Month/i)).toBeVisible();
		await expect(page.getByText(/Storage Used/i)).toBeVisible();
	});

	test("has sidebar navigation", async ({ page }) => {
		await page.goto("/dashboard");
		await expect(page.getByRole("link", { name: /overview/i })).toBeVisible();
		await expect(page.getByRole("link", { name: /gateways/i })).toBeVisible();
		await expect(page.getByRole("link", { name: /api keys/i })).toBeVisible();
		await expect(page.getByRole("link", { name: /usage/i })).toBeVisible();
	});
});
