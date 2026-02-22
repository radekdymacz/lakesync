import { expect, test } from "@playwright/test";

test.describe("Gateways", () => {
	test("loads gateways list page", async ({ page }) => {
		await page.goto("/gateways");
		await expect(page.getByRole("heading", { name: "Gateways" })).toBeVisible();
	});

	test("shows create gateway button", async ({ page }) => {
		await page.goto("/gateways");
		await expect(page.getByRole("button", { name: /create gateway/i })).toBeVisible();
	});

	test("opens create gateway modal", async ({ page }) => {
		await page.goto("/gateways", { waitUntil: "networkidle" });
		const btn = page.getByRole("button", { name: /create gateway/i });
		await expect(btn).toBeVisible();
		await btn.click();
		await expect(page.getByText("Set up a new sync gateway")).toBeVisible();
	});
});
