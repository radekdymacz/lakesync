import { expect, test } from "@playwright/test";

test.describe("Landing page", () => {
	test("loads and shows hero section", async ({ page }) => {
		await page.goto("/");
		await expect(page).toHaveTitle(/LakeSync/);
		await expect(page.getByText("Declare what data goes where")).toBeVisible();
	});

	test("has navigation links", async ({ page }) => {
		await page.goto("/");
		// Navigation bar at the top of the landing page
		const nav = page.locator("nav").first();
		await expect(nav.getByRole("link", { name: /dashboard/i })).toBeVisible();
		await expect(nav.getByRole("link", { name: /docs/i })).toBeVisible();
	});

	test("has call-to-action buttons", async ({ page }) => {
		await page.goto("/");
		await expect(page.getByRole("link", { name: /get started/i }).first()).toBeVisible();
		await expect(page.getByRole("link", { name: /view docs/i }).first()).toBeVisible();
	});

	test("primary CTA links to getting started docs", async ({ page }) => {
		await page.goto("/");
		const cta = page.getByRole("link", { name: /get started/i }).first();
		await expect(cta).toHaveAttribute("href", "/docs/getting-started");
	});

	test("states the offline-first value prop", async ({ page }) => {
		await page.goto("/");
		await expect(page.getByText(/offline-first sync/i).first()).toBeVisible();
		await expect(page.getByText(/SQLite on the device/i).first()).toBeVisible();
	});

	test("shows features section", async ({ page }) => {
		await page.goto("/");
		await expect(page.getByRole("heading", { name: /SQL Data/i })).toBeVisible();
		await expect(page.getByRole("heading", { name: /SaaS Data/i })).toBeVisible();
	});

	test("shows adapters section", async ({ page }) => {
		await page.goto("/");
		await expect(page.getByText(/Postgres/i).first()).toBeVisible();
	});

	test("footer has links", async ({ page }) => {
		await page.goto("/");
		const footer = page.locator("footer");
		await expect(footer.getByRole("link", { name: /docs/i })).toBeVisible();
		await expect(footer.getByRole("link", { name: /github/i })).toBeVisible();
	});
});
