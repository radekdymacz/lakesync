"use client";

import Link from "next/link";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";

export function LandingPage() {
	const { resolvedTheme, setTheme } = useTheme();
	const [scrolled, setScrolled] = useState(false);

	useEffect(() => {
		const onScroll = () => setScrolled(window.scrollY > 10);
		window.addEventListener("scroll", onScroll, { passive: true });
		return () => window.removeEventListener("scroll", onScroll);
	}, []);

	function toggleTheme() {
		setTheme(resolvedTheme === "dark" ? "light" : "dark");
	}

	return (
		<div
			style={{
				background: "var(--landing-bg)",
				color: "var(--landing-fg)",
				fontFamily:
					"-apple-system, BlinkMacSystemFont, 'Segoe UI Adjusted', 'Segoe UI', 'Liberation Sans', sans-serif",
			}}
		>
			{/* Nav */}
			<nav
				className="fixed top-0 left-0 right-0 z-50 transition-all duration-200"
				style={
					scrolled
						? {
								background:
									resolvedTheme === "dark" ? "rgba(25,25,25,0.85)" : "rgba(255,255,255,0.85)",
								backdropFilter: "blur(12px)",
								borderBottom: "1px solid var(--landing-border-light)",
							}
						: {}
				}
			>
				<div className="max-w-5xl mx-auto px-6 h-14 flex items-center justify-between">
					<Link href="/" className="flex items-center gap-2.5">
						<div className="w-7 h-7 rounded-md bg-neutral-900 dark:bg-white flex items-center justify-center">
							<span className="text-white dark:text-neutral-900 text-xs font-bold tracking-tight">
								LS
							</span>
						</div>
						<span className="font-semibold text-[15px]">LakeSync</span>
					</Link>

					<div
						className="hidden sm:flex items-center gap-6 text-sm"
						style={{ color: "var(--landing-fg-secondary)" }}
					>
						<a href="#features" className="hover:text-[--landing-fg] transition-colors">
							Features
						</a>
						<a href="#use-cases" className="hover:text-[--landing-fg] transition-colors">
							Use Cases
						</a>
						<Link href="/docs" className="hover:text-[--landing-fg] transition-colors">
							Docs
						</Link>
					</div>

					<div className="flex items-center gap-3">
						<button
							type="button"
							onClick={toggleTheme}
							className="w-8 h-8 flex items-center justify-center rounded-md hover:bg-[--landing-bg-secondary] transition-colors"
							style={{ color: "var(--landing-fg-secondary)" }}
						>
							{/* Sun icon (dark mode) */}
							<svg
								className="w-4 h-4 hidden dark:block"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								viewBox="0 0 24 24"
								role="img"
								aria-label="Toggle to light mode"
							>
								<circle cx="12" cy="12" r="5" />
								<path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42" />
							</svg>
							{/* Moon icon (light mode) */}
							<svg
								className="w-4 h-4 block dark:hidden"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								viewBox="0 0 24 24"
								role="img"
								aria-label="Toggle to dark mode"
							>
								<path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z" />
							</svg>
						</button>
						<Link
							href="/sign-up"
							className="hidden sm:inline-flex h-8 px-3.5 items-center rounded-md bg-neutral-900 dark:bg-white text-white dark:text-neutral-900 text-sm font-medium hover:bg-neutral-800 dark:hover:bg-neutral-100 transition-colors"
						>
							Dashboard
							<svg
								className="ml-1.5 w-3.5 h-3.5"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								viewBox="0 0 24 24"
								role="img"
								aria-label="Go to dashboard"
							>
								<path d="M5 12h14M12 5l7 7-7 7" />
							</svg>
						</Link>
					</div>
				</div>
			</nav>

			{/* Hero */}
			<section className="min-h-[90vh] flex items-center justify-center pt-14">
				<div className="max-w-5xl mx-auto px-6 py-24 text-center">
					<h1 className="text-4xl sm:text-5xl md:text-6xl font-bold tracking-tight leading-[1.1] animate-fade-up">
						Declare what data goes where.
						<br />
						<span style={{ color: "var(--landing-fg-tertiary)" }}>
							The engine handles the rest.
						</span>
					</h1>
					<p
						className="mt-6 text-lg sm:text-xl max-w-2xl mx-auto leading-relaxed animate-fade-up-delay"
						style={{ color: "var(--landing-fg-secondary)" }}
					>
						Sync, backup, migrate, and analyse data across SQL databases, SaaS tools, file stores,
						and AI systems — all from one managed platform.
					</p>
					<div className="mt-10 flex items-center justify-center gap-4 animate-fade-up-delay-2">
						<Link
							href="/sign-up"
							className="h-11 px-6 inline-flex items-center rounded-md bg-neutral-900 dark:bg-white text-white dark:text-neutral-900 font-medium hover:bg-neutral-800 dark:hover:bg-neutral-100 transition-colors"
						>
							Get Started Free
							<svg
								className="ml-2 w-4 h-4"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								viewBox="0 0 24 24"
								role="img"
								aria-label="Get started"
							>
								<path d="M5 12h14M12 5l7 7-7 7" />
							</svg>
						</Link>
						<Link
							href="/docs"
							className="h-11 px-6 inline-flex items-center rounded-md font-medium border transition-colors hover:bg-[--landing-bg-secondary]"
							style={{ borderColor: "var(--landing-border)" }}
						>
							<svg
								className="mr-2 w-4 h-4"
								fill="none"
								stroke="currentColor"
								strokeWidth="2"
								viewBox="0 0 24 24"
								role="img"
								aria-label="View documentation"
							>
								<polygon points="5 3 19 12 5 21 5 3" />
							</svg>
							View Docs
						</Link>
					</div>
					<p className="mt-4 text-sm" style={{ color: "var(--landing-fg-tertiary)" }}>
						No credit card required. Free tier included.
					</p>
				</div>
			</section>

			{/* Code Example */}
			<section className="py-16">
				<div className="max-w-3xl mx-auto px-6">
					<div className="code-block rounded-xl overflow-hidden shadow-lg">
						<div className="flex items-center gap-2 px-4 py-3 border-b border-white/10">
							<div className="w-3 h-3 rounded-full bg-white/20" />
							<div className="w-3 h-3 rounded-full bg-white/20" />
							<div className="w-3 h-3 rounded-full bg-white/20" />
							<span className="ml-2 text-xs text-white/40">app.ts</span>
						</div>
						<pre
							className="p-5 text-sm leading-relaxed overflow-x-auto"
							style={{ scrollbarWidth: "thin" }}
						>
							<code>
								<span className="kw">import</span>
								{" { "}
								<span className="fn">createClient</span>
								{" } "}
								<span className="kw">from</span> <span className="str">&quot;lakesync&quot;</span>
								{";"}
								{"\n\n"}
								<span className="kw">const</span> client = <span className="kw">await</span>{" "}
								<span className="fn">createClient</span>({"{"}
								{"\n  "}
								<span className="op">schemas:</span> [{"{"} <span className="op">table:</span>{" "}
								<span className="str">&quot;orders&quot;</span>
								{", "}
								<span className="op">columns:</span> [...] {"}"}]{",\n  "}
								<span className="op">gateway:</span> {"{"}
								{"\n    "}
								<span className="op">url:</span>{" "}
								<span className="str">&quot;https://api.lakesync.cloud&quot;</span>
								{"  "}
								<span className="cm">{"// we run the infra"}</span>
								{",\n    "}
								<span className="op">gatewayId:</span>{" "}
								<span className="str">&quot;your-project-id&quot;</span>
								{",\n    "}
								<span className="op">token:</span> apiToken
								{",\n  "}
								{"},\n"});
								{"\n\n"}
								<span className="cm">{"// That's it. Data syncs automatically."}</span>
								{"\n"}
								<span className="kw">const</span> orders = client.
								<span className="fn">query</span>(
								<span className="str">&quot;SELECT * FROM orders&quot;</span>);
							</code>
						</pre>
					</div>
				</div>
			</section>

			{/* Data Categories */}
			<section id="features" className="py-20">
				<div className="max-w-5xl mx-auto px-6">
					<h2 className="text-3xl sm:text-4xl font-bold text-center tracking-tight">
						One engine for all your data
					</h2>
					<p
						className="mt-4 text-center text-lg max-w-xl mx-auto"
						style={{ color: "var(--landing-fg-secondary)" }}
					>
						SQL databases, SaaS tools, file stores, AI systems. LakeSync bridges them all.
					</p>
					<div className="mt-16 grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
						{[
							{
								color: "#2383e2",
								bg: "rgba(35, 131, 226, 0.1)",
								title: "SQL Data",
								desc: "Postgres, MySQL, BigQuery. Sync between databases, materialise into destination tables, migrate without downtime.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#2383e2" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="SQL data"
									>
										<ellipse cx="12" cy="5" rx="9" ry="3" />
										<path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3" />
										<path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5" />
									</svg>
								),
							},
							{
								color: "#9b51e0",
								bg: "rgba(155, 81, 224, 0.1)",
								title: "SaaS Data",
								desc: "Jira, Salesforce, GitHub, Stripe. Continuous backup to Iceberg. Restore complex API objects from flat snapshots.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#9b51e0" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="SaaS data"
									>
										<path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z" />
									</svg>
								),
							},
							{
								color: "#27ae60",
								bg: "rgba(39, 174, 96, 0.1)",
								title: "File Data",
								desc: "S3, R2, Iceberg, Parquet. Immutable lakehouse storage with zero egress fees. Query backups directly with DuckDB.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#27ae60" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="File data"
									>
										<path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z" />
										<polyline points="13 2 13 9 20 9" />
									</svg>
								),
							},
							{
								color: "#f2994a",
								bg: "rgba(242, 153, 74, 0.1)",
								title: "AI Data",
								desc: "Keep vector databases in sync with production. Feed agents filtered subsets of live data. Embeddings always current.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#f2994a" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="AI data"
									>
										<path d="M12 2a4 4 0 0 1 4 4c0 1.95-1.4 3.57-3.25 3.92L12 22" />
										<path d="M12 2a4 4 0 0 0-4 4c0 1.95 1.4 3.57 3.25 3.92" />
										<path d="M5 10c0 2.76 3.13 5 7 5s7-2.24 7-5" />
									</svg>
								),
							},
						].map((cat) => (
							<div
								key={cat.title}
								className="rounded-xl p-6 border transition-colors"
								style={{
									borderColor: "var(--landing-border-light)",
									background: "var(--landing-bg-secondary)",
								}}
							>
								<div
									className="w-10 h-10 rounded-lg flex items-center justify-center mb-4"
									style={{ background: cat.bg }}
								>
									{cat.icon}
								</div>
								<h3 className="font-semibold text-[15px]">{cat.title}</h3>
								<p
									className="mt-2 text-sm leading-relaxed"
									style={{ color: "var(--landing-fg-secondary)" }}
								>
									{cat.desc}
								</p>
							</div>
						))}
					</div>
				</div>
			</section>

			{/* Use Cases */}
			<section
				id="use-cases"
				className="py-20"
				style={{ background: "var(--landing-bg-secondary)" }}
			>
				<div className="max-w-5xl mx-auto px-6">
					<h2 className="text-3xl sm:text-4xl font-bold text-center tracking-tight">Use cases</h2>
					<p
						className="mt-4 text-center text-lg max-w-xl mx-auto"
						style={{ color: "var(--landing-fg-secondary)" }}
					>
						Features usually locked behind expensive enterprise tools. Yours out of the box.
					</p>
					<div className="mt-14 space-y-4">
						{[
							{
								tag: "Resilience",
								tagColor: "#2383e2",
								tagBg: "rgba(35, 131, 226, 0.1)",
								items: [
									{
										title: "Continuous Backup",
										desc: "RDS, Cloud SQL, SaaS tools \u2192 immutable Iceberg snapshots on R2. Time-machine recovery at a fraction of cloud backup cost.",
									},
									{
										title: "SaaS Restore",
										desc: "Translate flat Parquet rows back into complex SaaS API objects. Restore Jira issues, GitHub repos, or Salesforce records from your lake.",
									},
								],
							},
							{
								tag: "Analytics",
								tagColor: "#27ae60",
								tagBg: "rgba(39, 174, 96, 0.1)",
								items: [
									{
										title: "Cold Analytics",
										desc: "Query your backups directly with SQL via DuckDB or BigQuery. No warehouse needed — your lake is already queryable.",
									},
								],
							},
							{
								tag: "AI / ML",
								tagColor: "#f2994a",
								tagBg: "rgba(242, 153, 74, 0.1)",
								items: [
									{
										title: "Real-time Vector Sync",
										desc: "Keep Pinecone, Qdrant, or any vector DB perfectly in sync with production data. Embeddings update as your data changes — no batch jobs.",
									},
								],
							},
							{
								tag: "Migration",
								tagColor: "#9b51e0",
								tagBg: "rgba(155, 81, 224, 0.1)",
								items: [
									{
										title: "Zero-Downtime Migration",
										desc: "Stream data in parallel from legacy or on-prem to modern cloud databases. Cutover in seconds, not hours.",
									},
									{
										title: "NoSQL to SQL",
										desc: "Automatically flatten DynamoDB or MongoDB documents into clean relational tables in Postgres or MySQL.",
									},
								],
							},
							{
								tag: "Compliance",
								tagColor: "#eb5757",
								tagBg: "rgba(235, 87, 87, 0.1)",
								items: [
									{
										title: "Audit Offloading",
										desc: "Meet 7-10 year retention laws by streaming application logs and audit trails to cheap, queryable R2 archive storage.",
									},
								],
							},
							{
								tag: "Development",
								tagColor: "var(--landing-fg-secondary)",
								tagBg: "var(--landing-hover, rgba(55, 53, 47, 0.08))",
								items: [
									{
										title: "Sandbox Seeding",
										desc: "Populate staging and test environments with real (but sanitised) production data in seconds.",
									},
								],
							},
						].map((group) => (
							<div
								key={group.tag}
								className="rounded-xl border overflow-hidden"
								style={{
									borderColor: "var(--landing-border-light)",
									background: "var(--landing-bg)",
								}}
							>
								<div
									className="px-6 py-4 flex items-center gap-3 border-b"
									style={{ borderColor: "var(--landing-border-light)" }}
								>
									<span
										className="text-xs font-semibold uppercase tracking-wider px-2.5 py-1 rounded-md"
										style={{ background: group.tagBg, color: group.tagColor }}
									>
										{group.tag}
									</span>
								</div>
								<div
									className={`${group.items.length > 1 ? "grid md:grid-cols-2 divide-y md:divide-y-0 md:divide-x" : ""}`}
									style={{ borderColor: "var(--landing-border-light)" }}
								>
									{group.items.map((item) => (
										<div key={item.title} className="px-6 py-5">
											<h4 className="font-semibold text-sm">{item.title}</h4>
											<p
												className="mt-1.5 text-sm leading-relaxed"
												style={{ color: "var(--landing-fg-secondary)" }}
											>
												{item.desc}
											</p>
										</div>
									))}
								</div>
							</div>
						))}
					</div>
				</div>
			</section>

			{/* Why LakeSync */}
			<section className="py-20">
				<div className="max-w-5xl mx-auto px-6">
					<h2 className="text-3xl sm:text-4xl font-bold text-center tracking-tight">
						Why LakeSync
					</h2>
					<div className="mt-14 grid md:grid-cols-3 gap-8">
						{[
							{
								color: "#2383e2",
								bg: "rgba(35, 131, 226, 0.1)",
								title: "Read-Only Utility",
								desc: "Your backup isn't just a backup. It's a live, queryable dataset for AI, analytics, and development.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#2383e2" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="SQL data"
									>
										<path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z" />
										<path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z" />
									</svg>
								),
							},
							{
								color: "#27ae60",
								bg: "rgba(39, 174, 96, 0.1)",
								title: "Private by Default",
								desc: "Data never leaves your network unnecessarily. LakeSync manages the encrypted flow — you control where it lands.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#27ae60" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="File data"
									>
										<rect x="3" y="11" width="18" height="11" rx="2" ry="2" />
										<path d="M7 11V7a5 5 0 0 1 10 0v4" />
									</svg>
								),
							},
							{
								color: "#f2994a",
								bg: "rgba(242, 153, 74, 0.1)",
								title: "Zero Egress Fees",
								desc: "Built on Cloudflare R2. No egress costs for reads or migrations. Enterprise-grade data movement without the enterprise bill.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#f2994a" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="AI data"
									>
										<line x1="12" y1="1" x2="12" y2="23" />
										<path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
									</svg>
								),
							},
						].map((item) => (
							<div key={item.title} className="text-center">
								<div
									className="w-12 h-12 rounded-full flex items-center justify-center mx-auto"
									style={{ background: item.bg }}
								>
									{item.icon}
								</div>
								<h3 className="mt-5 font-semibold text-[15px]">{item.title}</h3>
								<p
									className="mt-2 text-sm leading-relaxed"
									style={{ color: "var(--landing-fg-secondary)" }}
								>
									{item.desc}
								</p>
							</div>
						))}
					</div>
				</div>
			</section>

			{/* Adapters */}
			<section className="py-16" style={{ background: "var(--landing-bg-secondary)" }}>
				<div className="max-w-5xl mx-auto px-6 text-center">
					<h3
						className="text-sm font-semibold uppercase tracking-wider"
						style={{ color: "var(--landing-fg-tertiary)" }}
					>
						Connect anything — every adapter is a source and destination
					</h3>
					<div className="mt-6 flex flex-wrap items-center justify-center gap-3">
						{[
							"PostgreSQL",
							"MySQL",
							"BigQuery",
							"S3 / R2",
							"Iceberg",
							"Jira",
							"Salesforce",
							"DynamoDB",
							"MongoDB",
							"SQLite",
							"Pinecone",
							"CloudWatch",
							"Stripe",
						].map((a) => (
							<span
								key={a}
								className="h-9 px-4 inline-flex items-center rounded-full text-sm border"
								style={{
									borderColor: "var(--landing-border)",
									color: "var(--landing-fg-secondary)",
								}}
							>
								{a}
							</span>
						))}
						<span
							className="h-9 px-4 inline-flex items-center rounded-full text-sm border border-dashed"
							style={{ borderColor: "var(--landing-border)", color: "var(--landing-fg-tertiary)" }}
						>
							Your adapter
						</span>
					</div>
				</div>
			</section>

			{/* CTA */}
			<CtaSection />

			{/* Footer */}
			<footer className="py-10 border-t" style={{ borderColor: "var(--landing-border-light)" }}>
				<div className="max-w-5xl mx-auto px-6 flex flex-col sm:flex-row items-center justify-between gap-4">
					<div className="flex items-center gap-2.5">
						<div className="w-6 h-6 rounded bg-neutral-900 dark:bg-white flex items-center justify-center">
							<span className="text-white dark:text-neutral-900 text-[10px] font-bold">LS</span>
						</div>
						<span className="text-sm font-medium">LakeSync</span>
					</div>
					<div
						className="flex items-center gap-6 text-sm"
						style={{ color: "var(--landing-fg-secondary)" }}
					>
						<Link href="/docs" className="hover:text-[--landing-fg] transition-colors">
							Docs
						</Link>
						<Link href="/dashboard" className="hover:text-[--landing-fg] transition-colors">
							Dashboard
						</Link>
						<a
							href="https://github.com/radekdymacz/lakesync"
							className="hover:text-[--landing-fg] transition-colors"
						>
							GitHub
						</a>
					</div>
					<p className="text-sm" style={{ color: "var(--landing-fg-tertiary)" }}>
						&copy; 2026 LakeSync
					</p>
				</div>
			</footer>
		</div>
	);
}

function CtaSection() {
	return (
		<section className="py-20" style={{ background: "var(--landing-bg-secondary)" }}>
			<div className="max-w-xl mx-auto px-6 text-center">
				<h2 className="text-3xl sm:text-4xl font-bold tracking-tight">Start syncing today</h2>
				<p className="mt-4 text-lg" style={{ color: "var(--landing-fg-secondary)" }}>
					Create a free account and have your first gateway running in minutes.
				</p>
				<div className="mt-8 flex flex-col sm:flex-row gap-3 justify-center">
					<Link
						href="/sign-up"
						className="h-11 px-8 rounded-md bg-neutral-900 dark:bg-white text-white dark:text-neutral-900 font-medium text-sm hover:bg-neutral-800 dark:hover:bg-neutral-100 transition-colors inline-flex items-center justify-center"
					>
						Get started free
					</Link>
					<Link
						href="/sign-in"
						className="h-11 px-8 rounded-md border font-medium text-sm hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors inline-flex items-center justify-center"
						style={{ borderColor: "var(--landing-border)", color: "var(--landing-fg)" }}
					>
						Sign in
					</Link>
				</div>
				<p className="mt-4 text-sm" style={{ color: "var(--landing-fg-tertiary)" }}>
					Or self-host with{" "}
					<code
						className="font-mono text-xs px-1.5 py-0.5 rounded"
						style={{ background: "var(--landing-border-light)" }}
					>
						npm install lakesync
					</code>
				</p>
			</div>
		</section>
	);
}
