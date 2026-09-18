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
						Open-source, offline-first sync for TypeScript apps. Your data lives in SQLite on the
						device, syncs through a lightweight gateway, and flushes to the backend you choose —
						Postgres, BigQuery, or Iceberg on S3/R2.
					</p>
					<div className="mt-10 flex items-center justify-center gap-4 animate-fade-up-delay-2">
						<Link
							href="/docs/getting-started"
							className="h-11 px-6 inline-flex items-center rounded-md bg-neutral-900 dark:bg-white text-white dark:text-neutral-900 font-medium hover:bg-neutral-800 dark:hover:bg-neutral-100 transition-colors"
						>
							Get started
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
						Apache 2.0. Self-host or run at the edge.
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
								<span className="kw">from</span>{" "}
								<span className="str">&quot;lakesync/client&quot;</span>
								{";"}
								{"\n\n"}
								<span className="kw">const</span> client = <span className="kw">await</span>{" "}
								<span className="fn">createClient</span>({"{"}
								{"\n  "}
								<span className="op">name:</span> <span className="str">&quot;my-app&quot;</span>
								{",\n  "}
								<span className="op">clientId:</span>{" "}
								<span className="str">&quot;client-1&quot;</span>
								{",\n  "}
								<span className="op">schemas:</span> [{"{"} <span className="op">table:</span>{" "}
								<span className="str">&quot;orders&quot;</span>
								{", "}
								<span className="op">columns:</span> [...] {"}"}]{",\n  "}
								<span className="op">gateway:</span> {"{"}
								{"\n    "}
								<span className="op">url:</span>{" "}
								<span className="str">&quot;https://your-gateway.example.com&quot;</span>
								{",\n    "}
								<span className="op">gatewayId:</span> <span className="str">&quot;gw-1&quot;</span>
								{",\n    "}
								<span className="op">token:</span> jwtToken
								{",\n  "}
								{"},\n"});
								{"\n\n"}
								<span className="cm">{"// Writes hit local SQLite. Sync is automatic."}</span>
								{"\n"}
								<span className="kw">const</span> orders = <span className="kw">await</span>{" "}
								client.db.
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
						Offline-first. Any backend.
					</h2>
					<p
						className="mt-4 text-center text-lg max-w-xl mx-auto"
						style={{ color: "var(--landing-fg-secondary)" }}
					>
						Local SQLite in the browser, a pluggable gateway, and adapters for SQL, object storage,
						and SaaS sources.
					</p>
					<div className="mt-16 grid sm:grid-cols-2 lg:grid-cols-4 gap-5">
						{[
							{
								color: "#2383e2",
								bg: "rgba(35, 131, 226, 0.1)",
								title: "SQL Data",
								desc: "Postgres, MySQL, BigQuery. Flush deltas, materialise queryable destination tables, or migrate between adapters without changing client code.",
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
								desc: "Jira and Salesforce connectors poll issues, accounts, and more into the same gateway. The adapter interface is the extension point for any readable API.",
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
								desc: "S3, R2, Iceberg, Parquet. Batch flush to object storage. Query snapshots with DuckDB, Spark, Athena, or Trino — zero ETL.",
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
								title: "Local SQLite",
								desc: "The full working set lives on the device. Edits queue in IndexedDB, survive refresh, and drain when connectivity returns.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#f2994a" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="Local SQLite"
									>
										<rect x="2" y="3" width="20" height="14" rx="2" />
										<line x1="8" y1="21" x2="16" y2="21" />
										<line x1="12" y1="17" x2="12" y2="21" />
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
						One client SDK. A gateway you can self-host. Backends you already run.
					</p>
					<div className="mt-14 space-y-4">
						{[
							{
								tag: "Offline-first",
								tagColor: "#2383e2",
								tagBg: "rgba(35, 131, 226, 0.1)",
								items: [
									{
										title: "Apps that work on a plane",
										desc: "The full dataset lives in local SQLite. The IndexedDB outbox survives refresh and crash. When the network returns, catch-up is automatic.",
									},
									{
										title: "Column-level merge",
										desc: "Two users edit different fields of the same row — both changes are kept. Hybrid Logical Clocks plus last-write-wins only collide on the same column.",
									},
								],
							},
							{
								tag: "Backends",
								tagColor: "#27ae60",
								tagBg: "rgba(39, 174, 96, 0.1)",
								items: [
									{
										title: "Right-size storage",
										desc: "Postgres or MySQL for operational data. BigQuery for analytics. Iceberg on S3/R2 for large data. CompositeAdapter routes by table.",
									},
									{
										title: "Materialise & fan-out",
										desc: "Database adapters create queryable destination tables after flush. FanOutAdapter replicates to a secondary without blocking the write path.",
									},
								],
							},
							{
								tag: "Sources",
								tagColor: "#9b51e0",
								tagBg: "rgba(155, 81, 224, 0.1)",
								items: [
									{
										title: "Jira & Salesforce ingest",
										desc: "Shipped connectors poll Jira Cloud and Salesforce into the same gateway. Cursor or diff strategies, chunked push, memory-aware flush.",
									},
									{
										title: "Bring your own adapter",
										desc: "Implement LakeAdapter or DatabaseAdapter for any readable or writable system. Same client code regardless of backend.",
									},
								],
							},
							{
								tag: "Deploy",
								tagColor: "#f2994a",
								tagBg: "rgba(242, 153, 74, 0.1)",
								items: [
									{
										title: "Edge or self-hosted",
										desc: "Run the gateway on Cloudflare Workers and Durable Objects, or as a Node/Bun HTTP + WebSocket server with SQLite WAL persistence.",
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
								title: "Offline-first",
								desc: "Zero-latency local writes. Persistent outbox. Automatic drain on reconnect. The app keeps working when the network does not.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#2383e2" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="Offline-first"
									>
										<rect x="2" y="3" width="20" height="14" rx="2" />
										<line x1="8" y1="21" x2="16" y2="21" />
										<line x1="12" y1="17" x2="12" y2="21" />
									</svg>
								),
							},
							{
								color: "#27ae60",
								bg: "rgba(39, 174, 96, 0.1)",
								title: "Pluggable backends",
								desc: "Sync is decoupled from storage. Swap Postgres for Iceberg — or run both — without rewriting the client.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#27ae60" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="Pluggable backends"
									>
										<rect x="3" y="3" width="7" height="7" />
										<rect x="14" y="3" width="7" height="7" />
										<rect x="14" y="14" width="7" height="7" />
										<rect x="3" y="14" width="7" height="7" />
									</svg>
								),
							},
							{
								color: "#f2994a",
								bg: "rgba(242, 153, 74, 0.1)",
								title: "Open source",
								desc: "Apache 2.0. Self-host the gateway on Node, Bun, or Cloudflare Workers. The library is the product.",
								icon: (
									<svg
										className="w-5 h-5"
										style={{ color: "#f2994a" }}
										fill="none"
										stroke="currentColor"
										strokeWidth="2"
										viewBox="0 0 24 24"
										role="img"
										aria-label="Open source"
									>
										<circle cx="12" cy="12" r="10" />
										<polyline points="12 6 12 12 16 14" />
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
							"SQLite",
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
						<Link
							href="/docs/getting-started"
							className="hover:text-[--landing-fg] transition-colors"
						>
							Getting started
						</Link>
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
					Install the SDK, point it at a gateway, and keep a local SQLite that stays in sync.
				</p>
				<div className="mt-8 flex flex-col sm:flex-row gap-3 justify-center">
					<Link
						href="/docs/getting-started"
						className="h-11 px-8 rounded-md bg-neutral-900 dark:bg-white text-white dark:text-neutral-900 font-medium text-sm hover:bg-neutral-800 dark:hover:bg-neutral-100 transition-colors inline-flex items-center justify-center"
					>
						Get started
					</Link>
					<Link
						href="/docs"
						className="h-11 px-8 rounded-md border font-medium text-sm hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors inline-flex items-center justify-center"
						style={{ borderColor: "var(--landing-border)", color: "var(--landing-fg)" }}
					>
						View docs
					</Link>
				</div>
				<p className="mt-4 text-sm" style={{ color: "var(--landing-fg-tertiary)" }}>
					Self-host with{" "}
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
