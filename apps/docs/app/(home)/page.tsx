import Link from "next/link";
import { Mermaid } from "@/components/mdx/mermaid";

const CORE_FLOW = `sequenceDiagram
    participant Consumer as Web App / Agent
    participant DB as Local SQLite
    participant GW as Gateway
    participant Source as Any Data Source

    Consumer->>DB: read / write
    Note over Consumer,DB: Zero latency — local working set
    DB-->>GW: Push local changes
    GW-->>DB: Pull from source
    Note over GW: Sync rules decide<br/>what data flows where
    GW->>Source: Read / write via adapter
    Note over Source: Postgres? BigQuery?<br/>CloudWatch? S3?<br/>If you can read it, it's a source.`;

const AGENT_FLOW = `sequenceDiagram
    participant Agent as AI Agent
    participant DB as Local SQLite
    participant GW as Gateway
    participant CW as CloudWatch Logs

    Agent->>GW: "errors from last 24h"
    Note over GW: Sync rule evaluates query
    GW->>CW: query via adapter
    CW-->>GW: filtered log entries
    GW-->>DB: sync as deltas
    Agent->>DB: SELECT * FROM errors
    Note over Agent,DB: Local working copy<br/>Agent reasons over it`;

const WEBAPP_FLOW = `sequenceDiagram
    participant App as Web App (offline)
    participant DB as Local SQLite
    participant Q as Outbox (IndexedDB)
    participant GW as Gateway
    participant PG as Postgres

    App->>DB: Edit 1, Edit 2, Edit 3
    DB-->>Q: Deltas queued
    Note over App,Q: Fully functional offline
    Note over Q,GW: ← Connection restored →
    Q->>GW: Push all deltas
    GW->>PG: Flush to database
    GW-->>App: Pull remote changes
    Note over App,GW: Caught up ✓`;

const CROSS_BACKEND = `sequenceDiagram
    participant PG as Postgres
    participant GW as Gateway
    participant BQ as BigQuery
    participant ICE as Iceberg (S3/R2)

    Note over GW: Sync rules define flows
    PG->>GW: read operational data
    GW->>BQ: write for analytics
    Note over BQ: Dashboards query here
    PG->>GW: read data older than 90d
    GW->>ICE: archive to data lake
    Note over ICE: Long-term storage<br/>Open format`;

const CONFLICT_RESOLUTION = `sequenceDiagram
    participant A as Alice
    participant GW as Gateway
    participant B as Bob

    Note over A,B: Both editing the same todo
    A->>A: title = "Buy oat milk"
    B->>B: status = "done"
    A->>GW: push delta (title, HLC=100)
    B->>GW: push delta (status, HLC=101)
    Note over GW: Column-level merge<br/>title ← Alice (HLC 100)<br/>status ← Bob (HLC 101)
    GW->>A: pull → status = "done"
    GW->>B: pull → title = "Buy oat milk"
    Note over A,B: Both changes preserved ✓`;

const SYNC_RULES = `sequenceDiagram
    participant A as Alice (team=eng)
    participant GW as Gateway
    participant B as Agent (role=ops)

    Note over GW: Sync rules:<br/>filter by claims
    A->>GW: pull (JWT: team=eng)
    GW-->>A: eng data only
    B->>GW: pull (role=ops, ts>24h ago)
    GW-->>B: ops errors, last 24h
    Note over A,B: Each consumer gets<br/>exactly what it needs`;

export default function HomePage() {
	return (
		<main className="flex flex-1 flex-col items-center">
			{/* Hero */}
			<section className="flex w-full flex-col items-center px-4 pb-24 pt-16">
				<div className="mb-6 rounded-full border border-fd-border bg-fd-accent/50 px-4 py-1.5 text-xs font-medium text-fd-muted-foreground">
					Experimental &mdash; under active development
				</div>

				<h1 className="mb-6 max-w-4xl text-center text-5xl font-bold tracking-tight sm:text-6xl lg:text-7xl">
					Sync any data source{" "}
					<span className="text-fd-muted-foreground">
						to a local working set.
					</span>
				</h1>

				<p className="mx-auto mb-10 max-w-2xl text-center text-lg leading-relaxed text-fd-muted-foreground">
					Your app or agent declares what it needs. The engine handles the
					rest. Postgres, BigQuery, S3, CloudWatch &mdash; if you can read
					from it, it&apos;s a data source. LakeSync syncs a filtered subset
					to local SQLite so you can query it, display it, or reason over it.
				</p>

				<div className="flex flex-wrap items-center justify-center gap-4">
					<Link
						href="/docs/getting-started"
						className="rounded-lg bg-fd-primary px-8 py-3 text-sm font-medium text-fd-primary-foreground transition-colors hover:bg-fd-primary/90"
					>
						Get Started
					</Link>
					<a
						href="https://github.com/radekdymacz/lakesync"
						target="_blank"
						rel="noopener noreferrer"
						className="rounded-lg border border-fd-border px-8 py-3 text-sm font-medium transition-colors hover:bg-fd-accent"
					>
						View on GitHub
					</a>
				</div>

				<div className="mt-12 w-full max-w-xl rounded-lg border border-fd-border bg-fd-card/50 px-6 py-4 font-mono text-sm text-fd-muted-foreground">
					<span className="select-none text-fd-muted-foreground/50">$ </span>
					npm install lakesync
				</div>
			</section>

			{/* Core architecture */}
			<DiagramSection
				label="How it works"
				title="Any source → Gateway → Local SQLite"
				description="The gateway connects to data sources via pluggable adapters. Sync rules define what data flows to each consumer. Your app or agent gets a local SQLite working set — queryable, offline-capable, and always up to date."
				chart={CORE_FLOW}
				border
			/>

			{/* Agent use case */}
			<DiagramSection
				label="For AI agents"
				title="Give your agents a synced working set"
				description="An agent says &ldquo;I need the last 24 hours of errors.&rdquo; The gateway evaluates the sync rule, queries CloudWatch via its adapter, and syncs the result to local SQLite. The agent reasons over a local copy — no API calls per question."
				chart={AGENT_FLOW}
			/>

			{/* Web app use case */}
			<DiagramSection
				label="For web apps"
				title="Offline-first. Catches up when it reconnects."
				description="The full working set lives in local SQLite. Edits queue in a persistent IndexedDB outbox that survives page refreshes and browser crashes. When connectivity returns, the outbox drains automatically and remote changes sync down."
				chart={WEBAPP_FLOW}
				border
			/>

			{/* The adapter story */}
			<section className="w-full px-4 py-24">
				<div className="mx-auto max-w-5xl">
					<div className="mb-3 text-center text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
						Pluggable adapters
					</div>
					<h2 className="mb-6 text-center text-3xl font-bold">
						Any data source. Same interface.
					</h2>
					<p className="mx-auto mb-16 max-w-2xl text-center leading-relaxed text-fd-muted-foreground">
						If you can read from it, it can be a LakeSync adapter. The
						adapter interface abstracts the source &mdash; consumers don&apos;t
						know or care where the data lives. Adapters are both sources and
						destinations, enabling cross-backend flows.
					</p>

					<div className="mx-auto grid max-w-5xl grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-4">
						<AdapterCard
							name="Postgres / MySQL"
							description="Operational OLTP data. Familiar SQL tooling. Low-latency reads and writes."
						/>
						<AdapterCard
							name="BigQuery"
							description="Analytics-scale queries. Managed, serverless. Connect BI tools directly."
						/>
						<AdapterCard
							name="S3 / R2 (Iceberg)"
							description="Massive scale on object storage. Open format. Query with any engine."
						/>
						<AdapterCard
							name="Anything else"
							description="CloudWatch, Stripe, custom APIs. Implement the adapter interface and it's a source."
							highlight
						/>
					</div>
				</div>
			</section>

			{/* Cross-backend flows */}
			<DiagramSection
				label="Cross-backend"
				title="Move data between any two backends"
				description="Sync rules define directional flows. Archive old operational data from Postgres to Iceberg. Materialise query results from Iceberg into BigQuery. The gateway routes data between adapters based on rules you define."
				chart={CROSS_BACKEND}
				border
			/>

			{/* Sync rules */}
			<DiagramSection
				label="Sync rules"
				title="Declarative rules define what flows where"
				description="Sync rules are the core primitive. They define which data each consumer sees — filtered by JWT claims, temporal ranges, or any column predicate. A web app gets its team's data. An agent gets the errors it needs to analyse."
				chart={SYNC_RULES}
			/>

			{/* Conflict resolution */}
			<DiagramSection
				label="Conflict resolution"
				title="Column-level merge, not row-level overwrite"
				description="Concurrent edits to different fields of the same record are both preserved. Hybrid logical clocks provide causal ordering with deterministic client ID tiebreaking."
				chart={CONFLICT_RESOLUTION}
				border
			/>

			{/* Design decisions */}
			<section className="w-full px-4 py-24">
				<div className="mx-auto max-w-5xl">
					<div className="mb-3 text-center text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
						Under the hood
					</div>
					<h2 className="mb-12 text-center text-3xl font-bold">
						Design decisions
					</h2>
					<div className="grid grid-cols-1 gap-6 md:grid-cols-3">
						<Feature
							title="Adapter = data source"
							description="Any system you can read from becomes a LakeSync source. Postgres, BigQuery, Iceberg, CloudWatch, or your own custom adapter."
						/>
						<Feature
							title="Hybrid Logical Clocks"
							description="64-bit branded bigint — 48-bit wall clock + 16-bit counter. Causal ordering across clients without centralised coordination."
						/>
						<Feature
							title="Column-level LWW"
							description="Conflicts resolved per-column, not per-row. Concurrent edits to different fields never overwrite each other."
						/>
						<Feature
							title="Result&lt;T, E&gt; everywhere"
							description="Public APIs never throw. Error paths are explicit, composable, and impossible to accidentally ignore."
						/>
						<Feature
							title="Persistent outbox"
							description="Deltas queue in IndexedDB. Survives page refreshes, browser crashes, and network outages. Drains automatically."
						/>
						<Feature
							title="Sync rules as the product"
							description="Declarative rules define what data flows to each consumer. Filter by claims, columns, time ranges. The DSL is the interface between consumers and data."
						/>
					</div>
				</div>
			</section>

			{/* Status + CTA */}
			<section className="w-full border-t border-fd-border bg-fd-card/30 px-4 py-24">
				<div className="mx-auto max-w-2xl text-center">
					<div className="mb-3 text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
						Status
					</div>
					<h2 className="mb-4 text-3xl font-bold">Experimental, but real</h2>
					<p className="mb-8 leading-relaxed text-fd-muted-foreground">
						Core sync engine, conflict resolution, client SDK, Cloudflare Workers
						gateway, compaction, checkpoint generation, sync rules, and adapters
						for Postgres, MySQL, BigQuery, and S3/R2 are all implemented and
						tested. Cross-backend flows and the extended sync rules DSL are
						next. API is not yet stable &mdash; expect breaking changes.
					</p>
					<div className="flex flex-wrap items-center justify-center gap-4">
						<Link
							href="/docs/getting-started"
							className="rounded-lg bg-fd-primary px-8 py-3 text-sm font-medium text-fd-primary-foreground transition-colors hover:bg-fd-primary/90"
						>
							Try it out
						</Link>
						<Link
							href="/docs/architecture"
							className="rounded-lg border border-fd-border px-8 py-3 text-sm font-medium transition-colors hover:bg-fd-accent"
						>
							Read the architecture
						</Link>
					</div>
				</div>
			</section>
		</main>
	);
}

function DiagramSection({
	label,
	title,
	description,
	chart,
	border,
}: {
	label: string;
	title: string;
	description: string;
	chart: string;
	border?: boolean;
}) {
	return (
		<section
			className={`w-full px-4 py-24 ${border ? "border-y border-fd-border bg-fd-card/30" : ""}`}
		>
			<div className="mx-auto max-w-5xl">
				<div className="mb-3 text-center text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
					{label}
				</div>
				<h2 className="mb-4 text-center text-3xl font-bold">{title}</h2>
				<p className="mx-auto mb-12 max-w-2xl text-center leading-relaxed text-fd-muted-foreground">
					{description}
				</p>
				<div className="mx-auto max-w-3xl overflow-x-auto rounded-xl border border-fd-border p-6">
					<Mermaid chart={chart} />
				</div>
			</div>
		</section>
	);
}

function AdapterCard({
	name,
	description,
	highlight,
}: { name: string; description: string; highlight?: boolean }) {
	return (
		<div
			className={`rounded-xl border p-5 ${highlight ? "border-fd-primary/50 bg-fd-primary/5" : "border-fd-border"}`}
		>
			<h3 className="mb-2 font-semibold">{name}</h3>
			<p className="text-sm leading-relaxed text-fd-muted-foreground">
				{description}
			</p>
		</div>
	);
}

function Feature({
	title,
	description,
}: { title: string; description: string }) {
	return (
		<div className="rounded-xl border border-fd-border p-6">
			<h3 className="mb-2 font-semibold">{title}</h3>
			<p className="text-sm leading-relaxed text-fd-muted-foreground">
				{description}
			</p>
		</div>
	);
}
