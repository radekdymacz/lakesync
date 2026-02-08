import Link from "next/link";
import { Mermaid } from "@/components/mdx/mermaid";

const CORE_FLOW = `sequenceDiagram
    participant App as Your App
    participant DB as Local SQLite
    participant GW as Gateway (CF DO)
    participant Backend as Any Backend

    App->>DB: INSERT / UPDATE / DELETE
    Note over App,DB: Zero latency — local write
    DB-->>GW: Push column-level deltas
    GW-->>DB: ACK + pull remote changes
    Note over GW: Deltas merge via HLC + LWW
    GW->>Backend: Batch flush
    Note over Backend: Postgres? R2? S3?<br/>You choose the adapter.`;

const SMALL_DATA = `sequenceDiagram
    participant Client as Client (SQLite)
    participant GW as Gateway
    participant DB as Postgres / MySQL / RDS

    Client->>GW: push deltas
    GW->>DB: flush batch
    Note over DB: Familiar tooling<br/>Standard SQL queries<br/>Works with your existing stack
    Client->>GW: pull since last HLC
    GW-->>Client: new deltas`;

const LARGE_DATA = `sequenceDiagram
    participant Client as Client (SQLite)
    participant GW as Gateway
    participant Lake as Iceberg (S3/R2)
    participant BI as Analytics / BI

    Client->>GW: push deltas
    GW->>Lake: flush → Parquet files
    Note over Lake: Apache Iceberg table<br/>Open format, infinite scale
    BI->>Lake: SELECT * FROM events
    Note over BI,Lake: Query with Spark, DuckDB,<br/>Athena, Trino — no ETL
    Note over Client,Lake: Operational data IS analytics data`;

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

const OFFLINE_SYNC = `sequenceDiagram
    participant App as App (offline)
    participant DB as Local SQLite
    participant Q as Outbox (IndexedDB)
    participant GW as Gateway

    App->>DB: Edit 1
    DB-->>Q: Delta queued
    App->>DB: Edit 2
    DB-->>Q: Delta queued
    App->>DB: Edit 3
    DB-->>Q: Delta queued
    Note over App,Q: Fully functional offline
    Note over Q,GW: ← Connection restored →
    Q->>GW: Push all 3 deltas
    GW-->>Q: ACK
    GW->>App: Pull remote changes
    Note over App,GW: Caught up ✓`;

const SYNC_RULES = `sequenceDiagram
    participant A as Alice (team=eng)
    participant GW as Gateway
    participant B as Bob (team=sales)

    Note over GW: Sync rule:<br/>filter by jwt:team claim
    A->>GW: pull (JWT: team=eng)
    GW-->>A: eng todos only
    B->>GW: pull (JWT: team=sales)
    GW-->>B: sales todos only
    Note over A,B: Each client sees only their data`;

export default function HomePage() {
	return (
		<main className="flex flex-1 flex-col items-center">
			{/* Hero */}
			<section className="flex w-full flex-col items-center px-4 pb-24 pt-16">
				<div className="mb-6 rounded-full border border-fd-border bg-fd-accent/50 px-4 py-1.5 text-xs font-medium text-fd-muted-foreground">
					Experimental &mdash; under active development
				</div>

				<h1 className="mb-6 max-w-4xl text-center text-5xl font-bold tracking-tight sm:text-6xl lg:text-7xl">
					Local-first sync.{" "}
					<span className="text-fd-muted-foreground">Any backend.</span>
				</h1>

				<p className="mx-auto mb-10 max-w-2xl text-center text-lg leading-relaxed text-fd-muted-foreground">
					LakeSync is an open-source sync engine for local-first TypeScript apps.
					Your data lives in SQLite on the device, syncs through a lightweight
					gateway, and flushes to the backend of your choice &mdash; Postgres for
					small data, S3/R2 via Apache Iceberg for large data. Same client code
					either way.
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
				title="Client SQLite → Gateway → Any Backend"
				description="Mutations write to local SQLite with zero latency. Column-level deltas push to a lightweight gateway that merges via hybrid logical clocks, then flushes in batches to whatever backend you choose."
				chart={CORE_FLOW}
				border
			/>

			{/* The adapter story */}
			<section className="w-full px-4 py-24">
				<div className="mx-auto max-w-5xl">
					<div className="mb-3 text-center text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
						Pluggable backends
					</div>
					<h2 className="mb-6 text-center text-3xl font-bold">
						Right-size your backend
					</h2>
					<p className="mx-auto mb-16 max-w-2xl text-center leading-relaxed text-fd-muted-foreground">
						The gateway speaks a simple adapter interface &mdash; put, get, list,
						delete. Implement it for any storage you like. Start with a database
						you already know, switch to the lakehouse when your data outgrows it.
						Client code stays the same.
					</p>

					<div className="mx-auto grid max-w-5xl grid-cols-1 gap-12 md:grid-cols-2">
						<div>
							<h3 className="mb-3 text-sm font-medium uppercase tracking-wider text-fd-muted-foreground">
								Small data &mdash; use what you know
							</h3>
							<div className="space-y-3 rounded-xl border border-fd-border p-5">
								<div className="font-mono text-sm text-fd-muted-foreground">
									Client SQLite → Gateway → <strong className="text-fd-foreground">Postgres / MySQL / RDS</strong>
								</div>
								<ul className="space-y-1 text-sm text-fd-muted-foreground">
									<li>Familiar tooling and SQL queries</li>
									<li>Works with your existing stack</li>
									<li>Simple operational model</li>
								</ul>
							</div>
						</div>
						<div>
							<h3 className="mb-3 text-sm font-medium uppercase tracking-wider text-fd-muted-foreground">
								Large data &mdash; scale to the lake
							</h3>
							<div className="space-y-3 rounded-xl border border-fd-primary/50 bg-fd-primary/5 p-5">
								<div className="font-mono text-sm text-fd-muted-foreground">
									Client SQLite → Gateway → <strong className="text-fd-foreground">Iceberg (S3/R2)</strong>
								</div>
								<ul className="space-y-1 text-sm text-fd-muted-foreground">
									<li>Infinite scale on object storage</li>
									<li>Operational + analytics in one place</li>
									<li>Query with Spark, DuckDB, Athena, Trino</li>
									<li>Zero ETL &mdash; no replication pipeline</li>
								</ul>
							</div>
						</div>
					</div>
				</div>
			</section>

			{/* Small data diagram */}
			<DiagramSection
				label="Small data"
				title="Sync to Postgres, MySQL, or any database"
				description="For apps with manageable data volumes, flush to a traditional database. Standard SQL, familiar tooling, easy to operate. The gateway adapter abstracts the storage — swap backends without touching client code."
				chart={SMALL_DATA}
				border
			/>

			{/* Large data diagram */}
			<DiagramSection
				label="Large data"
				title="Sync to the lakehouse. Zero ETL."
				description="When your data outgrows a single database, flush to Apache Iceberg on S3 or Cloudflare R2. Parquet files, open table format, queryable by any analytics engine. Your operational data and your analytics data are the same thing."
				chart={LARGE_DATA}
			/>

			{/* Conflict resolution */}
			<DiagramSection
				label="Conflict resolution"
				title="Column-level merge, not row-level overwrite"
				description="Concurrent edits to different fields of the same record are both preserved. Hybrid logical clocks provide causal ordering with deterministic client ID tiebreaking."
				chart={CONFLICT_RESOLUTION}
				border
			/>

			{/* Offline */}
			<DiagramSection
				label="Offline-first"
				title="Works without a connection, catches up when it returns"
				description="The full dataset lives in local SQLite. Edits queue in a persistent IndexedDB outbox that survives page refreshes and browser crashes. When connectivity returns, the outbox drains automatically."
				chart={OFFLINE_SYNC}
			/>

			{/* Sync rules */}
			<DiagramSection
				label="Sync rules"
				title="Each client gets only the data they need"
				description="Declarative bucket-based filtering with JWT claim references. The gateway evaluates rules at pull time — clients never download data they shouldn't see."
				chart={SYNC_RULES}
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
							title="Pluggable adapters"
							description="The gateway flushes through a LakeAdapter interface — put, get, list, delete. Implement it for S3, R2, Postgres, MySQL, or anything else."
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
							title="Cloudflare DO gateway"
							description="Lightweight gateway on Durable Objects. Push/pull protocol, JWT auth, sync rules, batch flush to any adapter."
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
						gateway, compaction, checkpoint generation, sync rules, and initial sync
						are all implemented and tested. The S3/Iceberg adapter is production-ready;
						database adapters are coming next. API is not yet stable &mdash; expect
						breaking changes.
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
