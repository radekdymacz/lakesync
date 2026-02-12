import Link from "next/link";
import { Mermaid } from "@/components/mdx/mermaid";

const CORE_FLOW = `sequenceDiagram
    participant Consumer as Consumer (SQLite)
    participant GW as Gateway
    participant Source as Any Data Source

    Consumer->>GW: push local deltas (HTTP or WS)
    GW-->>Consumer: ACK + server HLC
    Consumer->>GW: pull (sync rule filter)
    GW->>Source: query via adapter
    Source-->>GW: filtered results
    GW-->>Consumer: deltas matching rule
    GW-->>Consumer: broadcast new deltas (WS)
    Note over GW,Source: Adapters are both<br/>sources and destinations`;

const OFFLINE_SYNC = `sequenceDiagram
    participant App as Consumer (offline)
    participant DB as Local SQLite
    participant Q as Outbox (IndexedDB)
    participant GW as Gateway

    App->>DB: Edit 1, Edit 2, Edit 3
    DB-->>Q: Deltas queued
    Note over App,Q: Fully functional offline
    Note over Q,GW: ← Connection restored →
    Q->>GW: Push all deltas
    GW-->>Q: ACK
    GW->>App: Pull remote changes
    Note over App,GW: Caught up ✓`;

const CROSS_BACKEND = `sequenceDiagram
    participant PG as Postgres
    participant GW as Gateway
    participant BQ as BigQuery
    participant ICE as Iceberg (S3/R2)

    Note over GW: Sync rules define flows
    PG->>GW: read via DatabaseAdapter
    GW->>BQ: write via DatabaseAdapter
    PG->>GW: read via DatabaseAdapter
    GW->>ICE: write via LakeAdapter
    Note over GW: Any source → any destination<br/>via adapter interfaces`;

const SOURCE_POLLING = `sequenceDiagram
    participant SaaS as External API (Jira, Salesforce)
    participant Poller as SourcePoller
    participant GW as Gateway

    loop Every interval
        Poller->>SaaS: poll for changes (cursor)
        SaaS-->>Poller: new/updated records
        Poller->>Poller: extract deltas (diff)
        Poller->>GW: push deltas (chunked)
        Note over Poller,GW: Memory-bounded<br/>streaming accumulation
    end
    GW-->>GW: buffer + flush to adapter`;

const CONFLICT_RESOLUTION = `sequenceDiagram
    participant A as Client A
    participant GW as Gateway
    participant B as Client B

    A->>A: UPDATE title = "Draft"
    B->>B: UPDATE status = "done"
    A->>GW: push delta (title, HLC=100)
    B->>GW: push delta (status, HLC=101)
    Note over GW: Column-level merge<br/>title ← A (HLC 100)<br/>status ← B (HLC 101)
    GW->>A: pull → status = "done"
    GW->>B: pull → title = "Draft"
    Note over A,B: Both changes preserved ✓`;

const SYNC_RULES = `sequenceDiagram
    participant A as Consumer A
    participant GW as Gateway
    participant B as Consumer B

    Note over GW: Sync rules:<br/>bucket-based filtering
    A->>GW: pull (JWT: team=eng)
    GW-->>A: deltas where team=eng
    B->>GW: pull (JWT: role=ops)
    GW-->>B: deltas where role=ops
    Note over A,B: Each consumer syncs<br/>only matching data`;

export default function HomePage() {
	return (
		<main className="flex flex-1 flex-col items-center">
			{/* Hero */}
			<section className="flex w-full flex-col items-center px-4 pb-24 pt-16">
				<div className="mb-6 rounded-full border border-fd-border bg-fd-accent/50 px-4 py-1.5 text-xs font-medium text-fd-muted-foreground">
					Experimental &mdash; under active development
				</div>

				<h1 className="mb-6 max-w-4xl text-center text-5xl font-bold tracking-tight sm:text-6xl lg:text-7xl">
					Declare what data goes where.{" "}
					<span className="text-fd-muted-foreground">
						The engine handles the rest.
					</span>
				</h1>

				<p className="mx-auto mb-10 max-w-2xl text-center text-lg leading-relaxed text-fd-muted-foreground">
					LakeSync is an open-source TypeScript sync engine. Pluggable
					adapters connect any readable or writable system. Declarative
					sync rules define what data flows between them. Every adapter
					is both a source and a destination &mdash; local SQLite,
					Postgres, MySQL, BigQuery, or S3/Iceberg.
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
				title="Consumer ↔ Gateway ↔ Any Data Source"
				description="Consumers push local deltas and pull filtered subsets from remote sources via the gateway. Adapters abstract the data source. Sync rules control what data each consumer receives."
				chart={CORE_FLOW}
				border
			/>

			{/* Adapters */}
			<section className="w-full px-4 py-24">
				<div className="mx-auto max-w-5xl">
					<div className="mb-3 text-center text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
						Pluggable adapters
					</div>
					<h2 className="mb-6 text-center text-3xl font-bold">
						Any source, any destination
					</h2>
					<p className="mx-auto mb-16 max-w-2xl text-center leading-relaxed text-fd-muted-foreground">
						Two interfaces: <code className="rounded bg-fd-accent/50 px-1.5 py-0.5 text-xs">DatabaseAdapter</code> for
						SQL-like systems and <code className="rounded bg-fd-accent/50 px-1.5 py-0.5 text-xs">LakeAdapter</code> for
						object storage. All three database adapters materialise deltas into
						queryable destination tables. Source connectors poll external APIs
						(Jira, Salesforce) and push changes as deltas.
					</p>

					<div className="mx-auto grid max-w-5xl grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
						<AdapterCard
							name="Postgres / MySQL"
							description="DatabaseAdapter. insertDeltas, queryDeltasSince, getLatestState, ensureSchema."
						/>
						<AdapterCard
							name="BigQuery"
							description="DatabaseAdapter. Idempotent MERGE inserts. INT64 HLC precision. Clustered by table + hlc."
						/>
						<AdapterCard
							name="S3 / R2 (Iceberg)"
							description="LakeAdapter. putObject, getObject, listObjects, deleteObject. Parquet + Iceberg table format."
						/>
						<AdapterCard
							name="Jira"
							description="Source connector. Polls issues, comments, and projects via Jira Cloud API with cursor-based change detection."
						/>
						<AdapterCard
							name="Salesforce"
							description="Source connector. Polls accounts, contacts, opportunities, and leads via SOQL with LastModifiedDate cursors."
						/>
						<AdapterCard
							name="Custom adapters"
							description="Implement either interface for any data source. CompositeAdapter, FanOutAdapter, and LifecycleAdapter for advanced routing."
							highlight
						/>
					</div>
				</div>
			</section>

			{/* Sync rules */}
			<DiagramSection
				label="Sync rules"
				title="Declarative filtering via sync rules DSL"
				description="Bucket-based filtering with eq, neq, in, gt, lt, gte, lte operators and JWT claim references via jwt: prefix. The gateway evaluates rules at pull time and returns only matching deltas. Sync rules define what data each consumer receives."
				chart={SYNC_RULES}
				border
			/>

			{/* Cross-backend flows */}
			<DiagramSection
				label="Cross-backend"
				title="Route data between any two adapters"
				description="Adapters are bidirectional — they implement both read and write. Sync rules define directional flows between adapters. The gateway reads from one adapter and writes to another."
				chart={CROSS_BACKEND}
			/>

			{/* Offline */}
			<DiagramSection
				label="Offline support"
				title="Persistent outbox with automatic drain"
				description="Deltas queue in an IndexedDB outbox that survives page refreshes and process crashes. When connectivity returns, the outbox drains automatically and remote changes sync down."
				chart={OFFLINE_SYNC}
				border
			/>

			{/* Conflict resolution */}
			<DiagramSection
				label="Conflict resolution"
				title="Column-level LWW with HLC ordering"
				description="Concurrent edits to different columns of the same row are both preserved. Hybrid logical clocks (48-bit wall + 16-bit counter) provide causal ordering. Equal timestamps use deterministic clientId tiebreaking."
				chart={CONFLICT_RESOLUTION}
			/>

			{/* Source polling */}
			<DiagramSection
				label="Source connectors"
				title="Poll external APIs and ingest as deltas"
				description="Source connectors extend BaseSourcePoller to poll external APIs on an interval and push changes as deltas. Memory-bounded streaming accumulation keeps resource usage predictable. Built-in connectors for Jira and Salesforce. Extend the base class for any API."
				chart={SOURCE_POLLING}
				border
			/>

			{/* Design decisions */}
			<section className="w-full border-t border-fd-border px-4 py-24">
				<div className="mx-auto max-w-5xl">
					<div className="mb-3 text-center text-xs font-medium uppercase tracking-wider text-fd-muted-foreground">
						Under the hood
					</div>
					<h2 className="mb-12 text-center text-3xl font-bold">
						Design decisions
					</h2>
					<div className="grid grid-cols-1 gap-6 md:grid-cols-3">
						<Feature
							title="Adapter interfaces"
							description="DatabaseAdapter for SQL-like sources (insertDeltas, queryDeltasSince). LakeAdapter for object storage (putObject, getObject). Both are bidirectional."
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
							title="Real-time WebSocket sync"
							description="WebSocketTransport uses binary protobuf framing for push, pull, and server-initiated broadcasts. Auto-reconnects with exponential backoff."
						/>
						<Feature
							title="Sync rules DSL"
							description="Declarative bucket-based filtering with eq, neq, in, gt, lt, gte, lte operators and JWT claim references. Pure function evaluation — filterDeltas() has no side effects."
						/>
						<Feature
							title="Deterministic delta IDs"
							description="SHA-256 of stable-stringified payload. Same logical change always produces the same deltaId. Enables idempotent processing."
						/>
						<Feature
							title="React bindings"
							description="LakeSyncProvider, useQuery, useMutation, and useSyncStatus hooks. Reactive local-first data access for React apps."
						/>
						<Feature
							title="Source polling"
							description="BaseSourcePoller base class with memory-bounded streaming accumulation. Built-in connectors for Jira and Salesforce. Extend for any API."
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
						14 packages, 3 apps. Core sync engine, conflict resolution, client SDK,
						React bindings, Cloudflare Workers gateway, self-hosted gateway server
						with WebSocket support, compaction, checkpoint generation, sync rules
						DSL, cross-backend flows, source connectors (Jira, Salesforce), and
						adapters for Postgres, MySQL, BigQuery, and S3/R2 are all implemented
						and tested. API is not yet stable &mdash; expect breaking changes.
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
