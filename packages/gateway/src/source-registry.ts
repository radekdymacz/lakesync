import type { DatabaseAdapter } from "@lakesync/core";

/**
 * Registry for named source adapters used in adapter-sourced pulls.
 *
 * Manages the mapping of source names to DatabaseAdapter instances,
 * decoupling source adapter management from the gateway.
 */
export class SourceRegistry {
	private sources: Map<string, DatabaseAdapter> = new Map();

	constructor(initial?: Record<string, DatabaseAdapter>) {
		if (initial) {
			for (const [name, adapter] of Object.entries(initial)) {
				this.sources.set(name, adapter);
			}
		}
	}

	/** Register a named source adapter. */
	register(name: string, adapter: DatabaseAdapter): void {
		this.sources.set(name, adapter);
	}

	/** Unregister a named source adapter. */
	unregister(name: string): void {
		this.sources.delete(name);
	}

	/** Get a source adapter by name, or undefined if not registered. */
	get(name: string): DatabaseAdapter | undefined {
		return this.sources.get(name);
	}

	/** List all registered source adapter names. */
	list(): string[] {
		return [...this.sources.keys()];
	}
}
