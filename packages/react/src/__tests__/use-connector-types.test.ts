import { act, renderHook, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { describe, expect, it, vi } from "vitest";
import type { LakeSyncProviderProps } from "../context";
import { LakeSyncProvider } from "../context";
import { useConnectorTypes } from "../use-connector-types";

// ────────────────────────────────────────────────────────────
// Mock coordinator
// ────────────────────────────────────────────────────────────

interface Listeners {
	onChange: Array<(...args: unknown[]) => void>;
	onSyncComplete: Array<(...args: unknown[]) => void>;
	onError: Array<(...args: unknown[]) => void>;
}

function mockCoordinator(
	listResult:
		| {
				ok: true;
				value: Array<{
					type: string;
					displayName: string;
					description: string;
					category: string;
					configSchema: Record<string, unknown>;
					ingestSchema: Record<string, unknown>;
					outputTables: null;
				}>;
		  }
		| { ok: false; error: { message: string; code: string } },
) {
	const listeners: Listeners = {
		onChange: [],
		onSyncComplete: [],
		onError: [],
	};

	return {
		tracker: {
			insert: vi.fn(),
			update: vi.fn(),
			delete: vi.fn(),
			query: vi.fn(),
		},
		on: vi.fn((event: string, cb: (...args: unknown[]) => void) => {
			if (event in listeners) {
				listeners[event as keyof Listeners].push(cb);
			}
		}),
		off: vi.fn(),
		queueDepth: vi.fn().mockResolvedValue(0),
		lastSyncTime: null,
		listConnectorTypes: vi.fn().mockResolvedValue(listResult),
		_listeners: listeners,
	};
}

type MockCoordinator = ReturnType<typeof mockCoordinator>;

function wrapper(coordinator: MockCoordinator) {
	return function Wrapper({ children }: { children: React.ReactNode }) {
		return createElement(
			LakeSyncProvider,
			{ coordinator } as unknown as LakeSyncProviderProps,
			children,
		);
	};
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

const postgresDescriptor = {
	type: "postgres" as const,
	displayName: "PostgreSQL",
	description: "PostgreSQL database connector",
	category: "database" as const,
	configSchema: { type: "object" },
	ingestSchema: { type: "object" },
	outputTables: null,
};

const jiraDescriptor = {
	type: "jira" as const,
	displayName: "Jira",
	description: "Jira Cloud connector",
	category: "api" as const,
	configSchema: { type: "object" },
	ingestSchema: { type: "object" },
	outputTables: null,
};

describe("useConnectorTypes", () => {
	it("returns loading state initially, then types array", async () => {
		const types = [postgresDescriptor, jiraDescriptor];
		const coord = mockCoordinator({ ok: true, value: types });

		const { result } = renderHook(() => useConnectorTypes(), {
			wrapper: wrapper(coord),
		});

		// Initially loading
		expect(result.current.isLoading).toBe(true);
		expect(result.current.types).toEqual([]);

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(result.current.types).toEqual(types);
		expect(result.current.error).toBeNull();
	});

	it("returns error when coordinator returns an error result", async () => {
		const err = { message: "FETCH_FAILED", code: "TRANSPORT_ERROR" };
		const coord = mockCoordinator({ ok: false, error: err });

		const { result } = renderHook(() => useConnectorTypes(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(result.current.error).toBe(err);
		expect(result.current.types).toEqual([]);
	});

	it("refetch triggers a new fetch", async () => {
		const types = [postgresDescriptor];
		const coord = mockCoordinator({ ok: true, value: types });

		const { result } = renderHook(() => useConnectorTypes(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(coord.listConnectorTypes).toHaveBeenCalledTimes(1);

		// Update mock to return additional connector
		const updatedTypes = [postgresDescriptor, jiraDescriptor];
		coord.listConnectorTypes.mockResolvedValue({ ok: true, value: updatedTypes });

		act(() => {
			result.current.refetch();
		});

		await waitFor(() => {
			expect(result.current.types).toEqual(updatedTypes);
		});

		expect(coord.listConnectorTypes).toHaveBeenCalledTimes(2);
	});

	it("returns empty types array before fetch completes", async () => {
		const coord = mockCoordinator({ ok: true, value: [postgresDescriptor] });

		const { result } = renderHook(() => useConnectorTypes(), {
			wrapper: wrapper(coord),
		});

		// Before resolution, types should be empty
		expect(result.current.types).toEqual([]);
		expect(result.current.isLoading).toBe(true);

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});
	});

	it("clears previous error on successful refetch", async () => {
		const err = { message: "NETWORK_ERROR", code: "TRANSPORT_ERROR" };
		const coord = mockCoordinator({ ok: false, error: err });

		const { result } = renderHook(() => useConnectorTypes(), {
			wrapper: wrapper(coord),
		});

		await waitFor(() => {
			expect(result.current.isLoading).toBe(false);
		});

		expect(result.current.error).toBe(err);

		// Refetch succeeds
		const types = [postgresDescriptor];
		coord.listConnectorTypes.mockResolvedValue({ ok: true, value: types });

		act(() => {
			result.current.refetch();
		});

		await waitFor(() => {
			expect(result.current.error).toBeNull();
		});

		expect(result.current.types).toEqual(types);
	});
});
