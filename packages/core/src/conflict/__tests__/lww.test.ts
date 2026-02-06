import { describe, it, expect } from 'vitest';
import { resolveLWW, LWWResolver } from '../lww';
import { HLC } from '../../hlc/hlc';
import type { RowDelta, DeltaOp } from '../../delta/types';
import type { HLCTimestamp } from '../../hlc/types';
import { ConflictError } from '../../result/errors';

/** Helper to build a RowDelta with sensible defaults */
function makeDelta(
	overrides: Partial<RowDelta> & { hlc: HLCTimestamp },
): RowDelta {
	return {
		op: 'UPDATE' as DeltaOp,
		table: 'todos',
		rowId: 'row-1',
		clientId: 'client-a',
		columns: [],
		deltaId: `delta-${Math.random().toString(36).slice(2)}`,
		...overrides,
	};
}

describe('LWWResolver', () => {
	const hlcLow = HLC.encode(1_000_000, 0);
	const hlcMid = HLC.encode(2_000_000, 0);
	const hlcHigh = HLC.encode(3_000_000, 0);

	it('merges non-overlapping columns as a union of both column sets', () => {
		const local = makeDelta({
			hlc: hlcMid,
			clientId: 'client-a',
			columns: [{ column: 'title', value: 'Local Title' }],
		});
		const remote = makeDelta({
			hlc: hlcMid,
			clientId: 'client-b',
			columns: [{ column: 'done', value: true }],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			const colNames = result.value.columns.map((c) => c.column).sort();
			expect(colNames).toEqual(['done', 'title']);
			// Both columns should keep their original values
			const titleCol = result.value.columns.find(
				(c) => c.column === 'title',
			);
			const doneCol = result.value.columns.find(
				(c) => c.column === 'done',
			);
			expect(titleCol?.value).toBe('Local Title');
			expect(doneCol?.value).toBe(true);
		}
	});

	it('picks remote value when remote HLC is higher for overlapping column', () => {
		const local = makeDelta({
			hlc: hlcLow,
			clientId: 'client-a',
			columns: [{ column: 'title', value: 'Old Title' }],
		});
		const remote = makeDelta({
			hlc: hlcHigh,
			clientId: 'client-b',
			columns: [{ column: 'title', value: 'New Title' }],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.columns).toHaveLength(1);
			const col = result.value.columns[0];
			expect(col).toBeDefined();
			expect(col!.value).toBe('New Title');
		}
	});

	it('picks local value when local HLC is higher for overlapping column', () => {
		const local = makeDelta({
			hlc: hlcHigh,
			clientId: 'client-a',
			columns: [{ column: 'title', value: 'Latest Title' }],
		});
		const remote = makeDelta({
			hlc: hlcLow,
			clientId: 'client-b',
			columns: [{ column: 'title', value: 'Stale Title' }],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.columns).toHaveLength(1);
			const col = result.value.columns[0];
			expect(col).toBeDefined();
			expect(col!.value).toBe('Latest Title');
		}
	});

	it('uses lexicographic clientId tiebreak when HLCs are equal', () => {
		const sameHlc = HLC.encode(1_500_000, 5);

		const local = makeDelta({
			hlc: sameHlc,
			clientId: 'client-a',
			columns: [{ column: 'title', value: 'A value' }],
		});
		const remote = makeDelta({
			hlc: sameHlc,
			clientId: 'client-b',
			columns: [{ column: 'title', value: 'B value' }],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// 'client-b' > 'client-a' lexicographically, so remote wins
			const col = result.value.columns[0];
			expect(col).toBeDefined();
			expect(col!.value).toBe('B value');
			expect(result.value.clientId).toBe('client-b');
		}
	});

	it('merges columns correctly for INSERT vs UPDATE', () => {
		const local = makeDelta({
			op: 'INSERT',
			hlc: hlcLow,
			clientId: 'client-a',
			columns: [
				{ column: 'title', value: 'Inserted' },
				{ column: 'priority', value: 1 },
			],
		});
		const remote = makeDelta({
			op: 'UPDATE',
			hlc: hlcHigh,
			clientId: 'client-b',
			columns: [
				{ column: 'title', value: 'Updated' },
				{ column: 'done', value: false },
			],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			// INSERT vs UPDATE => should produce UPDATE
			expect(result.value.op).toBe('UPDATE');
			// title: remote HLC is higher, so remote wins
			const titleCol = result.value.columns.find(
				(c) => c.column === 'title',
			);
			expect(titleCol?.value).toBe('Updated');
			// priority: only in local, always included
			const priorityCol = result.value.columns.find(
				(c) => c.column === 'priority',
			);
			expect(priorityCol?.value).toBe(1);
			// done: only in remote, always included
			const doneCol = result.value.columns.find(
				(c) => c.column === 'done',
			);
			expect(doneCol?.value).toBe(false);
		}
	});

	it('DELETE wins over UPDATE when DELETE has higher HLC (tombstone)', () => {
		const local = makeDelta({
			op: 'DELETE',
			hlc: hlcHigh,
			clientId: 'client-a',
			columns: [],
		});
		const remote = makeDelta({
			op: 'UPDATE',
			hlc: hlcLow,
			clientId: 'client-b',
			columns: [{ column: 'title', value: 'Should be tombstoned' }],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.op).toBe('DELETE');
			expect(result.value.columns).toEqual([]);
		}
	});

	it('UPDATE wins over DELETE when UPDATE has higher HLC (resurrection)', () => {
		const local = makeDelta({
			op: 'DELETE',
			hlc: hlcLow,
			clientId: 'client-a',
			columns: [],
		});
		const remote = makeDelta({
			op: 'UPDATE',
			hlc: hlcHigh,
			clientId: 'client-b',
			columns: [{ column: 'title', value: 'Resurrected' }],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.op).toBe('UPDATE');
			expect(result.value.columns).toHaveLength(1);
			const col = result.value.columns[0];
			expect(col).toBeDefined();
			expect(col!.value).toBe('Resurrected');
		}
	});

	it('UPDATE older than DELETE tombstone loses (tombstone wins)', () => {
		const local = makeDelta({
			op: 'UPDATE',
			hlc: hlcLow,
			clientId: 'client-a',
			columns: [{ column: 'title', value: 'Old update' }],
		});
		const remote = makeDelta({
			op: 'DELETE',
			hlc: hlcHigh,
			clientId: 'client-b',
			columns: [],
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.op).toBe('DELETE');
			expect(result.value.columns).toEqual([]);
			expect(result.value.hlc).toBe(hlcHigh);
		}
	});

	it('DELETE vs DELETE resolves to the winner by HLC/tiebreak', () => {
		const local = makeDelta({
			op: 'DELETE',
			hlc: hlcLow,
			clientId: 'client-a',
			columns: [],
			deltaId: 'delta-local',
		});
		const remote = makeDelta({
			op: 'DELETE',
			hlc: hlcHigh,
			clientId: 'client-b',
			columns: [],
			deltaId: 'delta-remote',
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.value.op).toBe('DELETE');
			expect(result.value.columns).toEqual([]);
			// Remote has higher HLC, so it should win
			expect(result.value.hlc).toBe(hlcHigh);
			expect(result.value.deltaId).toBe('delta-remote');
		}
	});

	it('returns Err(ConflictError) for mismatched table/rowId', () => {
		const local = makeDelta({
			hlc: hlcMid,
			table: 'todos',
			rowId: 'row-1',
		});
		const remote = makeDelta({
			hlc: hlcMid,
			table: 'users',
			rowId: 'row-2',
		});

		const result = resolveLWW(local, remote);

		expect(result.ok).toBe(false);
		if (!result.ok) {
			expect(result.error).toBeInstanceOf(ConflictError);
			expect(result.error.code).toBe('CONFLICT');
			expect(result.error.message).toContain('mismatched table/rowId');
		}
	});
});
