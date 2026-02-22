import { ClockDriftError, Err, Ok, type Result } from "../result";
import type { HLCTimestamp } from "./types";

/**
 * Hybrid Logical Clock implementation.
 *
 * 64-bit layout: [48-bit wall clock ms][16-bit logical counter].
 * Maximum allowed clock drift: 5 seconds.
 *
 * The wall clock source is injectable for deterministic testing.
 */
/** Immutable HLC state snapshot. */
interface HLCSnapshot {
	readonly wall: number;
	readonly counter: number;
}

export class HLC {
	private readonly wallClock: () => number;
	private state: HLCSnapshot = { wall: 0, counter: 0 };

	/** Maximum tolerated drift between local and remote physical clocks (ms). */
	static readonly MAX_DRIFT_MS = 5_000;

	/** Maximum value of the 16-bit logical counter. */
	static readonly MAX_COUNTER = 0xffff;

	/**
	 * Create a new HLC instance.
	 *
	 * @param wallClock - Optional injectable clock source returning epoch ms.
	 *                    Defaults to `Date.now`.
	 */
	constructor(wallClock?: () => number) {
		this.wallClock = wallClock ?? (() => Date.now());
	}

	/**
	 * Generate a new monotonically increasing HLC timestamp.
	 *
	 * The returned timestamp is guaranteed to be strictly greater than any
	 * previously returned by this instance.
	 */
	now(): HLCTimestamp {
		const physical = this.wallClock();
		const prev = this.state;
		const wall = Math.max(physical, prev.wall);

		let nextWall: number;
		let nextCounter: number;

		if (wall === prev.wall) {
			nextCounter = prev.counter + 1;
			if (nextCounter > HLC.MAX_COUNTER) {
				// Counter overflow: advance wall by 1 ms and reset counter
				nextWall = wall + 1;
				nextCounter = 0;
			} else {
				nextWall = wall;
			}
		} else {
			nextWall = wall;
			nextCounter = 0;
		}

		this.state = { wall: nextWall, counter: nextCounter };
		return HLC.encode(nextWall, nextCounter);
	}

	/**
	 * Receive a remote HLC timestamp and advance the local clock.
	 *
	 * Returns `Err(ClockDriftError)` if the remote timestamp indicates
	 * clock drift exceeding {@link MAX_DRIFT_MS}.
	 *
	 * @param remote - The HLC timestamp received from a remote node.
	 * @returns A `Result` containing the new local HLC timestamp, or a
	 *          `ClockDriftError` if the remote clock is too far ahead.
	 */
	recv(remote: HLCTimestamp): Result<HLCTimestamp, ClockDriftError> {
		const { wall: remoteWall, counter: remoteCounter } = HLC.decode(remote);
		const physical = this.wallClock();
		const prev = this.state;
		const localWall = Math.max(physical, prev.wall);

		// Check drift: compare remote wall against physical clock
		if (remoteWall - physical > HLC.MAX_DRIFT_MS) {
			return Err(
				new ClockDriftError(
					`Remote clock is ${remoteWall - physical}ms ahead (max drift: ${HLC.MAX_DRIFT_MS}ms)`,
				),
			);
		}

		let nextWall: number;
		let nextCounter: number;

		if (remoteWall > localWall) {
			nextWall = remoteWall;
			nextCounter = remoteCounter + 1;
		} else if (remoteWall === localWall) {
			nextWall = localWall;
			nextCounter = Math.max(prev.counter, remoteCounter) + 1;
		} else {
			nextWall = localWall;
			nextCounter = prev.counter + 1;
		}

		if (nextCounter > HLC.MAX_COUNTER) {
			// Counter overflow: advance wall by 1 ms and reset counter
			nextWall = nextWall + 1;
			nextCounter = 0;
		}

		this.state = { wall: nextWall, counter: nextCounter };
		return Ok(HLC.encode(nextWall, nextCounter));
	}

	/**
	 * Encode a wall clock value (ms) and logical counter into a 64-bit HLC timestamp.
	 *
	 * @param wall    - Wall clock component in epoch milliseconds (48-bit).
	 * @param counter - Logical counter component (16-bit, 0..65535).
	 * @returns The encoded {@link HLCTimestamp}.
	 */
	static encode(wall: number, counter: number): HLCTimestamp {
		return ((BigInt(wall) << 16n) | BigInt(counter & 0xffff)) as HLCTimestamp;
	}

	/**
	 * Decode an HLC timestamp into its wall clock (ms) and logical counter components.
	 *
	 * @param ts - The {@link HLCTimestamp} to decode.
	 * @returns An object with `wall` (epoch ms) and `counter` (logical) fields.
	 */
	static decode(ts: HLCTimestamp): { wall: number; counter: number } {
		return {
			wall: Number(ts >> 16n),
			counter: Number(ts & 0xffffn),
		};
	}

	/**
	 * Compare two HLC timestamps.
	 *
	 * @returns `-1` if `a < b`, `0` if `a === b`, `1` if `a > b`.
	 */
	static compare(a: HLCTimestamp, b: HLCTimestamp): -1 | 0 | 1 {
		if (a < b) return -1;
		if (a > b) return 1;
		return 0;
	}
}
