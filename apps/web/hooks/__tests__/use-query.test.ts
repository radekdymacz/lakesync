import { describe, expect, it } from "vitest";
import { useApiQuery } from "../use-query";

// ---------------------------------------------------------------------------
// useApiQuery is a React hook — full render tests require @testing-library/react
// which is not installed. Instead we verify the module exports correctly and
// defer to integration tests for runtime behaviour.
// ---------------------------------------------------------------------------

describe("useApiQuery — module exports", () => {
	it("exports useApiQuery as a function", () => {
		expect(typeof useApiQuery).toBe("function");
	});
});
