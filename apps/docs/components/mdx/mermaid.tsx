"use client";

import { useEffect, useId, useRef, useState } from "react";
import { useTheme } from "next-themes";

export function Mermaid({ chart }: { chart: string }) {
	const containerRef = useRef<HTMLDivElement>(null);
	const id = useId().replace(/:/g, "-");
	const { resolvedTheme } = useTheme();
	const [svg, setSvg] = useState("");

	useEffect(() => {
		let cancelled = false;
		const isDark = resolvedTheme === "dark";

		import("mermaid").then(({ default: mermaid }) => {
			if (cancelled) return;

			mermaid.initialize({
				startOnLoad: false,
				securityLevel: "loose",
				fontFamily: "inherit",
				theme: isDark ? "dark" : "default",
				themeVariables: isDark
					? {
							background: "transparent",
							mainBkg: "transparent",
							nodeBorder: "#555",
							lineColor: "#888",
							textColor: "#e0e0e0",
							actorBkg: "transparent",
							actorBorder: "#555",
							actorTextColor: "#e0e0e0",
							activationBkgColor: "#333",
							signalColor: "#e0e0e0",
							signalTextColor: "#e0e0e0",
							noteBkgColor: "#2a2a3a",
							noteTextColor: "#e0e0e0",
							noteBorderColor: "#555",
						}
					: {
							background: "transparent",
							mainBkg: "transparent",
						},
			});

			mermaid
				.render(`mermaid-${id}`, chart.replaceAll("\\n", "\n"))
				.then(({ svg: renderedSvg, bindFunctions }) => {
					if (cancelled) return;
					setSvg(renderedSvg);
					if (containerRef.current) bindFunctions?.(containerRef.current);
				});
		});

		return () => {
			cancelled = true;
		};
	}, [chart, resolvedTheme, id]);

	if (!svg) return null;

	return (
		<div
			ref={containerRef}
			className="flex w-full items-center justify-center [&_svg]:max-w-full"
			dangerouslySetInnerHTML={{ __html: svg }}
		/>
	);
}
