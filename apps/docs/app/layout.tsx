import "./global.css";
import { RootProvider } from "fumadocs-ui/provider";
import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
	title: {
		template: "%s | LakeSync",
		default: "LakeSync — Declare what data goes where",
	},
	description:
		"Open-source TypeScript sync engine. Pluggable adapters connect any source to any destination. Sync rules, materialisation, offline support, and column-level conflict resolution.",
};

export default function RootLayout({ children }: { children: ReactNode }) {
	return (
		<html lang="en" suppressHydrationWarning>
			<body>
				<RootProvider>{children}</RootProvider>
			</body>
		</html>
	);
}
