import "./global.css";
import { RootProvider } from "fumadocs-ui/provider";
import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
	title: {
		template: "%s | LakeSync",
		default: "LakeSync — Local-first sync for the modern web",
	},
	description:
		"Open-source local-first sync engine with column-level conflict resolution, offline support, and TypeScript-native APIs.",
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
