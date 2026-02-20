import { RootProvider } from "fumadocs-ui/provider";
import type { Metadata } from "next";
import { AuthProvider } from "@/components/auth-provider";
import { CLERK_ENABLED } from "@/lib/auth-config";
import "./globals.css";

export const metadata: Metadata = {
	title: {
		template: "%s | LakeSync",
		default: "LakeSync — Declare what data goes where",
	},
	description:
		"Declare what data goes where. The engine handles the rest. Sync, backup, migrate, and analyse data across SQL, SaaS, files, and AI — all from one platform.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
	return (
		<AuthProvider enabled={CLERK_ENABLED}>
			<html lang="en" suppressHydrationWarning>
				<body className="min-h-screen antialiased">
					<RootProvider>{children}</RootProvider>
				</body>
			</html>
		</AuthProvider>
	);
}
