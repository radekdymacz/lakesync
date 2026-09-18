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
		"Open-source, offline-first sync for TypeScript apps. Local SQLite on the device, a lightweight gateway, and pluggable backends — Postgres, BigQuery, or Iceberg on S3/R2.",
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
