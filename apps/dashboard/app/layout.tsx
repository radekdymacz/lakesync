import type { Metadata } from "next";
import { AuthProvider } from "@/components/auth-provider";
import { CLERK_ENABLED } from "@/lib/auth-config";
import "./globals.css";

export const metadata: Metadata = {
	title: "LakeSync Dashboard",
	description: "Manage your LakeSync gateways, API keys, and usage",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
	return (
		<AuthProvider enabled={CLERK_ENABLED}>
			<html lang="en">
				<body className="min-h-screen bg-gray-50 text-gray-900 antialiased">{children}</body>
			</html>
		</AuthProvider>
	);
}
