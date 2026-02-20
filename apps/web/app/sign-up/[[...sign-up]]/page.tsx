"use client";

import { SignUp } from "@clerk/nextjs";
import { CLERK_ENABLED } from "@/lib/auth-config";

export default function SignUpPage() {
	if (!CLERK_ENABLED) {
		return (
			<div className="flex min-h-screen items-center justify-center px-4">
				<div className="text-center text-sm text-gray-500">
					<p>Authentication is not configured.</p>
					<p className="mt-1">
						Set{" "}
						<code className="rounded bg-gray-100 px-1 py-0.5 text-xs">
							NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY
						</code>{" "}
						to enable sign-up.
					</p>
				</div>
			</div>
		);
	}

	return (
		<div className="flex min-h-screen items-center justify-center bg-gray-50">
			<SignUp />
		</div>
	);
}
