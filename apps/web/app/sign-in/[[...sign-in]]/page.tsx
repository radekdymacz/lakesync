"use client";

import { SignIn } from "@clerk/nextjs";
import { useRouter } from "next/navigation";
import { CLERK_ENABLED } from "@/lib/auth-config";

function DevSignIn() {
	const router = useRouter();

	return (
		<div className="w-full max-w-sm rounded-lg border border-gray-200 bg-white p-8 shadow-sm">
			<h1 className="text-center text-xl font-semibold">Sign in</h1>
			<p className="mt-2 text-center text-sm text-gray-500">
				Clerk is not configured. Running in dev mode.
			</p>
			<button
				type="button"
				onClick={() => router.push("/dashboard")}
				className="mt-6 w-full rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800"
			>
				Continue as dev user
			</button>
		</div>
	);
}

export default function SignInPage() {
	return (
		<div className="flex min-h-screen items-center justify-center bg-gray-50">
			{CLERK_ENABLED ? <SignIn /> : <DevSignIn />}
		</div>
	);
}
