"use client";

import { useCallback, useState } from "react";

interface ConfirmDialogProps {
	readonly open: boolean;
	readonly title: string;
	readonly message: string;
	readonly confirmLabel: string;
	readonly variant?: "danger" | "default";
	readonly onClose: () => void;
	readonly onConfirm: () => Promise<void>;
}

export function ConfirmDialog({
	open,
	title,
	message,
	confirmLabel,
	variant = "default",
	onClose,
	onConfirm,
}: ConfirmDialogProps) {
	const [submitting, setSubmitting] = useState(false);

	const handleConfirm = useCallback(async () => {
		setSubmitting(true);
		try {
			await onConfirm();
			onClose();
		} finally {
			setSubmitting(false);
		}
	}, [onConfirm, onClose]);

	if (!open) return null;

	const confirmClass =
		variant === "danger"
			? "bg-red-600 hover:bg-red-700 text-white"
			: "bg-gray-900 hover:bg-gray-800 text-white";

	return (
		<div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
			<div className="w-full max-w-sm rounded-lg bg-white p-6 shadow-xl">
				<h2 className="text-lg font-semibold">{title}</h2>
				<p className="mt-2 text-sm text-gray-600">{message}</p>

				<div className="mt-6 flex justify-end gap-3">
					<button
						type="button"
						onClick={onClose}
						disabled={submitting}
						className="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
					>
						Cancel
					</button>
					<button
						type="button"
						onClick={handleConfirm}
						disabled={submitting}
						className={`rounded-md px-4 py-2 text-sm font-medium disabled:opacity-50 ${confirmClass}`}
					>
						{submitting ? "Processing..." : confirmLabel}
					</button>
				</div>
			</div>
		</div>
	);
}
