import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";

export const baseOptions: BaseLayoutProps = {
	nav: {
		title: "LakeSync",
	},
	links: [
		{
			text: "Documentation",
			url: "/docs",
			active: "nested-url",
		},
		{
			text: "GitHub",
			url: "https://github.com/radekdymacz/lakesync",
			external: true,
		},
	],
};
