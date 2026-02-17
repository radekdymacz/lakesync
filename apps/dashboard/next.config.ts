import type { NextConfig } from "next";

const nextConfig: NextConfig = {
	transpilePackages: ["@lakesync/control-plane", "@lakesync/core"],
};

export default nextConfig;
