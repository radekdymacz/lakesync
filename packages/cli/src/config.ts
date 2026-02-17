import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

/** CLI configuration stored at ~/.lakesync/config.json */
export interface CliConfig {
	/** Base URL of the LakeSync gateway */
	gatewayUrl?: string;
	/** Default gateway ID */
	gatewayId?: string;
	/** Bearer token for authentication */
	token?: string;
	/** Default org ID (from control plane) */
	orgId?: string;
}

const CONFIG_DIR = join(homedir(), ".lakesync");
const CONFIG_FILE = join(CONFIG_DIR, "config.json");

/** Load the CLI configuration file. Returns empty config if not found. */
export function loadConfig(): CliConfig {
	if (!existsSync(CONFIG_FILE)) return {};
	try {
		const raw = readFileSync(CONFIG_FILE, "utf-8");
		return JSON.parse(raw) as CliConfig;
	} catch {
		return {};
	}
}

/** Save the CLI configuration file. Creates ~/.lakesync/ if it does not exist. */
export function saveConfig(config: CliConfig): void {
	if (!existsSync(CONFIG_DIR)) {
		mkdirSync(CONFIG_DIR, { recursive: true });
	}
	writeFileSync(
		CONFIG_FILE,
		`${JSON.stringify(config, null, "\t")}\n`,
		"utf-8",
	);
}

/** Get the config directory path. */
export function getConfigDir(): string {
	return CONFIG_DIR;
}

/** Get the config file path. */
export function getConfigFile(): string {
	return CONFIG_FILE;
}
