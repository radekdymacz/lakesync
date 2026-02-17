import { loadConfig, saveConfig } from "../config";
import { print } from "../output";

/**
 * `lakesync logout` — Remove stored token from ~/.lakesync/config.json.
 */
export function logout(): void {
	const config = loadConfig();
	delete config.token;
	saveConfig(config);
	print("Logged out (token removed from config)");
}
