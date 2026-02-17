/** Parsed command-line arguments. */
export interface ParsedArgs {
	/** The command path (e.g. ["token", "create"]) */
	command: string[];
	/** Named flags (e.g. --secret becomes { secret: "value" }) */
	flags: Record<string, string>;
	/** Positional arguments after the command */
	positional: string[];
}

/**
 * Parse process.argv into structured command, flags, and positional args.
 *
 * Supports:
 * - `--flag value` style options
 * - `--flag=value` style options
 * - Commands and subcommands before flags
 * - Positional arguments mixed with flags
 */
export function parseArgs(argv: string[]): ParsedArgs {
	// Skip node binary and script path
	const args = argv.slice(2);

	const command: string[] = [];
	const flags: Record<string, string> = {};
	const positional: string[] = [];

	let i = 0;

	// Known two-word commands
	const TWO_WORD_COMMANDS = new Set([
		"token create",
		"gateways list",
		"gateways create",
		"gateways delete",
		"keys create",
		"keys list",
		"keys revoke",
	]);

	// Consume the first non-flag word as the command
	if (i < args.length && !args[i]!.startsWith("-")) {
		command.push(args[i]!);
		i++;

		// Check if this could be a two-word command
		if (i < args.length && !args[i]!.startsWith("-")) {
			const twoWord = `${command[0]} ${args[i]}`;
			if (TWO_WORD_COMMANDS.has(twoWord)) {
				command.push(args[i]!);
				i++;
			}
		}
	}

	// Parse remaining as flags and positional args
	while (i < args.length) {
		const arg = args[i]!;

		if (arg.startsWith("--")) {
			const equalIdx = arg.indexOf("=");
			if (equalIdx !== -1) {
				// --flag=value
				const key = arg.slice(2, equalIdx);
				const value = arg.slice(equalIdx + 1);
				flags[key] = value;
			} else {
				// --flag value
				const key = arg.slice(2);
				const nextArg = args[i + 1];
				if (nextArg !== undefined && !nextArg.startsWith("-")) {
					flags[key] = nextArg;
					i++;
				} else {
					flags[key] = "true";
				}
			}
		} else if (arg.startsWith("-") && arg.length === 2) {
			// Short flag: -s value
			const key = arg.slice(1);
			const nextArg = args[i + 1];
			if (nextArg !== undefined && !nextArg.startsWith("-")) {
				flags[key] = nextArg;
				i++;
			} else {
				flags[key] = "true";
			}
		} else {
			positional.push(arg);
		}
		i++;
	}

	return { command, flags, positional };
}

/** Get a required flag value, printing an error and exiting if missing. */
export function requireFlag(flags: Record<string, string>, name: string): string {
	const value = flags[name];
	if (value === undefined || value === "true") {
		process.stderr.write(`Error: --${name} is required\n`);
		process.exit(1);
	}
	return value;
}
