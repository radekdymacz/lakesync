#!/usr/bin/env node

import { parseArgs } from "./args";
import { gatewaysCreate, gatewaysDelete, gatewaysList } from "./commands/gateways";
import { init } from "./commands/init";
import { keysCreate, keysList, keysRevoke } from "./commands/keys";
import { login } from "./commands/login";
import { logout } from "./commands/logout";
import { pull } from "./commands/pull";
import { push } from "./commands/push";
import { status } from "./commands/status";
import { tokenCreate } from "./commands/token";
import { fatal, print } from "./output";

const VERSION = "0.1.0";

const HELP = `lakesync — CLI for LakeSync sync engine

Usage: lakesync <command> [options]

Commands:
  init                     Set up a new LakeSync project
  login                    Store gateway URL and token in config
  logout                   Remove stored token from config
  token create             Generate a JWT for development/testing
  gateways list            List gateways for the organisation
  gateways create          Create a new gateway
  gateways delete          Delete a gateway
  keys list                List API keys for the organisation
  keys create              Create a new API key
  keys revoke              Revoke an API key
  push <file>              Push deltas from a JSON file
  pull                     Pull deltas from the gateway
  status                   Show gateway health and metrics

Login options:
  --url <url>              Gateway base URL (or LAKESYNC_GATEWAY_URL env)
  --token <token>          Bearer token (or LAKESYNC_TOKEN env)
  --gateway <id>           Default gateway ID (or LAKESYNC_GATEWAY_ID env)

Token options:
  --secret <secret>        JWT signing secret (or LAKESYNC_JWT_SECRET env)
  --gateway <id>           Gateway identifier
  --client <id>            Client identifier (default: "cli-user")
  --role <admin|client>    Token role (default: "client")
  --ttl <seconds>          Token time-to-live (default: 3600)

Gateway options:
  --org <id>               Organisation ID (or set orgId in config)
  --name <name>            Gateway name (for create)
  --region <region>        Gateway region (for create)
  --id <id>                Gateway ID (for delete)

Key options:
  --org <id>               Organisation ID (or set orgId in config)
  --name <name>            Key name (for create)
  --role <admin|client>    Key role (default: "client")
  --gateway <id>           Scope key to a gateway (for create)
  --expires <iso-date>     Expiry date (for create)
  --id <id>                Key ID (for revoke)

Push/Pull/Status options:
  --url <url>              Gateway base URL (or set in ~/.lakesync/config.json)
  --gateway <id>           Gateway identifier
  --token <token>          Bearer token (or LAKESYNC_TOKEN env)
  --client <id>            Client identifier (default: "cli")

Pull-specific options:
  --since <hlc>            Pull deltas since this HLC timestamp (default: 0)
  --limit <n>              Maximum deltas to pull (default: 1000)
  --source <name>          Named source adapter for adapter-sourced pull

General:
  --help, -h               Show this help message
  --version, -v            Show version

Examples:
  lakesync init
  lakesync login --url http://localhost:3000 --token eyJ...
  lakesync token create --secret my-secret --gateway my-gw
  lakesync gateways list --org org_123
  lakesync gateways create --org org_123 --name my-gateway
  lakesync keys create --org org_123 --name ci-key --role admin
  lakesync push deltas.json --url http://localhost:3000 --gateway my-gw
  lakesync pull --url http://localhost:3000 --gateway my-gw --since 0
  lakesync status --url http://localhost:3000
`;

async function main(): Promise<void> {
	const { command, flags, positional } = parseArgs(process.argv);

	if (flags.version === "true" || flags.v === "true") {
		print(VERSION);
		return;
	}

	if (flags.help === "true" || flags.h === "true" || command.length === 0) {
		print(HELP);
		return;
	}

	const cmd = command.join(" ");

	switch (cmd) {
		case "init":
			init(flags);
			break;

		case "login":
			login(flags);
			break;

		case "logout":
			logout();
			break;

		case "token create":
			await tokenCreate(flags);
			break;

		case "gateways list":
			await gatewaysList(flags);
			break;

		case "gateways create":
			await gatewaysCreate(flags);
			break;

		case "gateways delete":
			await gatewaysDelete(flags);
			break;

		case "keys list":
			await keysList(flags);
			break;

		case "keys create":
			await keysCreate(flags);
			break;

		case "keys revoke":
			await keysRevoke(flags);
			break;

		case "push":
			await push(flags, positional);
			break;

		case "pull":
			await pull(flags);
			break;

		case "status":
			await status(flags);
			break;

		case "help":
			print(HELP);
			break;

		case "version":
			print(VERSION);
			break;

		default:
			fatal(`Unknown command: ${cmd}\nRun 'lakesync --help' for usage.`);
	}
}

main().catch((err) => {
	fatal(String(err));
});
