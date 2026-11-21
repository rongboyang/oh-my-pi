import { Command } from "@oh-my-pi/pi-utils/cli";
import { parseArgs, reportCliUsageError } from "../cli/args";
import { runRootCommand } from "../main";
import { attachRemoteInstance, listRemoteInstances, RemoteRegistrationError } from "../modes/rpc/remote-registration";

const usage = "Usage: omp remote serve --name <name> [launch options] | list --json | attach --instance <key>";

export default class Remote extends Command {
	static description = "Serve, discover, or attach to a remote workspace instance";
	static strict = false;

	async run(): Promise<void> {
		const [action, ...args] = this.argv;
		try {
			if (action === "list" && args.length === 1 && args[0] === "--json") {
				const instances = await listRemoteInstances();
				if (instances.length === 0) {
					process.stderr.write("No live remote instance\n");
					process.exitCode = 3;
					return;
				}
				process.stdout.write(`${JSON.stringify(instances)}\n`);
				return;
			}
			if (action === "attach" && args.length === 2 && args[0] === "--instance" && /^[0-9a-f]{64}$/.test(args[1])) {
				await attachRemoteInstance(args[1]);
				return;
			}
			if (action === "serve") {
				const nameIndex = args.indexOf("--name");
				if (
					nameIndex < 0 ||
					!args[nameIndex + 1] ||
					args[nameIndex + 1].startsWith("-") ||
					args.lastIndexOf("--name") !== nameIndex
				) {
					process.stderr.write(`${usage}\n`);
					process.exitCode = 2;
					return;
				}
				const name = args[nameIndex + 1];
				const launchArgs = [...args.slice(0, nameIndex), ...args.slice(nameIndex + 2)];
				const parsed = parseArgs(launchArgs);
				if (
					parsed.help ||
					parsed.version ||
					parsed.export ||
					parsed.resume ||
					parsed.restore ||
					parsed.continue ||
					parsed.fork ||
					parsed.print ||
					parsed.noSession ||
					parsed.noUi
				) {
					process.stderr.write(`${usage}\n`);
					process.exitCode = 2;
					return;
				}
				parsed.mode = "rpc-ui";
				await runRootCommand(parsed, launchArgs, { remoteName: name });
				return;
			}
			process.stderr.write(`${usage}\n`);
			process.exitCode = 2;
		} catch (error) {
			if (reportCliUsageError(error)) {
				process.exitCode = 2;
				return;
			}
			if (error instanceof RemoteRegistrationError) {
				process.stderr.write(`Remote command failed: ${error.code}\n`);
				process.exitCode =
					error.code === "invalid_request"
						? 2
						: error.code === "no_instance"
							? 3
							: error.code === "identity_mismatch"
								? 4
								: error.code === "protocol_incompatible"
									? 5
									: 1;
				return;
			}
			process.stderr.write("Remote command failed\n");
			process.exitCode = 1;
		}
	}
}
