import { constants } from "node:fs";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as path from "node:path";
import {
	encodeRemoteFrame,
	parseRemoteHelloResult,
	parseRemoteInstanceList,
	parseRemoteServerFrame,
	REMOTE_MAX_FRAME_BYTES,
	REMOTE_MAX_INSTANCES,
	REMOTE_PROTOCOL_VERSION,
	REMOTE_REQUIRED_CAPABILITIES,
	RemoteFrameDecoder,
	type RemoteErrorCode,
	type RemoteHello,
	type RemoteInstance,
} from "./remote-wire";

export interface RemoteRegistration extends RemoteInstance {
	epoch: string;
	socketPath: string;
}

export class RemoteRegistrationError extends Error {
	constructor(
		readonly code: RemoteErrorCode,
		message: string,
	) {
		super(message);
		this.name = "RemoteRegistrationError";
	}
}

const PROBE_TIMEOUT_MS = 1500;
const keyPattern = /^[0-9a-f]{64}$/;
const epochPattern = /^[0-9a-f]{32}$/;

type FileIdentity = { dev: number; ino: number };
type Owner = { pid: number; epoch: string };

function uid(): number {
	const value = process.getuid?.();
	if (value === undefined) throw new RemoteRegistrationError("internal_error", "Unix account required");
	return value;
}

async function runtimeRoot(create: boolean): Promise<string> {
	// Unlike TMPDIR, this location is the same in interactive and SSH sessions, and
	// remains short enough for Darwin's 104-byte sockaddr_un path limit.
	const root = `/tmp/omp-${uid()}`;
	if (create) await fs.mkdir(root, { mode: 0o700, recursive: true });
	const stat = await fs.lstat(root);
	if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== uid() || (stat.mode & 0o777) !== 0o700) {
		throw new RemoteRegistrationError("internal_error", "Unsafe remote runtime directory");
	}
	return root;
}

function sameFile(a: FileIdentity, b: FileIdentity): boolean {
	return a.dev === b.dev && a.ino === b.ino;
}

function missing(error: unknown): boolean {
	return (error as NodeJS.ErrnoException).code === "ENOENT";
}

async function readPrivateJson(file: string): Promise<unknown> {
	const handle = await fs.open(file, constants.O_RDONLY | constants.O_NOFOLLOW);
	try {
		const stat = await handle.stat();
		if (!stat.isFile() || stat.uid !== uid() || (stat.mode & 0o077) !== 0 || stat.size > REMOTE_MAX_FRAME_BYTES) {
			throw new Error("Unsafe registry file");
		}
		const bytes = new Uint8Array(stat.size + 1);
		const { bytesRead } = await handle.read(bytes, 0, bytes.length, 0);
		if (bytesRead > REMOTE_MAX_FRAME_BYTES || bytesRead !== stat.size) throw new Error("Invalid registry size");
		return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, bytesRead)));
	} finally {
		await handle.close();
	}
}

function validOwner(value: unknown): value is Owner {
	if (typeof value !== "object" || value === null) return false;
	const owner = value as Record<string, unknown>;
	return (
		Number.isSafeInteger(owner.pid) &&
		(owner.pid as number) > 0 &&
		typeof owner.epoch === "string" &&
		epochPattern.test(owner.epoch)
	);
}

function isAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (error as NodeJS.ErrnoException).code !== "ESRCH";
	}
}

async function removeIfSame(file: string, identity: FileIdentity): Promise<void> {
	try {
		if (sameFile(await fs.lstat(file), identity)) await fs.unlink(file);
	} catch (error) {
		if (!missing(error)) throw error;
	}
}

async function reserve(root: string, key: string, epoch: string): Promise<{ dir: string; identity: FileIdentity }> {
	const dir = path.join(root, key);
	const staging = path.join(root, `pending-${epoch}`);
	await fs.mkdir(staging, { mode: 0o700 });
	try {
		await fs.writeFile(path.join(staging, "owner"), JSON.stringify({ pid: process.pid, epoch }), {
			mode: 0o600,
			flag: "wx",
		});
	} catch (error) {
		await fs.rm(staging, { recursive: true, force: true });
		throw error;
	}
	try {
		for (;;) {
			try {
				await fs.rename(staging, dir);
				return { dir, identity: await fs.lstat(dir) };
			} catch (error) {
				const code = (error as NodeJS.ErrnoException).code;
				if (code !== "EEXIST" && code !== "ENOTEMPTY") throw error;
			}
			// A PID only protects a reservation; discovery always checks the socket.
			// Claim stale cleanup inside the directory so only one contender can reap it.
			const claim = path.join(dir, "reap");
			try {
				await fs.mkdir(claim, { mode: 0o700 });
			} catch (error) {
				if (missing(error)) continue;
				throw new RemoteRegistrationError("busy", "Remote identity is reserved");
			}
			let retired: string | undefined;
			try {
				const stat = await fs.lstat(dir);
				if (!stat.isDirectory() || stat.uid !== uid() || (stat.mode & 0o777) !== 0o700)
					throw new Error("Unsafe reservation");
				const owner = await readPrivateJson(path.join(dir, "owner"));
				if (!validOwner(owner) || isAlive(owner.pid))
					throw new RemoteRegistrationError("busy", "Remote identity is reserved");
				const registration = await readRegistration(root, key).catch(() => null);
				if (registration) {
					try {
						await probe(registration);
						throw new RemoteRegistrationError("busy", "Remote identity is live");
					} catch (error) {
						if (error instanceof RemoteRegistrationError && error.code === "busy") throw error;
					}
				}
				// Keep the claim in the old directory while moving it out of the identity
				// namespace. A new owner can never be removed by the stale cleanup.
				retired = path.join(root, `retired-${crypto.randomUUID()}`);
				await fs.rename(dir, retired);
			} finally {
				if (retired) await fs.rm(retired, { recursive: true, force: true });
				else await fs.rmdir(claim).catch(() => {});
			}
		}
	} finally {
		await fs.rm(staging, { recursive: true, force: true });
	}
}

async function readRegistration(root: string, key: string): Promise<RemoteRegistration> {
	if (!keyPattern.test(key)) throw new RemoteRegistrationError("identity_mismatch", "Invalid instance key");
	const dir = path.join(root, key);
	const stat = await fs.lstat(dir);
	if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== uid() || (stat.mode & 0o777) !== 0o700)
		throw new Error("Unsafe reservation");
	const value = await readPrivateJson(path.join(dir, "registration"));
	if (typeof value !== "object" || value === null) throw new Error("Invalid registration");
	const record = value as Record<string, unknown>;
	const { epoch, socketPath, ...publicFields } = record;
	const parsed = parseRemoteInstanceList([publicFields]);
	if (
		!parsed ||
		parsed[0].instanceKey !== key ||
		typeof epoch !== "string" ||
		!epochPattern.test(epoch) ||
		socketPath !== path.join(root, `s-${epoch}`)
	)
		throw new Error("Invalid registration");
	const owner = await readPrivateJson(path.join(dir, "owner"));
	if (!validOwner(owner) || owner.epoch !== epoch) throw new Error("Invalid registration owner");
	const digest = new Bun.CryptoHasher("sha256")
		.update(JSON.stringify([parsed[0].name, parsed[0].workspace]))
		.digest("hex");
	if (digest !== key) throw new Error("Invalid registration identity");
	return { ...parsed[0], epoch, socketPath };
}

async function probe(registration: RemoteRegistration): Promise<void> {
	const offer: RemoteHello = {
		type: "hello",
		id: crypto.randomUUID(),
		versions: [REMOTE_PROTOCOL_VERSION],
		instanceKey: registration.instanceKey,
		workspace: registration.workspace,
		capabilities: REMOTE_REQUIRED_CAPABILITIES,
		maxFrameBytes: REMOTE_MAX_FRAME_BYTES,
	};
	const result = Promise.withResolvers<void>();
	const socket = net.createConnection(registration.socketPath);
	const decoder = new RemoteFrameDecoder();
	const timer = setTimeout(
		() => result.reject(new RemoteRegistrationError("timeout", "Remote probe timed out")),
		PROBE_TIMEOUT_MS,
	);
	socket.once("connect", () => socket.write(encodeRemoteFrame(offer)));
	socket.on("data", chunk => {
		try {
			const frames = decoder.push(chunk);
			if (frames.length === 0) return;
			const frame = parseRemoteServerFrame(frames[0]);
			if (!frame || frame.type !== "reply" || frame.id !== offer.id)
				throw new RemoteRegistrationError("protocol_incompatible", "Invalid remote hello");
			if (!frame.ok) throw new RemoteRegistrationError(frame.error.code, "Remote hello rejected");
			const data = parseRemoteHelloResult(frame.data, offer);
			if (!data) {
				const raw = frame.data as Record<string, unknown> | null;
				const mismatch =
					raw && (raw.instanceKey !== registration.instanceKey || raw.workspace !== registration.workspace);
				throw new RemoteRegistrationError(
					mismatch ? "identity_mismatch" : "protocol_incompatible",
					"Remote hello does not match registration",
				);
			}
			if (data.epoch !== registration.epoch)
				throw new RemoteRegistrationError("identity_mismatch", "Remote epoch does not match registration");
			result.resolve();
		} catch (error) {
			result.reject(
				error instanceof RemoteRegistrationError
					? error
					: new RemoteRegistrationError("protocol_incompatible", "Invalid remote hello"),
			);
		}
	});
	socket.once("error", () => result.reject(new RemoteRegistrationError("no_instance", "Remote instance unavailable")));
	socket.once("end", () => result.reject(new RemoteRegistrationError("no_instance", "Remote instance closed probe")));
	try {
		await result.promise;
	} finally {
		clearTimeout(timer);
		socket.destroy();
	}
}

export async function prepareRemoteRegistration(
	name: string,
	workspace: string,
): Promise<{ registration: RemoteRegistration; publish(): Promise<void>; dispose(): Promise<void> }> {
	const canonical = await fs.realpath(workspace);
	const instanceKey = new Bun.CryptoHasher("sha256").update(JSON.stringify([name, canonical])).digest("hex");
	const epoch = crypto.randomUUID().replaceAll("-", "");
	const root = await runtimeRoot(true);
	const registration: RemoteRegistration = {
		name,
		instanceKey,
		workspace: canonical,
		versions: [REMOTE_PROTOCOL_VERSION],
		capabilities: REMOTE_REQUIRED_CAPABILITIES,
		epoch,
		socketPath: path.join(root, `s-${epoch}`),
	};
	if (
		!parseRemoteInstanceList([
			{
				name,
				instanceKey,
				workspace: canonical,
				versions: registration.versions,
				capabilities: registration.capabilities,
			},
		])
	)
		throw new RemoteRegistrationError("invalid_request", "Invalid remote identity");
	const reservation = await reserve(root, instanceKey, epoch);
	let published: FileIdentity | undefined;
	let socketIdentity: FileIdentity | undefined;
	let disposed = false;
	return {
		registration,
		async publish() {
			if (disposed || published)
				throw new RemoteRegistrationError("internal_error", "Registration is not publishable");
			if (!sameFile(await fs.lstat(reservation.dir), reservation.identity))
				throw new RemoteRegistrationError("identity_mismatch", "Reservation was replaced");
			const stat = await fs.lstat(registration.socketPath);
			if (!stat.isSocket() || stat.uid !== uid())
				throw new RemoteRegistrationError("internal_error", "Remote socket is unsafe");
			socketIdentity = stat;
			await fs.chmod(registration.socketPath, 0o600);
			const file = path.join(reservation.dir, "registration");
			const temp = path.join(reservation.dir, `registration-${epoch}`);
			await fs.writeFile(temp, JSON.stringify(registration), { flag: "wx", mode: 0o600 });
			try {
				await fs.rename(temp, file);
			} catch (error) {
				await fs.unlink(temp).catch(() => {});
				throw error;
			}
			published = await fs.lstat(file);
		},
		async dispose() {
			if (disposed) return;
			disposed = true;
			if (socketIdentity) await removeIfSame(registration.socketPath, socketIdentity);
			const current = await fs.lstat(reservation.dir).catch(() => null);
			if (!current || !sameFile(current, reservation.identity)) return;
			const ownerFile = path.join(reservation.dir, "owner");
			const owner = await readPrivateJson(ownerFile).catch(() => null);
			if (!validOwner(owner) || owner.epoch !== epoch || owner.pid !== process.pid) return;
			if (published) await removeIfSame(path.join(reservation.dir, "registration"), published);
			await fs.unlink(ownerFile).catch(error => {
				if (!missing(error)) throw error;
			});
			await fs.rmdir(reservation.dir).catch(error => {
				if (!missing(error) && (error as NodeJS.ErrnoException).code !== "ENOTEMPTY") throw error;
			});
		},
	};
}

export async function listRemoteInstances(): Promise<RemoteInstance[]> {
	let root: string;
	try {
		root = await runtimeRoot(false);
	} catch (error) {
		if (missing(error)) return [];
		throw error;
	}
	const entries = await fs.readdir(root);
	const instances: RemoteInstance[] = [];
	for (const key of entries) {
		if (!keyPattern.test(key)) continue;
		try {
			const registration = await readRegistration(root, key);
			await probe(registration);
			const { epoch: _epoch, socketPath: _socketPath, ...instance } = registration;
			instances.push(instance);
			if (instances.length > REMOTE_MAX_INSTANCES)
				throw new RemoteRegistrationError("overflow", "Too many remote instances");
		} catch (error) {
			if (error instanceof RemoteRegistrationError && error.code === "overflow") throw error;
		}
	}
	return instances;
}

export async function attachRemoteInstance(key: string): Promise<void> {
	if (!keyPattern.test(key)) throw new RemoteRegistrationError("identity_mismatch", "Invalid instance key");
	let registration: RemoteRegistration;
	try {
		registration = await readRegistration(await runtimeRoot(false), key);
	} catch (error) {
		if (error instanceof RemoteRegistrationError) throw error;
		throw new RemoteRegistrationError("no_instance", "Remote instance unavailable");
	}
	const initialSocket = await fs.lstat(registration.socketPath).catch(() => null);
	if (!initialSocket?.isSocket() || initialSocket.uid !== uid())
		throw new RemoteRegistrationError("no_instance", "Remote socket unavailable");
	await probe(registration);
	// Probe is a separate connection: the forwarded stream must begin with the client's hello.
	const done = Promise.withResolvers<void>();
	const socket = net.createConnection(registration.socketPath);
	socket.once("connect", () => {
		void fs.lstat(registration.socketPath).then(
			current => {
				if (!sameFile(initialSocket, current) || !current.isSocket()) {
					done.reject(new RemoteRegistrationError("identity_mismatch", "Remote socket was replaced"));
					return;
				}
				process.stdin.pipe(socket);
				socket.pipe(process.stdout, { end: false });
			},
			() => done.reject(new RemoteRegistrationError("identity_mismatch", "Remote socket was replaced")),
		);
	});
	socket.once("error", () => done.reject(new RemoteRegistrationError("disconnected", "Remote bridge failed")));
	socket.once("end", () => done.resolve());
	socket.once("close", () => done.resolve());
	try {
		await done.promise;
	} finally {
		process.stdin.unpipe(socket);
		process.stdin.pause();
		socket.unpipe(process.stdout);
		socket.destroy();
	}
}
