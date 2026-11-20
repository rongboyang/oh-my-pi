import { expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import {
	attachRemoteInstance,
	listRemoteInstances,
	prepareRemoteRegistration,
	type RemoteRegistration,
} from "../src/modes/rpc/remote-registration";
import { REMOTE_MAX_FRAME_BYTES, REMOTE_REQUIRED_CAPABILITIES, RemoteFrameDecoder } from "../src/modes/rpc/remote-wire";

async function serve(registration: RemoteRegistration, epoch = registration.epoch): Promise<net.Server> {
	const server = net.createServer(socket => {
		const decoder = new RemoteFrameDecoder();
		socket.on("data", chunk => {
			const frames = decoder.push(chunk) as { id: string }[];
			for (const frame of frames)
				socket.write(
					`${JSON.stringify({
						type: "reply",
						id: frame.id,
						ok: true,
						data: {
							version: 1,
							instanceKey: registration.instanceKey,
							workspace: registration.workspace,
							epoch,
							capabilities: REMOTE_REQUIRED_CAPABILITIES,
							maxFrameBytes: REMOTE_MAX_FRAME_BYTES,
						},
					})}\n`,
				);
		});
	});
	const ready = Promise.withResolvers<void>();
	server.once("error", ready.reject);
	server.listen(registration.socketPath, ready.resolve);
	await ready.promise;
	return server;
}

async function close(server: net.Server): Promise<void> {
	const done = Promise.withResolvers<void>();
	server.close(error => (error ? done.reject(error) : done.resolve()));
	await done.promise;
}

test("identity uses physical workspace and reserves concurrent starts", async () => {
	const dir = await fs.mkdtemp(path.join(os.tmpdir(), "omp-reg-test-"));
	const link = `${dir}-link`;
	await fs.symlink(dir, link);
	const name = crypto.randomUUID();
	const prepared = await prepareRemoteRegistration(name, link);
	try {
		const canonical = await fs.realpath(dir);
		expect(prepared.registration.workspace).toBe(canonical);
		expect(prepared.registration.instanceKey).toBe(
			new Bun.CryptoHasher("sha256").update(JSON.stringify([name, canonical])).digest("hex"),
		);
		await expect(prepareRemoteRegistration(name, dir)).rejects.toMatchObject({ code: "busy" });
	} finally {
		await prepared.dispose();
		await fs.unlink(link);
		await fs.rm(dir, { recursive: true, force: true });
	}
});

test("discovery checks peer epoch, publishes only public attributes, and omits stale sockets", async () => {
	const prepared = await prepareRemoteRegistration(crypto.randomUUID(), process.cwd());
	const { registration } = prepared;
	let server = await serve(registration);
	try {
		await prepared.publish();
		const found = (await listRemoteInstances()).find(item => item.instanceKey === registration.instanceKey);
		expect(found).toEqual({
			name: registration.name,
			instanceKey: registration.instanceKey,
			workspace: registration.workspace,
			versions: registration.versions,
			capabilities: registration.capabilities,
		});
		await close(server);
		server = await serve(registration, crypto.randomUUID());
		expect((await listRemoteInstances()).some(item => item.instanceKey === registration.instanceKey)).toBe(false);
		await expect(attachRemoteInstance(registration.instanceKey)).rejects.toMatchObject({ code: "identity_mismatch" });
		await close(server);
		expect((await listRemoteInstances()).some(item => item.instanceKey === registration.instanceKey)).toBe(false);
	} finally {
		if (server.listening) await close(server);
		await prepared.dispose();
	}
});

test("stale owner is reaped and old disposal cannot remove its replacement", async () => {
	const name = crypto.randomUUID();
	const old = await prepareRemoteRegistration(name, process.cwd());
	const dir = path.join(path.dirname(old.registration.socketPath), old.registration.instanceKey);
	await fs.writeFile(path.join(dir, "owner"), JSON.stringify({ pid: 2147483647, epoch: old.registration.epoch }), {
		mode: 0o600,
	});
	const current = await prepareRemoteRegistration(name, process.cwd());
	try {
		expect(current.registration.epoch).not.toBe(old.registration.epoch);
		await old.dispose();
		await expect(prepareRemoteRegistration(name, process.cwd())).rejects.toMatchObject({ code: "busy" });
	} finally {
		await current.dispose();
	}
});

test("disposal does not unlink a replacement at the socket path", async () => {
	const prepared = await prepareRemoteRegistration(crypto.randomUUID(), process.cwd());
	const server = await serve(prepared.registration);
	try {
		await prepared.publish();
		await fs.unlink(prepared.registration.socketPath);
		await fs.writeFile(prepared.registration.socketPath, "replacement", { mode: 0o600 });
		await prepared.dispose();
		expect(await fs.readFile(prepared.registration.socketPath, "utf8")).toBe("replacement");
	} finally {
		await close(server);
		await fs.unlink(prepared.registration.socketPath).catch(() => {});
		await prepared.dispose();
	}
});
