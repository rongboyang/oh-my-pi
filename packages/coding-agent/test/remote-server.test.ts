import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import type { AgentSession } from "../src/session/agent-session";
import { startRemoteRpcServer } from "../src/modes/rpc/remote-server";
import {
	encodeRemoteFrame,
	RemoteFrameDecoder,
	REMOTE_MAX_FRAME_BYTES,
	REMOTE_REQUIRED_CAPABILITIES,
} from "../src/modes/rpc/remote-wire";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
	while (cleanup.length) await cleanup.pop()?.();
});

async function connect(socketPath: string) {
	const socket = net.createConnection(socketPath);
	const connected = Promise.withResolvers<void>();
	socket.once("connect", connected.resolve);
	socket.once("error", connected.reject);
	await connected.promise;
	const decoder = new RemoteFrameDecoder();
	const frames: Record<string, unknown>[] = [];
	let waiter: ((frame: Record<string, unknown>) => void) | undefined;
	socket.on("data", bytes => {
		for (const frame of decoder.push(bytes) as Record<string, unknown>[]) {
			if (waiter) {
				const resolve = waiter;
				waiter = undefined;
				resolve(frame);
			} else frames.push(frame);
		}
	});
	return {
		socket,
		send(frame: object) {
			socket.write(encodeRemoteFrame(frame));
		},
		async next(): Promise<Record<string, unknown>> {
			const frame = frames.shift();
			if (frame) return frame;
			const deferred = Promise.withResolvers<Record<string, unknown>>();
			waiter = deferred.resolve;
			return deferred.promise;
		},
	};
}

describe("remote RPC instance", () => {
	test("keeps the owned RPC input and pending approval alive across attaches", async () => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-server-"));
		cleanup.push(() => fs.rm(directory, { recursive: true, force: true }));
		const workspace = await fs.realpath(directory);
		const name = `test-${randomUUID()}`;
		const session = {
			sessionManager: { getCwd: () => workspace, getSessionId: () => "session-1" },
			isRunActive: false,
		} as unknown as AgentSession;
		const server = await startRemoteRpcServer(name, session);
		cleanup.push(() => server.close());
		const key = new Bun.CryptoHasher("sha256").update(JSON.stringify([name, workspace])).digest("hex");
		const record = (await Bun.file(`/tmp/omp-${process.getuid?.()}/${key}/registration`).json()) as {
			socketPath: string;
			epoch: string;
		};
		const reader = server.input.getReader();
		const nextInput = reader.read();
		const hello = {
			type: "hello",
			id: "hello",
			versions: [1],
			instanceKey: key,
			workspace,
			capabilities: REMOTE_REQUIRED_CAPABILITIES,
			maxFrameBytes: REMOTE_MAX_FRAME_BYTES,
		};
		const first = await connect(record.socketPath);
		first.send(hello);
		expect((await first.next()).ok).toBe(true);
		first.send({ type: "request", id: "acquire", op: "acquire", data: {} });
		expect((await first.next()).data).toEqual({ lease: 1 });
		server.output({ type: "extension_ui_request", id: "approval-1", method: "confirm", title: "startup" });
		first.send({ type: "request", id: "sync", op: "sync", data: { lease: 1 } });
		const snapshot = (await first.next()).data as { phase: string; pendingApprovals: unknown[] };
		expect(snapshot.phase).toBe("starting");
		expect(snapshot.pendingApprovals).toHaveLength(1);
		const firstClosed = once(first.socket, "close");
		first.socket.destroy();
		await firstClosed;
		const second = await connect(record.socketPath);
		second.send(hello);
		expect((await second.next()).ok).toBe(true);
		second.send({ type: "request", id: "acquire-2", op: "acquire", data: {} });
		expect((await second.next()).data).toEqual({ lease: 2 });
		second.send({
			type: "request",
			id: "answer",
			op: "action",
			data: {
				lease: 2,
				epoch: record.epoch,
				sessionId: "session-1",
				actionId: "answer-1",
				command: { type: "respond", requestId: "approval-1", response: { confirmed: true } },
			},
		});
		expect((await second.next()).ok).toBe(true);
		const answer = await nextInput;
		expect(JSON.parse(new TextDecoder().decode(answer.value))).toEqual({
			type: "extension_ui_response",
			id: "approval-1",
			confirmed: true,
		});
		server.output({ type: "ready" });
		second.send({ type: "request", id: "sync-2", op: "sync", data: { lease: 2 } });
		await second.next();
		second.send({
			type: "request",
			id: "prompt-ack",
			op: "action",
			data: {
				lease: 2,
				epoch: record.epoch,
				sessionId: "session-1",
				actionId: "action-ack",
				command: { type: "prompt", message: "hello" },
			},
		});
		const acknowledged = JSON.parse(new TextDecoder().decode((await reader.read()).value)) as { id: string };
		server.output({ type: "response", id: acknowledged.id, command: "prompt", success: true });
		expect((await second.next()).data).toEqual({ status: "succeeded", data: null });
		second.send({
			type: "request",
			id: "prompt",
			op: "action",
			data: {
				lease: 2,
				epoch: record.epoch,
				sessionId: "session-1",
				actionId: "prompt-1",
				command: { type: "prompt", message: "continue working" },
			},
		});
		const prompt = JSON.parse(new TextDecoder().decode((await reader.read()).value)) as {
			id: string;
			type: string;
			message: string;
		};
		expect(prompt.type).toBe("prompt");
		expect(prompt.message).toBe("continue working");
		const secondClosed = once(second.socket, "close");
		second.socket.destroy();
		await secondClosed;
		server.output({ type: "response", id: prompt.id, command: "prompt", success: true });
		server.output({ type: "agent_start" });
		const third = await connect(record.socketPath);
		third.send(hello);
		expect((await third.next()).ok).toBe(true);
		third.send({ type: "request", id: "acquire-3", op: "acquire", data: {} });
		expect((await third.next()).data).toEqual({ lease: 3 });
		third.send({ type: "request", id: "sync-3", op: "sync", data: { lease: 3 } });
		const reattached = (await third.next()).data as { epoch: string; runId: string };
		expect(reattached.epoch).toBe(record.epoch);
		expect(reattached.runId).toBeTruthy();
		third.socket.destroy();
		reader.releaseLock();
	});

	test.each(["SIGINT", "SIGTERM"] as const)("closes input on %s without resending the signal", async signal => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-server-"));
		cleanup.push(() => fs.rm(directory, { recursive: true, force: true }));
		const workspace = await fs.realpath(directory);
		const session = {
			sessionManager: { getCwd: () => workspace, getSessionId: () => "session-1" },
			isRunActive: false,
		} as unknown as AgentSession;
		const previousListeners = new Set(process.rawListeners(signal));
		const server = await startRemoteRpcServer(`test-${randomUUID()}`, session);
		cleanup.push(() => server.close());
		const listeners = process.rawListeners(signal).filter(listener => !previousListeners.has(listener));
		expect(listeners).toHaveLength(1);
		const kill = spyOn(process, "kill").mockImplementation(() => true);
		try {
			server.output({ type: "extension_ui_request", id: "approval-1", method: "confirm", title: "startup" });
			const reader = server.input.getReader();
			const pendingInput = reader.read();
			listeners[0].call(process);
			expect((await pendingInput).done).toBe(true);
			await server.close();
			await Promise.resolve();
			expect(kill).not.toHaveBeenCalled();
			reader.releaseLock();
		} finally {
			kill.mockRestore();
		}
	});

	test("rejects wrong identities and does not let another peer preempt a lease", async () => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-server-"));
		cleanup.push(() => fs.rm(directory, { recursive: true, force: true }));
		const workspace = await fs.realpath(directory);
		const name = `test-${randomUUID()}`;
		const session = {
			sessionManager: { getCwd: () => workspace, getSessionId: () => "session-1" },
			isRunActive: false,
		} as unknown as AgentSession;
		const server = await startRemoteRpcServer(name, session);
		cleanup.push(() => server.close());
		const key = new Bun.CryptoHasher("sha256").update(JSON.stringify([name, workspace])).digest("hex");
		const record = (await Bun.file(`/tmp/omp-${process.getuid?.()}/${key}/registration`).json()) as {
			socketPath: string;
		};
		const hello = {
			type: "hello",
			id: "hello",
			versions: [1],
			instanceKey: key,
			workspace,
			capabilities: REMOTE_REQUIRED_CAPABILITIES,
			maxFrameBytes: REMOTE_MAX_FRAME_BYTES,
		};
		const wrong = await connect(record.socketPath);
		wrong.send({ ...hello, workspace: "/wrong" });
		expect((await wrong.next()).error).toEqual({ code: "identity_mismatch" });
		const first = await connect(record.socketPath);
		first.send(hello);
		expect((await first.next()).ok).toBe(true);
		first.send({ type: "request", id: "acquire", op: "acquire", data: {} });
		expect((await first.next()).data).toEqual({ lease: 1 });
		const second = await connect(record.socketPath);
		second.send(hello);
		expect((await second.next()).ok).toBe(true);
		second.send({ type: "request", id: "acquire-2", op: "acquire", data: {} });
		expect((await second.next()).error).toEqual({ code: "busy" });
		first.socket.destroy();
		second.socket.destroy();
	});
});
