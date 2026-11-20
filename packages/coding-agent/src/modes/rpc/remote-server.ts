import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import type { AgentSession } from "../../session/agent-session";
import { prepareRemoteRegistration } from "./remote-registration";
import {
	encodeRemoteFrame,
	parseRemoteClientFrame,
	RemoteFrameDecoder,
	REMOTE_LEASE_TIMEOUT_MS,
	REMOTE_MAX_FRAME_BYTES,
	REMOTE_MAX_QUEUE_BYTES,
	REMOTE_MAX_QUEUE_FRAMES,
	REMOTE_PROTOCOL_VERSION,
	REMOTE_REQUIRED_CAPABILITIES,
	type RemoteErrorCode,
	type RemoteRequest,
} from "./remote-wire";

interface Peer {
	socket: net.Socket;
	decoder: RemoteFrameDecoder;
	limit: number;
	hello: boolean;
	synced: boolean;
	queuedFrames: number;
}

interface PendingCommand {
	peer: Peer;
	requestId: string;
}

/** Owns the RPC stream for the lifetime of the instance, not an attach. */
export async function startRemoteRpcServer(
	name: string,
	session: AgentSession,
): Promise<{
	input: ReadableStream<Uint8Array>;
	output(frame: object): void;
	close(): Promise<void>;
}> {
	const workspace = await fs.realpath(session.sessionManager.getCwd());
	const owner = await prepareRemoteRegistration(name, workspace);
	const { registration } = owner;
	const peers = new Set<Peer>();
	const pending = new Map<string, PendingCommand>();
	const approvals = new Map<string, Record<string, unknown>>();
	let controller: ReadableStreamDefaultController<Uint8Array>;
	let inputBytes = 0;
	const inputQueue: Uint8Array[] = [];
	let inputClosed = false;
	let waitingForInput = false;
	const input = new ReadableStream<Uint8Array>(
		{
			start(value) {
				controller = value;
			},
			pull(value) {
				const next = inputQueue.shift();
				if (next) {
					inputBytes -= next.byteLength;
					value.enqueue(next);
				} else waitingForInput = true;
			},
			cancel() {
				inputClosed = true;
				inputQueue.length = 0;
				inputBytes = 0;
			},
		},
		{ highWaterMark: 0 },
	);
	const sendInput = (frame: object): boolean => {
		if (inputClosed) return false;
		const bytes = encodeRemoteFrame(frame);
		if (inputQueue.length >= REMOTE_MAX_QUEUE_FRAMES || inputBytes + bytes.byteLength > REMOTE_MAX_QUEUE_BYTES)
			return false;
		if (waitingForInput) {
			waitingForInput = false;
			controller.enqueue(bytes);
		} else {
			inputQueue.push(bytes);
			inputBytes += bytes.byteLength;
		}
		return true;
	};
	let lease: { peer: Peer; number: number; expires: number } | undefined;
	let fencing = 0;
	let sequence = 0;
	let ready = false;
	let runId: string | null = session.isRunActive ? randomUUID() : null;
	let closed = false;
	let closePromise: Promise<void> | undefined;

	const release = (peer: Peer) => {
		if (lease?.peer === peer) lease = undefined;
		peer.synced = false;
	};
	const currentLease = (peer: Peer, number: number): boolean => {
		if (lease && lease.expires <= Date.now()) release(lease.peer);
		return lease?.peer === peer && lease.number === number;
	};
	const send = (peer: Peer, frame: object): void => {
		if (peer.socket.destroyed) return;
		try {
			const bytes = encodeRemoteFrame(frame, peer.limit);
			if (
				peer.queuedFrames >= REMOTE_MAX_QUEUE_FRAMES ||
				peer.socket.writableLength + bytes.byteLength > REMOTE_MAX_QUEUE_BYTES
			) {
				peer.socket.destroy();
				return;
			}
			peer.queuedFrames++;
			peer.socket.write(bytes, () => {
				peer.queuedFrames--;
			});
		} catch {
			peer.socket.destroy();
		}
	};
	const fail = (peer: Peer, id: string, code: RemoteErrorCode) =>
		send(peer, { type: "reply", id, ok: false, error: { code } });
	const reply = (peer: Peer, id: string, data: unknown) => send(peer, { type: "reply", id, ok: true, data });
	const handle = (peer: Peer, value: unknown) => {
		const frame = parseRemoteClientFrame(value);
		if (!frame) {
			peer.socket.destroy();
			return;
		}
		if (!peer.hello) {
			if (frame.type !== "hello") {
				peer.socket.destroy();
				return;
			}
			if (frame.instanceKey !== registration.instanceKey || frame.workspace !== workspace) {
				fail(peer, frame.id, "identity_mismatch");
				peer.socket.end();
				return;
			}
			if (
				!frame.versions.includes(REMOTE_PROTOCOL_VERSION) ||
				!REMOTE_REQUIRED_CAPABILITIES.every(cap => frame.capabilities.includes(cap))
			) {
				fail(peer, frame.id, "protocol_incompatible");
				peer.socket.end();
				return;
			}
			peer.limit = Math.min(frame.maxFrameBytes, REMOTE_MAX_FRAME_BYTES);
			peer.hello = true;
			reply(peer, frame.id, {
				version: REMOTE_PROTOCOL_VERSION,
				instanceKey: registration.instanceKey,
				workspace,
				epoch: registration.epoch,
				capabilities: REMOTE_REQUIRED_CAPABILITIES,
				maxFrameBytes: peer.limit,
			});
			peer.decoder = new RemoteFrameDecoder(peer.limit);
			return;
		}
		if (frame.type !== "request") {
			peer.socket.destroy();
			return;
		}
		handleRequest(peer, frame);
	};
	const handleRequest = (peer: Peer, request: RemoteRequest) => {
		const { id } = request;
		switch (request.op) {
			case "acquire": {
				if (lease && lease.expires <= Date.now()) release(lease.peer);
				if (lease) {
					fail(peer, id, "busy");
					return;
				}
				lease = { peer, number: ++fencing, expires: Date.now() + REMOTE_LEASE_TIMEOUT_MS };
				reply(peer, id, { lease: lease.number });
				return;
			}
			case "heartbeat":
				if (!currentLease(peer, request.data.lease)) {
					fail(peer, id, "stale_lease");
					return;
				}
				if (lease) lease.expires = Date.now() + REMOTE_LEASE_TIMEOUT_MS;
				reply(peer, id, {});
				return;
			case "sync": {
				if (!currentLease(peer, request.data.lease)) {
					fail(peer, id, "stale_lease");
					return;
				}
				const sessionId = session.sessionManager.getSessionId() || null;
				if (request.data.resumeSessionId && request.data.resumeSessionId !== sessionId) {
					fail(peer, id, "unsupported");
					return;
				}
				peer.synced = true;
				reply(peer, id, {
					epoch: registration.epoch,
					seq: sequence,
					view: randomUUID(),
					sessionId,
					cwd: workspace,
					phase: !ready ? "starting" : approvals.size ? "waiting" : session.isRunActive ? "running" : "idle",
					runId,
					partialReply: null,
					currentTools: [],
					pendingApprovals: [...approvals.values()],
					historyCursor: null,
				});
				return;
			}
			case "action": {
				const action = request.data;
				if (!currentLease(peer, action.lease)) {
					fail(peer, id, "stale_lease");
					return;
				}
				if (action.epoch !== registration.epoch) {
					fail(peer, id, "stale_epoch");
					return;
				}
				if (action.sessionId !== session.sessionManager.getSessionId()) {
					fail(peer, id, "stale_session");
					return;
				}
				const command = action.command;
				if (command.type === "respond") {
					if (!approvals.has(command.requestId)) {
						fail(peer, id, "unknown_request");
						return;
					}
					const response = command.response as Record<string, unknown>;
					if (!sendInput({ ...response, type: "extension_ui_response", id: command.requestId })) {
						fail(peer, id, "overflow");
						return;
					}
					approvals.delete(command.requestId);
					reply(peer, id, { status: "succeeded", data: null });
					return;
				}
				if (!peer.synced || !ready) {
					fail(peer, id, "invalid_request");
					return;
				}
				if (command.type !== "prompt" && command.type !== "stop") {
					fail(peer, id, "unsupported");
					return;
				}
				if (command.type === "stop" && (!runId || command.runId !== runId || !session.isRunActive)) {
					fail(peer, id, "stale_run");
					return;
				}
				if (pending.size >= REMOTE_MAX_QUEUE_FRAMES) {
					fail(peer, id, "overflow");
					return;
				}
				const innerId = randomUUID();
				const inner =
					command.type === "prompt"
						? { id: innerId, type: "prompt", message: command.message }
						: { id: innerId, type: "abort" };
				if (!sendInput(inner)) {
					fail(peer, id, "overflow");
					return;
				}
				pending.set(innerId, { peer, requestId: id });
				return;
			}
			default:
				fail(peer, id, "unsupported");
		}
	};
	const server = net.createServer(socket => {
		const peer: Peer = {
			socket,
			decoder: new RemoteFrameDecoder(),
			limit: REMOTE_MAX_FRAME_BYTES,
			hello: false,
			synced: false,
			queuedFrames: 0,
		};
		peers.add(peer);
		socket.on("error", () => {});
		socket.on("end", () => release(peer));
		socket.on("close", () => {
			release(peer);
			peers.delete(peer);
			for (const [id, command] of pending) if (command.peer === peer) pending.delete(id);
		});
		socket.on("data", chunk => {
			try {
				// Decode one line at a time so the hello limit applies to the very next frame,
				// even if both arrive in the same network packet.
				let start = 0;
				for (let i = 0; i < chunk.length; i++) {
					if (chunk[i] !== 10) continue;
					for (const frame of peer.decoder.push(chunk.subarray(start, i + 1))) handle(peer, frame);
					start = i + 1;
					if (socket.destroyed || socket.writableEnded) return;
				}
				if (start < chunk.length) peer.decoder.push(chunk.subarray(start));
			} catch {
				socket.destroy();
			}
		});
	});
	try {
		const listening = Promise.withResolvers<void>();
		server.once("error", listening.reject);
		server.listen(registration.socketPath, () => {
			server.off("error", listening.reject);
			listening.resolve();
		});
		await listening.promise;
		await fs.chmod(registration.socketPath, 0o600);
		await owner.publish();
	} catch (error) {
		server.close();
		await owner.dispose();
		throw error;
	}
	const onInt = () => {
		void close();
	};
	const onTerm = () => {
		void close();
	};
	process.once("SIGINT", onInt);
	process.once("SIGTERM", onTerm);
	function close(): Promise<void> {
		if (closePromise) return closePromise;
		closed = true;
		process.off("SIGINT", onInt);
		process.off("SIGTERM", onTerm);
		for (const peer of peers) peer.socket.destroy();
		if (!inputClosed) {
			inputClosed = true;
			controller.close();
		}
		const done = Promise.withResolvers<void>();
		server.close(error => (error ? done.reject(error) : done.resolve()));
		closePromise = done.promise.finally(() => owner.dispose());
		return closePromise;
	}
	function output(frame: object): void {
		if (closed) return;
		const value = frame as Record<string, unknown>;
		if (value.type === "ready") {
			ready = true;
			return;
		}
		if (value.type === "response" && typeof value.id === "string") {
			const waiting = pending.get(value.id);
			if (waiting) {
				pending.delete(value.id);
				reply(
					waiting.peer,
					waiting.requestId,
					value.success
						? { status: "succeeded", data: value.data ?? null }
						: { status: "rejected", error: { code: "internal_error" } },
				);
			}
			return;
		}
		if (value.type === "extension_ui_request") {
			if (value.method === "cancel" && typeof value.targetId === "string") approvals.delete(value.targetId);
			else if (
				typeof value.id === "string" &&
				typeof value.method === "string" &&
				["select", "confirm", "input", "editor", "open_url", "ask", "form"].includes(value.method)
			)
				approvals.set(value.id, { ...value, requestId: value.id });
		}
		if (value.type === "agent_start") runId = randomUUID();
		else if (value.type === "agent_end") runId = null;
		const sessionId = session.sessionManager.getSessionId();
		if (!sessionId) return;
		sequence++;
		for (const peer of peers)
			if (peer.synced && lease?.peer === peer && currentLease(peer, lease.number))
				send(peer, { type: "event", epoch: registration.epoch, seq: sequence, sessionId, event: frame });
	}
	return { input, output, close };
}
