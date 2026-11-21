import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import type { ReadableStreamDefaultReader } from "node:stream/web";
import type { AgentSession } from "../src/session/agent-session";
import type { SessionInfo } from "../src/session/session-listing";
import { startRemoteRpcServer, type RemoteRpcInstance, type RemoteServerOptions } from "../src/modes/rpc/remote-server";
import { createWorkspaceSessionCatalog } from "../src/modes/rpc/remote-sessions";
import {
	encodeRemoteFrame,
	RemoteFrameDecoder,
	REMOTE_LEASE_TIMEOUT_MS,
	REMOTE_MAX_FRAME_BYTES,
	REMOTE_REQUIRED_CAPABILITIES,
} from "../src/modes/rpc/remote-wire";

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
	while (cleanup.length) await cleanup.pop()?.();
});

interface TestClient {
	socket: net.Socket;
	send(frame: object): void;
	next(): Promise<Record<string, unknown>>;
	/** Send `frame` and wait for the matching reply; intervening events are discarded. */
	ask(id: string, frame: object): Promise<Record<string, unknown>>;
	/** Send `frame` and wait for the matching reply, returning skipped frames in order. */
	collect(id: string, frame: object): Promise<{ reply: Record<string, unknown>; events: Record<string, unknown>[] }>;
}

async function connect(socketPath: string): Promise<TestClient> {
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
	const client: TestClient = {
		socket,
		send(frame: object) {
			socket.write(encodeRemoteFrame(frame));
		},
		async next() {
			const frame = frames.shift();
			if (frame) return frame;
			const deferred = Promise.withResolvers<Record<string, unknown>>();
			waiter = deferred.resolve;
			return deferred.promise;
		},
		async ask(id, frame) {
			const { reply } = await client.collect(id, frame);
			return reply;
		},
		async collect(id, frame) {
			this.send(frame);
			const events: Record<string, unknown>[] = [];
			for (;;) {
				const next = await client.next();
				if (next.type === "reply" && next.id === id) return { reply: next, events };
				events.push(next);
			}
		},
	};
	return client;
}

/**
 * Negative delivery check on real socket I/O. "No frame arrives" cannot be proven
 * with fake timers — only the real transport can fail to produce bytes — so this
 * waits one short (30 ms) real window. All lease/receipt timing in these tests is
 * deterministic through the injected clock; nothing here waits on protocol timers.
 */
async function staysIdle(read: Promise<unknown>): Promise<void> {
	const idle = Promise.withResolvers<"idle">();
	setTimeout(() => idle.resolve("idle"), 30);
	const winner = await Promise.race([read.then(() => "frame" as const), idle.promise]);
	expect(winner).toBe("idle");
}

async function decode(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<Record<string, unknown>> {
	const frame = await reader.read();
	if (frame.done) throw new Error("remote input closed");
	return JSON.parse(new TextDecoder().decode(frame.value)) as Record<string, unknown>;
}

interface Fixture {
	server: RemoteRpcInstance;
	state: { sessionId: string | null; messages: Record<string, unknown>[] };
	reader: ReadableStreamDefaultReader<Uint8Array>;
	hello: object;
	epoch: () => string;
	socketPath: string;
	advance: (ms: number) => void;
}

async function serve(remote?: RemoteServerOptions): Promise<Fixture> {
	const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-server-"));
	cleanup.push(() => fs.rm(directory, { recursive: true, force: true }));
	const workspace = await fs.realpath(directory);
	const name = `test-${randomUUID()}`;
	let time = 1_000_000;
	const state = { sessionId: "session-1", messages: [] as Record<string, unknown>[] };
	const session = {
		sessionManager: { getCwd: () => workspace, getSessionId: () => state.sessionId },
		isRunActive: false,
		messages: state.messages,
	} as unknown as AgentSession;
	const server = await startRemoteRpcServer(name, session, { now: () => time, ...remote });
	cleanup.push(() => server.close());
	const key = new Bun.CryptoHasher("sha256").update(JSON.stringify([name, workspace])).digest("hex");
	const record = (await Bun.file(`/tmp/omp-${process.getuid?.()}/${key}/registration`).json()) as {
		socketPath: string;
		epoch: string;
	};
	return {
		server,
		state,
		reader: server.input.getReader(),
		hello: {
			type: "hello",
			id: randomUUID(),
			versions: [1],
			instanceKey: key,
			workspace,
			capabilities: REMOTE_REQUIRED_CAPABILITIES,
			maxFrameBytes: REMOTE_MAX_FRAME_BYTES,
		},
		epoch: () => record.epoch,
		socketPath: record.socketPath,
		advance: ms => {
			time += ms;
		},
	};
}

/** hello only — for peers that must NOT acquire yet (busy-path checks). */
async function hello(fixture: Fixture, tag: string): Promise<TestClient> {
	const client = await connect(fixture.socketPath);
	expect((await client.ask(`hello-${tag}`, { ...fixture.hello, id: `hello-${tag}` })).ok).toBe(true);
	return client;
}

/** hello + successful acquire; the instance hands out one lease per generation. */
async function attach(fixture: Fixture, lease: number): Promise<TestClient> {
	const client = await hello(fixture, `acquire-${lease}`);
	const acquired = await client.ask(`acquire-${lease}a`, {
		type: "request",
		id: `acquire-${lease}a`,
		op: "acquire",
		data: {},
	});
	expect(acquired.data).toEqual({ lease });
	return client;
}

function action(
	id: string,
	fixture: Fixture,
	lease: number,
	actionId: string,
	command: object,
	sessionId = "session-1",
): object {
	return {
		type: "request",
		id,
		op: "action",
		data: { lease, epoch: fixture.epoch(), sessionId, actionId, command },
	};
}

function receiptQuery(id: string, fixture: Fixture, actionId: string, epoch = fixture.epoch()): object {
	return { type: "request", id, op: "receipt", data: { epoch, sessionId: "session-1", actionId } };
}

function sessionInfo(overrides: Partial<SessionInfo> & Pick<SessionInfo, "id" | "path">): SessionInfo {
	return {
		cwd: "/unset",
		created: new Date("2026-01-01T00:00:00Z"),
		modified: new Date("2026-01-02T00:00:00Z"),
		messageCount: 1,
		size: 10,
		firstMessage: "hello",
		allMessagesText: "hello",
		...overrides,
	};
}

describe("remote RPC instance", () => {
	test("approval outlives attaches, is consumed exactly once, and cancel kills it", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "extension_ui_request", id: "approval-1", method: "confirm", title: "startup" });
		const first = await attach(fixture, 1);
		const dropped = once(first.socket, "close");
		first.socket.destroy();
		await dropped;

		const second = await attach(fixture, 2);
		// The ACK carries the terminal result because answering settled inside the request.
		const answered = await second.ask(
			"answer",
			action("answer", fixture, 2, "answer-1", {
				type: "respond",
				requestId: "approval-1",
				response: { confirmed: true },
			}),
		);
		expect(answered.data).toEqual({ status: "succeeded", data: null });
		expect(await decode(fixture.reader)).toEqual({
			type: "extension_ui_response",
			id: "approval-1",
			confirmed: true,
		});

		// The same action id replays its receipt and must not reach the resolver again.
		const replay = await second.ask(
			"answer-replay",
			action("answer-replay", fixture, 2, "answer-1", {
				type: "respond",
				requestId: "approval-1",
				response: { confirmed: true },
			}),
		);
		expect(replay.data).toEqual({ status: "succeeded", data: null });
		// A different action id on a consumed request cannot affect another approval.
		const late = await second.ask(
			"answer-late",
			action("answer-late", fixture, 2, "answer-2", {
				type: "respond",
				requestId: "approval-1",
				response: { confirmed: false },
			}),
		);
		expect(late.error).toEqual({ code: "unknown_request" });
		const wrongSession = await second.ask(
			"answer-wrong",
			action(
				"answer-wrong",
				fixture,
				2,
				"answer-3",
				{ type: "respond", requestId: "approval-1", response: {} },
				"session-9",
			),
		);
		expect(wrongSession.error).toEqual({ code: "stale_session" });
		await staysIdle(fixture.reader.read());

		// Pending dialogs ride the snapshot of the leased connection, keyed by session.
		fixture.server.output({ type: "ready" });
		fixture.server.output({ type: "extension_ui_request", id: "approval-2", method: "input", title: "tool" });
		const snapshot = (await second.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 2 } }))
			.data as Record<string, unknown>;
		expect(snapshot.phase).toBe("waiting");
		expect(snapshot.pendingApprovals).toEqual([
			{
				type: "extension_ui_request",
				id: "approval-2",
				method: "input",
				title: "tool",
				requestId: "approval-2",
				sessionId: "session-1",
			},
		]);
		// A timeout/abort closes the dialog instance-wide: late answers are refused.
		fixture.server.output({
			type: "extension_ui_request",
			id: randomUUID(),
			method: "cancel",
			targetId: "approval-2",
		});
		const cancelled = await second.ask(
			"answer-cancelled",
			action("answer-cancelled", fixture, 2, "answer-4", { type: "respond", requestId: "approval-2", response: {} }),
		);
		expect(cancelled.error).toEqual({ code: "unknown_request" });
		second.socket.destroy();
	});

	test("prompt ACK is accepted, not finished; the receipt survives the lost reply", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		const first = await attach(fixture, 1);
		await first.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } });

		first.send(action("prompt-ack", fixture, 1, "action-ack", { type: "prompt", message: "hello" }));
		// The reply confirms registration, before Core has acknowledged anything.
		expect((await first.next()).data).toEqual({ status: "accepted" });
		const inFlight = await first.ask("receipt-1", receiptQuery("receipt-1", fixture, "action-ack"));
		expect(inFlight.data).toEqual({ status: "accepted" });

		const prompt = await decode(fixture.reader);
		// The client dies with the ACK in flight; Core finishes the work unseen.
		const dropped = once(first.socket, "close");
		first.socket.destroy();
		await dropped;
		fixture.server.output({ type: "response", id: prompt.id as string, command: "prompt", success: true });

		const second = await attach(fixture, 2);
		const settled = await second.ask("receipt-2", receiptQuery("receipt-2", fixture, "action-ack"));
		expect(settled.data).toEqual({ status: "succeeded", data: null });
		// Reconciled as done: repeating the action returns the receipt without a second input.
		const replay = await second.ask(
			"prompt-replay",
			action("prompt-replay", fixture, 2, "action-ack", { type: "prompt", message: "hello" }),
		);
		expect(replay.data).toEqual({ status: "succeeded", data: null });
		await staysIdle(fixture.reader.read());
		second.socket.destroy();
	});

	test("a restart starts a new epoch: old receipts read unknown, nothing replays", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		const before = await attach(fixture, 1);
		await before.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } });
		const oldEpoch = fixture.epoch();
		before.send(action("prompt-old", fixture, 1, "action-old", { type: "prompt", message: "hang" }));
		expect((await before.next()).data).toEqual({ status: "accepted" });
		await decode(fixture.reader);

		await fixture.server.close();
		expect((await fixture.reader.read()).done).toBe(true);
		const restarted = await serve();
		expect(restarted.epoch()).not.toBe(oldEpoch);
		const client = await attach(restarted, 1);
		const wrongEpoch = await client.ask("wrong-epoch", {
			type: "request",
			id: "wrong-epoch",
			op: "action",
			data: {
				lease: 1,
				epoch: oldEpoch,
				sessionId: "session-1",
				actionId: "action-any",
				command: { type: "prompt", message: "x" },
			},
		});
		expect(wrongEpoch.error).toEqual({ code: "stale_epoch" });
		const forgotten = await client.ask("receipt-old", receiptQuery("receipt-old", restarted, "action-old", oldEpoch));
		expect(forgotten.data).toEqual({ status: "unknown" });
		const unseen = await client.ask("receipt-none", receiptQuery("receipt-none", restarted, "never-sent"));
		expect(unseen.data).toEqual({ status: "unknown" });
		client.socket.destroy();
	});

	test("expired lease releases on the injected clock; stale holders cannot preempt or write", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		const holder = await attach(fixture, 1);
		const waiter = await hello(fixture, "waiter");
		expect(
			(await waiter.ask("acquire-wait", { type: "request", id: "acquire-wait", op: "acquire", data: {} })).error,
		).toEqual({ code: "busy" });
		// No sleep: lease expiry is arithmetic on the injected clock.
		fixture.advance(REMOTE_LEASE_TIMEOUT_MS + 1);
		expect(
			(await holder.ask("hb", { type: "request", id: "hb", op: "heartbeat", data: { lease: 1 } })).error,
		).toEqual({ code: "stale_lease" });
		expect(
			(await waiter.ask("acquire-wait2", { type: "request", id: "acquire-wait2", op: "acquire", data: {} })).data,
		).toEqual({ lease: 2 });
		const late = await holder.ask(
			"late",
			action("late", fixture, 1, "action-late", { type: "prompt", message: "late" }),
		);
		expect(late.error).toEqual({ code: "stale_lease" });
		expect(
			(await holder.ask("sync-late", { type: "request", id: "sync-late", op: "sync", data: { lease: 1 } })).error,
		).toEqual({ code: "stale_lease" });
		// The new holder writes normally; the accepted prompt counts exactly one input.
		await waiter.ask("sync-wait", { type: "request", id: "sync-wait", op: "sync", data: { lease: 2 } });
		const fresh = await waiter.ask(
			"fresh",
			action("fresh", fixture, 2, "action-fresh", { type: "prompt", message: "mine" }),
		);
		expect(fresh.data).toEqual({ status: "accepted" });
		expect(await decode(fixture.reader)).toMatchObject({ type: "prompt", message: "mine" });
		await staysIdle(fixture.reader.read());
		holder.socket.destroy();
		waiter.socket.destroy();
	});

	test("stop binds one run: wrong ids refuse; a queued abort dies before a later run", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		const client = await attach(fixture, 1);
		await client.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } });

		fixture.server.output({ type: "agent_start" });
		const running = (await client.ask("sync-2", { type: "request", id: "sync-2", op: "sync", data: { lease: 1 } }))
			.data as { runId: string; phase: string };
		expect(running.phase).toBe("running");
		const bogus = await client.ask(
			"stop-bogus",
			action("stop-bogus", fixture, 1, "stop-bogus", { type: "stop", runId: "run-ghost" }),
		);
		expect(bogus.error).toEqual({ code: "stale_run" });

		// Three frames in one burst with no read in between: stream bookkeeping decides
		// which wait in the instance queue, but the abort must die either way.
		client.send(action("warm", fixture, 1, "warm", { type: "prompt", message: "warm" }));
		expect((await client.next()).data).toEqual({ status: "accepted" });
		const stopRun = running.runId;
		client.send(action("stop-run", fixture, 1, "stop-run", { type: "stop", runId: stopRun }));
		expect((await client.next()).data).toEqual({ status: "accepted" });
		client.send(action("probe", fixture, 1, "probe", { type: "prompt", message: "probe" }));
		expect((await client.next()).data).toEqual({ status: "accepted" });

		// The run finished naturally and a delivery started a newer run before Core
		// consumed the abort.
		fixture.server.output({ type: "agent_end" });
		fixture.server.output({ type: "agent_start" });

		// Core drains the burst: the stale abort must die at delivery, never stopping the
		// newer run, and the frame behind it arrives in its place.
		expect(await decode(fixture.reader)).toMatchObject({ type: "prompt", message: "warm" });
		const delivered = await decode(fixture.reader);
		expect(delivered).toMatchObject({ type: "prompt", message: "probe" });

		// The typed update names the action and its terminal receipt.
		const { reply, events } = await client.collect("receipt-stop", receiptQuery("receipt-stop", fixture, "stop-run"));
		// The burst carries forwarded agent frames too; only the receipt event names
		// this action.
		const receiptEvent = events.find(frame => {
			if (frame.type !== "event") return false;
			const event = frame.event as { type?: string; actionId?: string } | undefined;
			return event?.type === "remote_receipt" && event.actionId === "stop-run";
		})?.event as { type: string; actionId: string; receipt: Record<string, unknown> } | undefined;
		expect(receiptEvent).toEqual({
			type: "remote_receipt",
			actionId: "stop-run",
			receipt: { status: "rejected", error: { code: "stale_run" } },
		});
		expect(reply.data).toEqual({ status: "rejected", error: { code: "stale_run" } });
		const after = (await client.ask("sync-3", { type: "request", id: "sync-3", op: "sync", data: { lease: 1 } }))
			.data as { runId: string; phase: string };
		expect(after.runId).toBeTruthy();
		expect(after.runId).not.toBe(stopRun);
		expect(after.phase).toBe("running");

		// Once that run is over, an id-reusing late stop still refuses to touch anything.
		fixture.server.output({ type: "agent_end" });
		const late = await client.ask(
			"stop-late",
			action("stop-late", fixture, 1, "stop-late", { type: "stop", runId: stopRun }),
		);
		expect(late.error).toEqual({ code: "stale_run" });
		await staysIdle(fixture.reader.read());
		client.socket.destroy();
	});

	test("session switches refuse busy state and the prompt-to-run race, dispatching once idle", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		fixture.server.output({ type: "extension_ui_request", id: "approval-x", method: "confirm", title: "tool" });
		const client = await attach(fixture, 1);
		await client.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } });

		// Burst with no read in between: an approval answer (which admits no run), a switch
		// that legitimately looks idle at accept time, and a probe prompt behind the switch.
		const answered = await client.ask(
			"answer-x",
			action("answer-x", fixture, 1, "answer-x", {
				type: "respond",
				requestId: "approval-x",
				response: { confirmed: true },
			}),
		);
		expect(answered.data).toEqual({ status: "succeeded", data: null });
		client.send(action("race-new", fixture, 1, "race-new", { type: "new_session" }));
		expect((await client.next()).data).toEqual({ status: "accepted" });
		client.send(action("probe", fixture, 1, "probe", { type: "prompt", message: "probe" }));
		expect((await client.next()).data).toEqual({ status: "accepted" });
		// A admitted prompt's run materializes before Core drains the switch: the switch
		// must lose the race, and the probe arriving behind it proves that exactly.
		fixture.server.output({ type: "agent_start" });

		expect(await decode(fixture.reader)).toEqual({
			type: "extension_ui_response",
			id: "approval-x",
			confirmed: true,
		});
		const delivered = await decode(fixture.reader);
		expect(delivered).toMatchObject({ type: "prompt", message: "probe" });

		const refused = await client.ask("receipt-new", receiptQuery("receipt-new", fixture, "race-new"));
		expect(refused.data).toEqual({ status: "rejected", error: { code: "invalid_request" } });

		// A directly attempted switch under a live run refuses at the door, input untouched.
		const busy = await client.ask(
			"busy-new",
			action("busy-new", fixture, 1, "busy-new", { type: "open_session", sessionId: "ses-1" }),
		);
		expect(busy.error).toEqual({ code: "invalid_request" });

		fixture.server.output({ type: "agent_end" });
		const idle = await client.ask("idle-new", action("idle-new", fixture, 1, "idle-new", { type: "new_session" }));
		expect(idle.data).toEqual({ status: "accepted" });
		expect(await decode(fixture.reader)).toMatchObject({ type: "new_session" });

		// The session id changed underneath the client: old-scoped actions must not act.
		fixture.state.sessionId = "session-2";
		const stale = await client.ask(
			"stale-session",
			action("stale-session", fixture, 1, "stale-session", { type: "prompt", message: "old" }),
		);
		expect(stale.error).toEqual({ code: "stale_session" });
		await staysIdle(fixture.reader.read());
		client.socket.destroy();
	});

	test("sessions list the bound workspace only; open maps opaque ids, never paths", async () => {
		const workspace = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-same-")));
		const foreign = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-other-")));
		const pinned = path.join(workspace, "sessions");
		const catalog = createWorkspaceSessionCatalog(workspace, {
			sessionDir: pinned,
			listStored: async () => [
				sessionInfo({ id: "ses-a", cwd: workspace, path: path.join(pinned, "a.jsonl"), title: "Alpha" }),
				sessionInfo({ id: "ses-b", cwd: foreign, path: path.join(pinned, "b.jsonl"), title: "Foreign" }),
				sessionInfo({ id: "ses-c", cwd: "", path: path.join(pinned, "c.jsonl") }),
				sessionInfo({ id: "ses-d", cwd: workspace, path: path.join(workspace, "outside.jsonl") }),
			],
		});
		const fixture = await serve({ sessions: catalog });
		fixture.server.output({ type: "ready" });
		const client = await attach(fixture, 1);
		await client.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } });

		const page = await client.ask("sessions-1", { type: "request", id: "sessions-1", op: "sessions", data: {} });
		// Metadata only: no path, and only header-verified workspace sessions.
		expect(page.data).toEqual({
			sessions: [{ sessionId: "ses-a", name: "Alpha", updatedAt: "2026-01-02T00:00:00.000Z" }],
			cursor: null,
		});

		// Cross-workspace, escaping and unknown ids refuse with input untouched: any leaked
		// dispatch would surface ahead of the next real frame, caught by its exact shape.
		const foreignOpen = await client.ask(
			"open-b",
			action("open-b", fixture, 1, "open-b", { type: "open_session", sessionId: "ses-b" }),
		);
		expect(foreignOpen.error).toEqual({ code: "invalid_request" });
		const escape = await client.ask(
			"open-d",
			action("open-d", fixture, 1, "open-d", { type: "open_session", sessionId: "ses-d" }),
		);
		expect(escape.error).toEqual({ code: "invalid_request" });
		const ghost = await client.ask(
			"open-x",
			action("open-x", fixture, 1, "open-x", { type: "open_session", sessionId: "ses-x" }),
		);
		expect(ghost.error).toEqual({ code: "invalid_request" });

		const opened = await client.ask(
			"open-a",
			action("open-a", fixture, 1, "open-a", { type: "open_session", sessionId: "ses-a" }),
		);
		expect(opened.data).toEqual({ status: "accepted" });
		// The opaque id resolved inside the pinned dir; the path never came from the client.
		expect(await decode(fixture.reader)).toMatchObject({
			type: "switch_session",
			sessionPath: path.join(pinned, "a.jsonl"),
		});
		await staysIdle(fixture.reader.read());
		client.socket.destroy();
	});

	test.each(["SIGINT", "SIGTERM"] as const)("closes input on %s without resending the signal", async signal => {
		const directory = await fs.mkdtemp(path.join(os.tmpdir(), "omp-remote-server-"));
		cleanup.push(() => fs.rm(directory, { recursive: true, force: true }));
		const workspace = await fs.realpath(directory);
		const session = {
			sessionManager: { getCwd: () => workspace, getSessionId: () => "session-1" },
			isRunActive: false,
			messages: [],
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
		const fixture = await serve();
		const wrong = await connect(fixture.socketPath);
		wrong.send({ ...fixture.hello, id: "hello-wrong", workspace: "/wrong" });
		expect((await wrong.next()).error).toEqual({ code: "identity_mismatch" });
		const first = await attach(fixture, 1);
		const second = await hello(fixture, "preempt");
		expect(
			(await second.ask("acquire-2", { type: "request", id: "acquire-2", op: "acquire", data: {} })).error,
		).toEqual({ code: "busy" });
		first.socket.destroy();
		second.socket.destroy();
	});

	test("sync captures one view: history pages the captured transcript while live events number past the watermark", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		const captured: Record<string, unknown>[] = Array.from({ length: 200 }, (_, index) => ({
			role: "user",
			content: `m${index}`,
		}));
		// A single tool result larger than any servable page.
		captured[5] = {
			role: "toolResult",
			toolCallId: "t-huge",
			toolName: "bash",
			isError: false,
			timestamp: 1,
			content: [{ type: "text", text: "x".repeat(400_000) }],
		};
		fixture.state.messages.push(...captured);
		const client = await attach(fixture, 1);
		const snapshot = (await client.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } }))
			.data as { view: string; seq: number; historyCursor: unknown; partialReply: unknown; currentTools: unknown[] };
		expect(typeof snapshot.view).toBe("string");
		expect(snapshot.historyCursor).toBeNull();
		expect(snapshot.partialReply).toBeNull();
		expect(snapshot.currentTools).toEqual([]);

		// A live delta issued after the reply is numbered past the watermark.
		fixture.server.output({ type: "text_delta", delta: "later" });
		expect(await client.next()).toMatchObject({
			type: "event",
			seq: snapshot.seq + 1,
			event: { type: "text_delta" },
		});

		// The captured transcript pages through with opaque cursors: more than one
		// page, chained to the end, with the giant tool result as an explicit
		// truncated fragment (no archive record exists, so no output reference).
		const view = snapshot.view;
		const pages: { asked: string | undefined; items: Record<string, unknown>[]; next: string | undefined }[] = [];
		for (;;) {
			const asked = pages.length === 0 ? undefined : pages[pages.length - 1]!.next;
			const data = (
				await client.ask(`history-${pages.length}`, {
					type: "request",
					id: `history-${pages.length}`,
					op: "history",
					data: asked === undefined ? { view } : { view, cursor: asked },
				})
			).data as { items: Record<string, unknown>[]; cursor: string | null };
			expect(data.items.length).toBeGreaterThan(0);
			pages.push({ asked, items: data.items, next: data.cursor ?? undefined });
			if (data.cursor === null) break;
		}
		expect(pages.length).toBeGreaterThan(1);
		const flat = pages.flatMap(page => page.items);
		expect(flat).toHaveLength(200);
		const huge = flat[5];
		expect(huge).toMatchObject({ role: "toolResult", toolCallId: "t-huge", truncated: true });
		expect(typeof huge.preview === "string" && huge.preview.length > 0).toBe(true);
		expect(huge.outputId).toBe("history.5");
		const collected: string[] = [];
		let outputCursor: string | null = null;
		do {
			const requestId = `persisted-output-${collected.length}`;
			const page = (
				await client.ask(requestId, {
					type: "request",
					id: requestId,
					op: "output",
					data: { view, outputId: huge.outputId, ...(outputCursor ? { cursor: outputCursor } : {}) },
				})
			).data as { data: string; cursor: string | null; truncated: boolean };
			expect(page.truncated).toBe(false);
			expect(page.data.length).toBeGreaterThan(0);
			collected.push(page.data);
			outputCursor = page.cursor;
		} while (outputCursor && collected.length < 10);
		expect(collected.join("")).toBe("x".repeat(400_000));
		for (const [index, item] of flat.entries()) if (index !== 5) expect(item).toEqual(captured[index]);

		// Appends to the live transcript never shift a captured page.
		const midCursor = pages[0]!.next;
		expect(typeof midCursor).toBe("string");
		const before = (
			await client.ask("history-mid", {
				type: "request",
				id: "history-mid",
				op: "history",
				data: { view, cursor: midCursor },
			})
		).data;
		fixture.state.messages.push({ role: "user", content: "late-append" });
		expect(
			(
				await client.ask("history-mid-2", {
					type: "request",
					id: "history-mid-2",
					op: "history",
					data: { view, cursor: midCursor },
				})
			).data,
		).toEqual(before);

		// A second sync supersedes the view; the old id is refused on both page ops.
		const resync = (await client.ask("sync-2", { type: "request", id: "sync-2", op: "sync", data: { lease: 1 } }))
			.data as Record<string, unknown>;
		expect(resync.view).not.toBe(view);
		expect(
			(await client.ask("history-old", { type: "request", id: "history-old", op: "history", data: { view } })).error,
		).toEqual({ code: "stale_session" });
		expect(
			(
				await client.ask("output-old", {
					type: "request",
					id: "output-old",
					op: "output",
					data: { view, outputId: "t-huge" },
				})
			).error,
		).toEqual({ code: "stale_session" });
		client.socket.destroy();
	});

	test("output recorded while detached stays pageable; partial reply rides the snapshot; session change retires the view", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		// No client has ever attached: the instance still records the live output.
		fixture.server.output({
			type: "tool_execution_start",
			toolCallId: "tool-1",
			toolName: "bash",
			args: { command: "make" },
		});
		fixture.server.output({
			type: "tool_execution_update",
			toolCallId: "tool-1",
			toolName: "bash",
			partialResult: "built-while-detached",
		});
		fixture.server.output({
			type: "tool_execution_end",
			toolCallId: "tool-1",
			toolName: "bash",
			result: "built-while-detached",
			isError: false,
		});

		const client = await attach(fixture, 1);
		const snap1 = (await client.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } }))
			.data as { view: string; currentTools: { toolCallId: string; outputId: string }[] };
		// The result finished without a client and is addressable from the snapshot.
		expect(snap1.currentTools).toMatchObject([{ toolCallId: "tool-1", toolName: "bash", done: true }]);
		const outputId = snap1.currentTools[0]!.outputId;
		const page = (
			await client.ask("output-1", {
				type: "request",
				id: "output-1",
				op: "output",
				data: { view: snap1.view, outputId },
			})
		).data as Record<string, unknown>;
		expect(page).toMatchObject({ view: snap1.view, outputId, data: "built-while-detached", truncated: false });
		expect(page.cursor).toBeNull();

		// The partial reply carries the newest full assistant message only.
		fixture.server.output({ type: "message_start", messageId: "m1", message: { role: "assistant", content: [] } });
		fixture.server.output({
			type: "message_update",
			messageId: "m1",
			message: { role: "assistant", content: [{ type: "text", text: "half-written" }] },
		});
		const snap2 = (await client.ask("sync-2", { type: "request", id: "sync-2", op: "sync", data: { lease: 1 } }))
			.data as Record<string, unknown>;
		expect(snap2.partialReply).toEqual({
			messageId: "m1",
			message: { role: "assistant", content: [{ type: "text", text: "half-written" }] },
		});
		// The superseded view dies the moment its sync is replaced.
		expect(
			(
				await client.ask("history-stale", {
					type: "request",
					id: "history-stale",
					op: "history",
					data: { view: snap1.view },
				})
			).error,
		).toEqual({ code: "stale_session" });

		// A finished message leaves no partial behind.
		fixture.server.output({
			type: "message_end",
			messageId: "m1",
			message: { role: "assistant", content: [{ type: "text", text: "half-written" }] },
		});
		const snap3 = (await client.ask("sync-3", { type: "request", id: "sync-3", op: "sync", data: { lease: 1 } }))
			.data as Record<string, unknown>;
		expect(snap3.partialReply).toBeNull();

		// A session identity change resets the archive and retires old-session views.
		fixture.state.sessionId = "session-2";
		fixture.server.output({ type: "turn_start" });
		const snap4 = (await client.ask("sync-4", { type: "request", id: "sync-4", op: "sync", data: { lease: 1 } }))
			.data as Record<string, unknown>;
		expect(snap4.sessionId).toBe("session-2");
		expect(snap4.currentTools).toEqual([]);
		expect(
			(
				await client.ask("history-old-session", {
					type: "request",
					id: "history-old-session",
					op: "history",
					data: { view: snap3.view },
				})
			).error,
		).toEqual({ code: "stale_session" });
		// The fresh view is live but reads only the new session's (empty) archive.
		expect(
			(
				await client.ask("output-old-session", {
					type: "request",
					id: "output-old-session",
					op: "output",
					data: { view: snap4.view, outputId },
				})
			).error,
		).toEqual({ code: "invalid_request" });
		client.socket.destroy();
	});

	test("ready lands in order; oversized live frames degrade to explicit previews; agent events carry the run", async () => {
		const fixture = await serve();
		const client = await attach(fixture, 1);
		const snapshot = (await client.ask("sync-1", { type: "request", id: "sync-1", op: "sync", data: { lease: 1 } }))
			.data as { seq: number };

		// The ready marker takes a sequence slot ahead of later deltas.
		fixture.server.output({ type: "ready" });
		fixture.server.output({ type: "text_delta", delta: "after-ready" });
		expect(await client.next()).toMatchObject({
			type: "event",
			seq: snapshot.seq + 1,
			event: { type: "remote_ready" },
		});
		expect(await client.next()).toMatchObject({
			type: "event",
			seq: snapshot.seq + 2,
			event: { type: "text_delta" },
		});

		// A frame larger than the negotiated limit cannot evict the attach: the
		// client receives an explicit truncated preview and stays connected.
		fixture.server.output({
			type: "message_end",
			messageId: "m-big",
			message: { role: "assistant", content: [{ type: "text", text: "y".repeat(2 * 1024 * 1024) }] },
		});
		const big = await client.next();
		const preview = big.event as Record<string, unknown>;
		expect(preview).toMatchObject({ type: "message_end", messageId: "m-big", truncated: true });
		expect(typeof preview.preview === "string" && preview.preview.length > 0).toBe(true);
		expect(client.socket.destroyed).toBe(false);

		// Runs are named on the events so the GUI can bind state to the right one.
		fixture.server.output({ type: "agent_start" });
		const started = await client.next();
		const startedEvent = started.event as { type: string; runId: string };
		expect(startedEvent.type).toBe("agent_start");
		const runId = startedEvent.runId;
		expect(runId).toBeTruthy();
		fixture.server.output({ type: "agent_end", messages: [] });
		expect(await client.next()).toMatchObject({ type: "event", event: { type: "agent_end", runId } });
		const after = (await client.ask("sync-2", { type: "request", id: "sync-2", op: "sync", data: { lease: 1 } }))
			.data as Record<string, unknown>;
		expect(after).toMatchObject({ phase: "idle", runId: null });
		client.socket.destroy();
	});

	test("a stalled reader overruns its queue without dropping the instance or its run", async () => {
		const fixture = await serve();
		fixture.server.output({ type: "ready" });
		const slow = await attach(fixture, 1);
		await slow.ask("slow-sync", { type: "request", id: "slow-sync", op: "sync", data: { lease: 1 } });
		const closed = once(slow.socket, "close");
		slow.socket.pause();
		fixture.server.output({ type: "agent_start" });
		const text = "x".repeat(512 * 1024);
		for (let index = 0; index < 40; index++) fixture.server.output({ type: "text_delta", text });
		slow.socket.resume();
		await closed;
		const next = await attach(fixture, 2);
		expect(
			(await next.ask("next-sync", { type: "request", id: "next-sync", op: "sync", data: { lease: 2 } })).data,
		).toMatchObject({ phase: "running", epoch: fixture.epoch(), sessionId: "session-1" });
		next.socket.destroy();
	});
});
