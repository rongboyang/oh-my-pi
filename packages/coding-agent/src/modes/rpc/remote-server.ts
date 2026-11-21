import { randomUUID } from "node:crypto";
import * as fs from "node:fs/promises";
import * as net from "node:net";
import type { AgentSession } from "../../session/agent-session";
import { openRemoteDocumentWorkspace } from "./remote-document";
import {
	boundedHistoryItem,
	boundedRemoteEvent,
	RemoteHistoryView,
	type RemoteOutputIdResolver,
} from "./remote-history-view";
import { prepareRemoteRegistration } from "./remote-registration";
import { RemoteReceiptLedger, type RemoteActionScope } from "./remote-receipts";
import { createWorkspaceSessionCatalog, type RemoteSessionCatalog } from "./remote-sessions";
import { RemoteToolOutputs, type RemoteToolOutputView } from "./remote-tool-outputs";
import {
	encodeRemoteFrame,
	parseRemoteClientFrame,
	RemoteFrameDecoder,
	REMOTE_LEASE_TIMEOUT_MS,
	REMOTE_MAX_FRAME_BYTES,
	REMOTE_MAX_OUTPUT_PAGE_BYTES,
	REMOTE_MAX_QUEUE_BYTES,
	REMOTE_MAX_QUEUE_FRAMES,
	REMOTE_PROTOCOL_VERSION,
	REMOTE_REQUIRED_CAPABILITIES,
	type RemoteErrorCode,
	type RemoteReceipt,
	type RemoteRequest,
} from "./remote-wire";

/** Approval dialogs the remote surface may answer; anything else is not consumable. */
const REMOTE_UI_METHODS = ["select", "confirm", "input", "editor", "open_url", "ask", "form"];

interface Peer {
	socket: net.Socket;
	decoder: RemoteFrameDecoder;
	limit: number;
	hello: boolean;
	documents?: boolean;
	synced: boolean;
	queuedFrames: number;
	/** Id of the latest synchronous capture this peer may page with. */
	view?: string;
}

/** One dispatched action awaiting Core's reply; the receipt, not a peer, owns it. */
interface PendingAction {
	readonly scope: RemoteActionScope;
	readonly kind: "prompt" | "stop" | "session";
	/** For stops: the run this abort may stop. Core's reply settles the receipt. */
	readonly runId?: string;
}

/** A frame bound for Core's serial input stream, possibly gated at delivery time. */
interface InputEntry {
	readonly bytes: Uint8Array;
	readonly kind?: "stop" | "session";
	readonly abortRunId?: string;
	readonly scope?: RemoteActionScope;
	readonly innerId?: string;
}

/**
 * One synchronous capture, keyed by the archive's view id. Pages may only be
 * read by the peer that received it, on the lease number and session it was
 * bound to, so an old view can never cross into a newer sync, lease or session.
 */
interface SyncedView {
	readonly id: string;
	readonly owner: Peer;
	readonly lease: number;
	readonly sessionId: string | null;
	/** Watermark in the sync reply: every delta sent afterwards carries `seq` strictly greater. */
	readonly seq: number;
	/** Transcript reference taken at sync; live appends never shift served pages. */
	readonly history: RemoteHistoryView;
	/** Detached tool-output capture pageable under `id`. */
	readonly outputs: RemoteToolOutputView;
}

/** Page budget: output-page cap shrunk to the negotiated frame limit, with envelope slack. */
const pageBudget = (peer: Peer): number => Math.max(1024, Math.min(REMOTE_MAX_OUTPUT_PAGE_BYTES, peer.limit) - 1024);

/** An archive reference exists only for outputs this capture actually retains under its view. */
const archiveReference =
	(capture: RemoteToolOutputView): RemoteOutputIdResolver =>
	toolCallId =>
		capture.currentTools.some(tool => tool.toolCallId === toolCallId) ? toolCallId : undefined;

export interface RemoteServerOptions {
	/** Injectable clock (lease expiry, receipt retention); defaults to `Date.now`. */
	now?: () => number;
	/** Core-owned session lookup for the bound workspace. */
	sessions?: RemoteSessionCatalog;
}

/** The instance-owned remote transport: Core's RPC stream plus lifecycle control. */
export interface RemoteRpcInstance {
	input: ReadableStream<Uint8Array>;
	output(frame: object): void;
	close(): Promise<void>;
	/**
	 * Delivery-side fence for Core's abort handler: true only while `commandId` names a
	 * registered stop still aimed at the active run of the session it was admitted for.
	 */
	mayAbort(commandId: string | undefined): boolean;
}

/** Owns the RPC stream for the lifetime of the instance, not an attach. */
export async function startRemoteRpcServer(
	name: string,
	session: AgentSession,
	options: RemoteServerOptions = {},
): Promise<RemoteRpcInstance> {
	const owner = await prepareRemoteRegistration(name, session.sessionManager.getCwd());
	const { registration } = owner;
	const { workspace } = registration;
	const documents = openRemoteDocumentWorkspace(workspace);
	if (documents) registration.capabilities = [...REMOTE_REQUIRED_CAPABILITIES, "read_document"];
	const now = options.now ?? Date.now;
	const ledger = new RemoteReceiptLedger({ now });
	const catalog = options.sessions ?? createWorkspaceSessionCatalog(workspace);
	const peers = new Set<Peer>();
	const pending = new Map<string, PendingAction>();
	// Approvals belong to the instance and to the session that raised them:
	// disconnecting neither answers nor cancels them, and one request id can
	// never consume another session's pending dialog.
	const approvals = new Map<string, Map<string, Record<string, unknown>>>();
	// Tool output belongs to the session, not to a socket: frames are recorded
	// even while no peer is attached, and only a session identity change resets them.
	const toolOutputs = new RemoteToolOutputs();
	const views = new Map<string, SyncedView>();
	// The newest full assistant snapshot only; the delta log that produced it is never kept.
	let partialReply: { sessionId: string | null; messageId: string; message: unknown } | null = null;
	let scopedSessionId: string | null | undefined;
	let controller: ReadableStreamDefaultController<Uint8Array>;
	let inputBytes = 0;
	const inputQueue: InputEntry[] = [];
	let inputClosed = false;
	let waitingForInput = false;
	// Prompts admitted to Core whose run has not materialized yet (agent_start or
	// failure). Session switches race with them, so they gate like busy state.
	let unresolvedPrompts = 0;
	const input = new ReadableStream<Uint8Array>(
		{
			start(value) {
				controller = value;
			},
			pull(value) {
				if (!deliverOne(value)) waitingForInput = true;
			},
			cancel() {
				inputClosed = true;
				inputQueue.length = 0;
				inputBytes = 0;
			},
		},
		{ highWaterMark: 0 },
	);
	let lease: { peer: Peer; number: number; expires: number } | undefined;
	let fencing = 0;
	let sequence = 0;
	let ready = false;
	let runId: string | null = session.isRunActive ? randomUUID() : null;
	let closed = false;
	let closePromise: Promise<void> | undefined;

	/** A gated frame delivers only while its precondition still holds at the moment Core asks for it. */
	const isCurrent = (entry: InputEntry): boolean => {
		if (entry.kind === "stop") return entry.abortRunId !== undefined && entry.abortRunId === runId;
		if (entry.kind === "session") return runId === null && unresolvedPrompts === 0;
		return true;
	};
	/** The window passed before Core read the frame: settle the receipt, deliver nothing. */
	const discard = (entry: InputEntry): void => {
		if (entry.innerId !== undefined) pending.delete(entry.innerId);
		if (entry.kind === "stop" && entry.abortRunId !== undefined) {
			// The stop must never reach a later run; the target already changed.
			if (entry.scope) settle(entry.scope, { status: "rejected", error: { code: "stale_run" } });
		} else if (entry.scope) {
			settle(entry.scope, { status: "rejected", error: { code: "invalid_request" } });
		}
	};
	const deliverOne = (value: ReadableStreamDefaultController<Uint8Array>): boolean => {
		while (inputQueue.length > 0) {
			const entry = inputQueue.shift()!;
			inputBytes -= entry.bytes.byteLength;
			if (!isCurrent(entry)) {
				discard(entry);
				continue;
			}
			value.enqueue(entry.bytes);
			return true;
		}
		return false;
	};
	const sendInput = (
		frame: object,
		gate?: { kind: "stop" | "session"; abortRunId?: string; scope?: RemoteActionScope; innerId?: string },
	): boolean => {
		if (inputClosed) return false;
		const bytes = encodeRemoteFrame(frame);
		if (inputQueue.length >= REMOTE_MAX_QUEUE_FRAMES || inputBytes + bytes.byteLength > REMOTE_MAX_QUEUE_BYTES)
			return false;
		inputQueue.push({
			bytes,
			kind: gate?.kind,
			abortRunId: gate?.abortRunId,
			scope: gate?.scope,
			innerId: gate?.innerId,
		});
		inputBytes += bytes.byteLength;
		if (waitingForInput) waitingForInput = !deliverOne(controller);
		return true;
	};
	const release = (peer: Peer) => {
		if (lease?.peer === peer) lease = undefined;
		peer.synced = false;
		// The superseded sync's pages die with its lease; the next sync captures fresh ones.
		if (peer.view !== undefined) {
			views.delete(peer.view);
			peer.view = undefined;
		}
	};
	const currentLease = (peer: Peer, number: number): boolean => {
		if (lease && lease.expires <= now()) release(lease.peer);
		return lease?.peer === peer && lease.number === number;
	};
	const sendBytes = (peer: Peer, bytes: Uint8Array): void => {
		if (peer.socket.destroyed) return;
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
	};
	const send = (peer: Peer, frame: object): void => {
		try {
			sendBytes(peer, encodeRemoteFrame(frame, peer.limit));
		} catch {
			peer.socket.destroy();
		}
	};
	const fail = (peer: Peer, id: string, code: RemoteErrorCode) =>
		send(peer, { type: "reply", id, ok: false, error: { code } });
	const reply = (peer: Peer, id: string, data: unknown) => send(peer, { type: "reply", id, ok: true, data });
	/** A page never kills its connection: what cannot encode at the negotiated limit answers `overflow`. */
	const replyCapped = (peer: Peer, id: string, data: unknown): void => {
		try {
			sendBytes(peer, encodeRemoteFrame({ type: "reply", id, ok: true, data }, peer.limit));
		} catch {
			fail(peer, id, "overflow");
		}
	};
	/** A view reads only for its owning peer, on the live lease number and session it captured. */
	const validView = (peer: Peer, viewId: string): SyncedView | undefined => {
		const view = views.get(viewId);
		if (!view || view.owner !== peer) return undefined;
		if (!lease || lease.peer !== peer || lease.number !== view.lease || lease.expires <= now()) return undefined;
		return view.sessionId === (session.sessionManager.getSessionId() ?? null) ? view : undefined;
	};
	const publishReceipt = (scope: RemoteActionScope, receipt: RemoteReceipt): void => {
		sequence++;
		for (const peer of peers)
			if (peer.synced && lease?.peer === peer && currentLease(peer, lease.number))
				send(peer, {
					type: "event",
					epoch: registration.epoch,
					seq: sequence,
					sessionId: scope.sessionId,
					event: { type: "remote_receipt", actionId: scope.actionId, receipt },
				});
	};
	/** First terminal result stands; the typed update carries what the register now proves. */
	const settle = (scope: RemoteActionScope, result: Extract<RemoteReceipt, { status: "succeeded" | "rejected" }>) => {
		ledger.finish(scope, result);
		publishReceipt(scope, ledger.lookup(scope));
	};
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
			peer.documents = documents !== null && frame.capabilities.includes("read_document");
			reply(peer, frame.id, {
				version: REMOTE_PROTOCOL_VERSION,
				instanceKey: registration.instanceKey,
				workspace,
				epoch: registration.epoch,
				capabilities: peer.documents ? registration.capabilities : REMOTE_REQUIRED_CAPABILITIES,
				maxFrameBytes: peer.limit,
			});
			peer.decoder = new RemoteFrameDecoder(peer.limit);
			return;
		}
		if (frame.type !== "request") {
			peer.socket.destroy();
			return;
		}
		void handleRequest(peer, frame).catch(() => peer.socket.destroy());
	};
	const handleRequest = async (peer: Peer, request: RemoteRequest): Promise<void> => {
		const { id } = request;
		switch (request.op) {
			case "acquire": {
				if (lease && lease.expires <= now()) release(lease.peer);
				if (lease) {
					fail(peer, id, "busy");
					return;
				}
				lease = { peer, number: ++fencing, expires: now() + REMOTE_LEASE_TIMEOUT_MS };
				reply(peer, id, { lease: lease.number });
				return;
			}
			case "heartbeat":
				if (!currentLease(peer, request.data.lease)) {
					fail(peer, id, "stale_lease");
					return;
				}
				if (lease) lease.expires = now() + REMOTE_LEASE_TIMEOUT_MS;
				reply(peer, id, {});
				return;
			case "sync": {
				const leaseNumber = request.data.lease;
				if (!currentLease(peer, leaseNumber)) {
					fail(peer, id, "stale_lease");
					return;
				}
				const sessionId = session.sessionManager.getSessionId() || null;
				// `resumeSessionId` re-attaches the same target; it is never permission to
				// switch (that is `open_session`, which refuses while work is live).
				if (request.data.resumeSessionId && request.data.resumeSessionId !== sessionId) {
					fail(peer, id, "unsupported");
					return;
				}
				// One synchronous capture: watermark, transcript view, tool-output
				// capture and partial reply are read in the same event-loop turn, so
				// every delta sent after this reply carries a strictly greater `seq`.
				const capture = toolOutputs.capture();
				const view: SyncedView = {
					id: capture.view,
					owner: peer,
					lease: leaseNumber,
					sessionId,
					seq: sequence,
					history: new RemoteHistoryView(capture.view, session.messages),
					outputs: capture,
				};
				if (peer.view !== undefined) views.delete(peer.view);
				peer.view = view.id;
				views.set(view.id, view);
				peer.synced = true;
				const pendingForSession = sessionId ? [...(approvals.get(sessionId)?.values() ?? [])] : [];
				const partial = partialReply?.sessionId === sessionId ? partialReply : undefined;
				replyCapped(peer, id, {
					epoch: registration.epoch,
					seq: view.seq,
					view: view.id,
					sessionId,
					cwd: workspace,
					phase: !ready
						? "starting"
						: pendingForSession.length > 0
							? "waiting"
							: runId && [...pending.values()].some(a => a.kind === "stop" && a.runId === runId)
								? "stopping"
								: runId
									? "running"
									: "idle",
					runId,
					// The newest full assistant message, bounded if it outgrew a frame —
					// never the token deltas that produced it.
					partialReply: partial
						? { messageId: partial.messageId, message: boundedHistoryItem(partial.message, pageBudget(peer)) }
						: null,
					currentTools: capture.currentTools,
					pendingApprovals: pendingForSession,
					historyCursor: null,
				});
				return;
			}
			case "history": {
				const view = validView(peer, request.data.view);
				if (!view) {
					fail(peer, id, "stale_session");
					return;
				}
				const page = view.history.page(request.data.cursor, pageBudget(peer), archiveReference(view.outputs));
				if (!page) {
					fail(peer, id, "invalid_request");
					return;
				}
				replyCapped(peer, id, { view: view.id, items: page.items, cursor: page.cursor });
				return;
			}
			case "output": {
				const view = validView(peer, request.data.view);
				if (!view) {
					fail(peer, id, "stale_session");
					return;
				}
				// Paging runs off the capture taken at this sync: growth, eviction
				// and `clear` afterwards cannot change what the view serves.
				const budget = pageBudget(peer);
				const { outputId, cursor } = request.data;
				const page = outputId.startsWith("history.")
					? view.history.output(outputId, cursor, budget)
					: view.outputs.page(
							view.id,
							outputId,
							cursor,
							Math.max(4, Math.min(budget, Math.floor((peer.limit - 1024) / 6))),
						);
				if (!page) {
					fail(peer, id, "invalid_request");
					return;
				}
				replyCapped(peer, id, page);
				return;
			}
			case "read_document": {
				if (!peer.documents || !documents) {
					fail(peer, id, "unsupported");
					return;
				}
				if (!peer.view || !validView(peer, peer.view)) {
					fail(peer, id, "stale_session");
					return;
				}
				const result = documents.read(request.data.path, peer.limit);
				if (result.ok) replyCapped(peer, id, result.data);
				else fail(peer, id, result.code);
				return;
			}
			case "sessions": {
				// Read-only workspace metadata for an identity-verified peer; listing never
				// takes or needs the write lease.
				const raw = request.data.cursor;
				const offset = raw === undefined ? 0 : Number(raw);
				if (!Number.isSafeInteger(offset) || offset < 0) {
					fail(peer, id, "invalid_request");
					return;
				}
				try {
					reply(peer, id, await catalog.list(offset));
				} catch {
					fail(peer, id, "internal_error");
				}
				return;
			}
			case "receipt": {
				// Reconciliation must work before ordinary writes are re-enabled, so it never
				// waits on the lease. Absent, evicted or foreign scopes read back `unknown` —
				// never a guess that the action did not run.
				const scope = request.data;
				reply(peer, id, ledger.lookup(scope));
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
				const scope: RemoteActionScope = {
					epoch: action.epoch,
					sessionId: action.sessionId,
					actionId: action.actionId,
				};
				const command = action.command;
				// Register before dispatch so a lost reply cannot turn a retry into a second
				// side effect. The register is scoped to the ORIGINAL session: after a switch
				// the active id differs, but repeating the action must still return the same
				// receipt and never run again.
				const acceptance = ledger.accept(scope, command);
				if (acceptance.kind === "known") {
					reply(peer, id, acceptance.receipt);
					return;
				}
				if (acceptance.kind === "conflict") {
					fail(peer, id, "invalid_request");
					return;
				}
				if (acceptance.kind === "full") {
					fail(peer, id, "overflow");
					return;
				}
				if (pending.size >= REMOTE_MAX_QUEUE_FRAMES) {
					settle(scope, { status: "rejected", error: { code: "overflow" } });
					fail(peer, id, "overflow");
					return;
				}
				if (action.sessionId !== session.sessionManager.getSessionId()) {
					settle(scope, { status: "rejected", error: { code: "stale_session" } });
					fail(peer, id, "stale_session");
					return;
				}
				if (command.type === "respond") {
					// Initialization approvals answer before ordinary readiness.
					const dialogs = approvals.get(action.sessionId);
					const dialog = dialogs?.get(command.requestId);
					if (!dialog) {
						settle(scope, { status: "rejected", error: { code: "unknown_request" } });
						fail(peer, id, "unknown_request");
						return;
					}
					// Claim before forwarding: exactly one answer reaches the resolver, and a
					// late or duplicate request can only read `unknown_request`.
					dialogs!.delete(command.requestId);
					const response = command.response as Record<string, unknown>;
					if (!sendInput({ ...response, type: "extension_ui_response", id: command.requestId })) {
						dialogs!.set(command.requestId, dialog);
						settle(scope, { status: "rejected", error: { code: "overflow" } });
						fail(peer, id, "overflow");
						return;
					}
					settle(scope, { status: "succeeded", data: null });
					reply(peer, id, { status: "succeeded", data: null });
					return;
				}
				if (!peer.synced || !ready) {
					settle(scope, { status: "rejected", error: { code: "invalid_request" } });
					fail(peer, id, "invalid_request");
					return;
				}
				const innerId = randomUUID();
				let gate:
					| { kind: "stop" | "session"; abortRunId?: string; scope?: RemoteActionScope; innerId?: string }
					| undefined;
				let inner: object;
				if (command.type === "stop") {
					// Stop is explicit and bound to the exact run: a late stop for any other
					// (or finished) run is refused here, and again at delivery time below.
					if (!runId || command.runId !== runId) {
						settle(scope, { status: "rejected", error: { code: "stale_run" } });
						fail(peer, id, "stale_run");
						return;
					}
					pending.set(innerId, { scope, kind: "stop", runId });
					gate = { kind: "stop", abortRunId: runId, scope, innerId };
					inner = { id: innerId, type: "abort" };
				} else if (command.type === "new_session" || command.type === "open_session") {
					// Session switches never interrupt live work: refuse when a run is active
					// or a prompt was admitted whose run has not started yet.
					if (runId !== null || unresolvedPrompts > 0) {
						settle(scope, { status: "rejected", error: { code: "invalid_request" } });
						fail(peer, id, "invalid_request");
						return;
					}
					let targetPath: string | undefined;
					if (command.type === "open_session") {
						// The opaque id resolves through Core inside the pinned dir with the
						// canonical header cwd — no client-supplied path reaches the switch.
						let target;
						try {
							target = await catalog.resolve(command.sessionId);
						} catch {
							settle(scope, { status: "rejected", error: { code: "internal_error" } });
							fail(peer, id, "internal_error");
							return;
						}
						if (!target) {
							settle(scope, { status: "rejected", error: { code: "invalid_request" } });
							fail(peer, id, "invalid_request");
							return;
						}
						targetPath = target.path;
					}
					pending.set(innerId, { scope, kind: "session" });
					gate = { kind: "session", scope, innerId };
					inner = targetPath
						? { id: innerId, type: "switch_session", sessionPath: targetPath }
						: { id: innerId, type: "new_session" };
				} else {
					pending.set(innerId, { scope, kind: "prompt" });
					unresolvedPrompts++;
					inner = { id: innerId, type: "prompt", message: command.message };
				}
				if (!sendInput(inner, gate)) {
					if (pending.delete(innerId) && command.type === "prompt") unresolvedPrompts--;
					settle(scope, { status: "rejected", error: { code: "overflow" } });
					fail(peer, id, "overflow");
					return;
				}
				if (!pending.has(innerId)) {
					// The gate closed before Core read the frame; `discard` already settled it.
					reply(peer, id, ledger.lookup(scope));
					return;
				}
				// ACK means registered and dispatched, not finished. The terminal result rides
				// the receipt register (queryable, plus the optional typed event below).
				reply(peer, id, { status: "accepted" });
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
			// Accepted work outlives the attach: `pending` and `approvals` stay owned by
			// the instance, so their receipts still settle after the client is gone.
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
		documents?.close();
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
		documents?.close();
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
	/**
	 * Core asks this before an abort touches the current run: the pre-delivery gate only
	 * proves the window before Core read the frame, so a stop whose run finished, or whose
	 * session was switched, must still not act. An unregistered id proves nothing either.
	 */
	function mayAbort(commandId: string | undefined): boolean {
		if (commandId === undefined) return false;
		const waiting = pending.get(commandId);
		if (waiting?.kind !== "stop") return false;
		if (runId === null || waiting.runId !== runId) return false;
		const sessionId = session.sessionManager.getSessionId();
		return sessionId !== undefined && waiting.scope.sessionId === sessionId;
	}
	function output(frame: object): void {
		if (closed) return;
		const value = frame as Record<string, unknown>;
		const sessionId = session.sessionManager.getSessionId() ?? null;
		if (sessionId !== scopedSessionId) {
			// A session identity change — never a peer disconnect — retires the
			// captured transcript state and every view that could read it.
			scopedSessionId = sessionId;
			toolOutputs.clear();
			partialReply = null;
			for (const [viewId, view] of views) if (view.sessionId !== sessionId) views.delete(viewId);
		}
		let forward: object = frame;
		// The archive belongs to the session independently of attachments.
		toolOutputs.observe(value);
		if (value.type === "command_output" && typeof value.text === "string") {
			const outputId = `command.${sequence + 1}`;
			toolOutputs.observe({
				type: "tool_execution_end",
				toolCallId: outputId,
				toolName: "command_output",
				result: value.text,
			});
			forward = { ...value, outputId };
		}
		if (value.type === "ready") {
			ready = true;
			// Ordered ready marker for the GUI reducer: it claims this sequence
			// slot, so a synced client applies it before any later delta.
			forward = { type: "remote_ready" };
		}
		if (value.type === "response" && typeof value.id === "string") {
			const waiting = pending.get(value.id);
			if (waiting) {
				pending.delete(value.id);
				if (waiting.kind === "prompt" && unresolvedPrompts > 0) unresolvedPrompts--;
				settle(
					waiting.scope,
					value.success
						? { status: "succeeded", data: value.data ?? null }
						: {
								status: "rejected",
								// Core state refusals (busy, kind mismatch) were not executed;
								// a stale-run fence refusal is public too, so the old receipt
								// settles as rejected-not-executed rather than as a failure.
								// Anything else is an internal failure. Neither is replayed.
								error: {
									code:
										value.code === "session_busy" || value.code === "session_kind_mismatch"
											? "invalid_request"
											: value.code === "stale_run"
												? "stale_run"
												: "internal_error",
								},
							},
				);
			}
			return;
		}
		if (value.type === "extension_ui_request") {
			if (value.method === "cancel" && typeof value.targetId === "string") {
				// Abort and timeout both arrive as a cancel frame for the settled dialog: the
				// id dies instance-wide so no later response can act on it.
				for (const dialogs of approvals.values()) dialogs.delete(value.targetId);
			} else if (
				typeof value.id === "string" &&
				typeof value.method === "string" &&
				REMOTE_UI_METHODS.includes(value.method)
			) {
				if (sessionId) {
					let dialogs = approvals.get(sessionId);
					if (!dialogs) {
						dialogs = new Map();
						approvals.set(sessionId, dialogs);
					}
					dialogs.set(value.id, { ...value, requestId: value.id, sessionId });
				}
			}
		}
		if (value.type === "message_start" || value.type === "message_update") {
			const message = value.message as Record<string, unknown> | undefined;
			if (message?.role === "assistant" && typeof value.messageId === "string")
				partialReply = { sessionId, messageId: value.messageId, message };
		} else if (value.type === "message_end" && partialReply?.messageId === value.messageId) {
			partialReply = null;
		}
		if (value.type === "agent_start") {
			runId = randomUUID();
			// The run materialized: admitted prompts no longer race session switches.
			unresolvedPrompts = 0;
			// The GUI binds its reducer slot to the run this event opened.
			forward = { ...value, runId };
		} else if (value.type === "agent_end") {
			const endedRunId = runId;
			runId = null;
			if (partialReply?.sessionId === sessionId) partialReply = null;
			// The id of the run that just ended, not the (absent) current one.
			forward = { type: "agent_end", runId: endedRunId, isTerminal: value.isTerminal };
		}
		if (!sessionId) return;
		sequence++;
		for (const peer of peers) {
			if (!peer.synced || lease?.peer !== peer || !currentLease(peer, lease.number)) continue;
			const envelope = { type: "event", epoch: registration.epoch, seq: sequence, sessionId };
			try {
				sendBytes(peer, encodeRemoteFrame({ ...envelope, event: forward }, peer.limit));
			} catch {
				const view = peer.view === undefined ? undefined : views.get(peer.view);
				send(peer, {
					...envelope,
					event: boundedRemoteEvent(
						forward,
						Math.max(512, peer.limit - 512),
						view && archiveReference(view.outputs),
					),
				});
			}
		}
	}
	return { input, output, close, mayAbort };
}
