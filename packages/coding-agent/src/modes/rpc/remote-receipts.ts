import { stableStringifyJson } from "@oh-my-pi/pi-utils";
import {
	REMOTE_MAX_ID_LENGTH,
	REMOTE_MAX_RECEIPTS,
	REMOTE_RECEIPT_RETENTION_MS,
	type RemoteActionCommand,
	type RemoteReceipt,
} from "./remote-wire";

/**
 * Everything a client must retain to reconcile one mutation: no message text,
 * no approval body, no credential. Transport request IDs are a different
 * namespace and are never used here.
 */
export interface RemoteActionScope {
	epoch: string;
	sessionId: string;
	actionId: string;
}

/** Result of registering an action before it is dispatched. */
export type RemoteActionAcceptance =
	// Registered now: the caller owns the only execution of this action.
	| { kind: "new" }
	// Already registered under this scope, so dispatching again would repeat a side effect.
	| { kind: "known"; receipt: RemoteReceipt }
	// Same action ID carrying a different command: refused, never re-executed or overwritten.
	| { kind: "conflict" }
	// Capacity reached while every register is still inside its retention window.
	| { kind: "full" };

interface Register {
	readonly digest: string;
	receipt: RemoteReceipt;
	/** Terminal time; equals the clock at acceptance while still in flight. */
	updatedAt: number;
}

/**
 * Ids are opaque text that never contains NUL (remote-wire rejects it), so
 * joining on NUL cannot make two (epoch, session, action) splits collide.
 */
const SCOPE_SEPARATOR = "\u0000";

/**
 * The instance's register of remote mutations.
 *
 * `accept` runs before the command is dispatched, so a client whose reply was
 * lost can prove with `lookup` that the action was taken rather than guess and
 * replay it. Only a SHA-256 digest of the canonical command is kept: receipts
 * are what the peer gets back, so a stored prompt or approval body would cross
 * the wire.
 *
 * A terminal receipt lives for `retentionMs` measured from its result. In-flight
 * registers are never expired or evicted, because dropping one turns a retry
 * into a second execution; capacity therefore surfaces as an explicit `full`
 * instead of silent loss. The ledger lives and dies with one `serve`, so a new
 * `epoch` starts empty and an old epoch's scope can only ever be read back under
 * that same epoch.
 */
export class RemoteReceiptLedger {
	readonly #now: () => number;
	readonly #maxEntries: number;
	readonly #retentionMs: number;
	readonly #registers = new Map<string, Register>();
	/** Lowest expiry among terminal receipts, so pruning can skip the scan. */
	#nextExpiry = Number.POSITIVE_INFINITY;

	constructor(options?: { now?: () => number; maxEntries?: number; retentionMs?: number }) {
		this.#now = options?.now ?? Date.now;
		this.#maxEntries = positive(options?.maxEntries ?? REMOTE_MAX_RECEIPTS, "maxEntries");
		this.#retentionMs = positive(options?.retentionMs ?? REMOTE_RECEIPT_RETENTION_MS, "retentionMs");
	}

	/**
	 * Register `scope` before dispatching `command`. Callers MUST NOT dispatch
	 * unless this returns `new`; every other kind already has an owner.
	 */
	accept(scope: RemoteActionScope, command: RemoteActionCommand): RemoteActionAcceptance {
		const key = this.#key(scope);
		const digest = commandDigest(command);
		const now = this.#now();
		this.#prune(now);
		const register = this.#registers.get(key);
		if (register) {
			if (register.digest !== digest) return { kind: "conflict" };
			return { kind: "known", receipt: copy(register.receipt) };
		}
		if (this.#registers.size >= this.#maxEntries) return { kind: "full" };
		this.#registers.set(key, { digest, receipt: { status: "accepted" }, updatedAt: now });
		return { kind: "new" };
	}

	/**
	 * Record the outcome of an accepted action. An unregistered scope is ignored
	 * — nothing was registered, so there is nothing to prove — and the first
	 * terminal result stands, so a late duplicate (a timeout path firing after a
	 * real reply) cannot rewrite a completed action.
	 */
	finish(scope: RemoteActionScope, result: Extract<RemoteReceipt, { status: "succeeded" | "rejected" }>): void {
		const register = this.#registers.get(this.#key(scope));
		if (!register || register.receipt.status !== "accepted") return;
		register.receipt = copy(result);
		register.updatedAt = this.#now();
		const expires = register.updatedAt + this.#retentionMs;
		if (expires < this.#nextExpiry) this.#nextExpiry = expires;
	}

	/** The stored outcome, or `unknown` when the scope is absent, expired or foreign. */
	lookup(scope: RemoteActionScope): RemoteReceipt {
		const key = this.#key(scope);
		const register = this.#registers.get(key);
		if (!register) return { status: "unknown" };
		if (this.#expired(register, this.#now())) {
			this.#registers.delete(key);
			return { status: "unknown" };
		}
		return copy(register.receipt);
	}

	#key(scope: RemoteActionScope): string {
		const epoch = scopeId(scope.epoch, "epoch");
		const sessionId = scopeId(scope.sessionId, "sessionId");
		const actionId = scopeId(scope.actionId, "actionId");
		return `${epoch}${SCOPE_SEPARATOR}${sessionId}${SCOPE_SEPARATOR}${actionId}`;
	}

	#expired(register: Register, now: number): boolean {
		return register.receipt.status !== "accepted" && register.updatedAt + this.#retentionMs <= now;
	}

	/** Free expired terminal receipts; in-flight registers are kept. */
	#prune(now: number): void {
		if (now < this.#nextExpiry) return;
		this.#nextExpiry = Number.POSITIVE_INFINITY;
		for (const [key, register] of this.#registers) {
			if (this.#expired(register, now)) {
				this.#registers.delete(key);
				continue;
			}
			if (register.receipt.status === "accepted") continue;
			const expires = register.updatedAt + this.#retentionMs;
			if (expires < this.#nextExpiry) this.#nextExpiry = expires;
		}
	}
}

function positive(value: number, name: string): number {
	if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`Invalid remote receipt ${name}`);
	return value;
}

/**
 * Remote ids are non-empty and free of NUL/CR/LF, the same boundary the frame
 * validator enforces. A violation is a caller bug: it could forge a scope
 * collision, so it fails loudly instead of being keyed or answered as `unknown`.
 */
function scopeId(value: string, name: string): string {
	if (
		typeof value !== "string" ||
		value.length === 0 ||
		value.length > REMOTE_MAX_ID_LENGTH ||
		/[\0\r\n]/.test(value)
	) {
		throw new TypeError(`Invalid remote action scope ${name}`);
	}
	return value;
}

/** Canonical serialization, so property order at any depth cannot change it. */
function commandDigest(command: RemoteActionCommand): string {
	return new Bun.CryptoHasher("sha256").update(stableStringifyJson(command)).digest("hex");
}

/** Fresh receipt for the caller: the register keeps its own status and error. */
function copy(receipt: RemoteReceipt): RemoteReceipt {
	switch (receipt.status) {
		case "accepted":
			return { status: "accepted" };
		case "succeeded":
			return { status: "succeeded", data: receipt.data };
		case "rejected":
			return { status: "rejected", error: { code: receipt.error.code } };
		case "unknown":
			return { status: "unknown" };
	}
}
