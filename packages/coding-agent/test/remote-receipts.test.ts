import { expect, test } from "bun:test";
import { RemoteReceiptLedger, type RemoteActionScope } from "../src/modes/rpc/remote-receipts";
import { REMOTE_MAX_ID_LENGTH, type RemoteActionCommand, type RemoteReceipt } from "../src/modes/rpc/remote-wire";

let issued = 0;
function scope(overrides: Partial<RemoteActionScope> = {}): RemoteActionScope {
	return { epoch: "epoch-1", sessionId: "session-1", actionId: `action-${++issued}`, ...overrides };
}

function prompt(message: string): RemoteActionCommand {
	return { type: "prompt", message };
}

function respond(requestId: string, response: unknown): RemoteActionCommand {
	return { type: "respond", requestId, response };
}

function clock(start = 1_000_000): { now(): number; advance(ms: number): void } {
	let current = start;
	return {
		now: () => current,
		advance: (ms: number) => {
			current += ms;
		},
	};
}

test("an accepted action is reconcilable before it finishes and after", () => {
	const ledger = new RemoteReceiptLedger();
	const action = scope();
	expect(ledger.accept(action, prompt("run the deploy"))).toEqual({ kind: "new" });
	// The register exists before the command reaches the agent, so a client whose
	// reply was lost learns "registered" and must not replay the deploy.
	expect(ledger.lookup(action)).toEqual({ status: "accepted" });
	ledger.finish(action, { status: "succeeded", data: { stopped: true } });
	expect(ledger.lookup(action)).toEqual({ status: "succeeded", data: { stopped: true } });
	// Retrying the same scoped action reports that completed outcome instead of running again.
	expect(ledger.accept(action, prompt("run the deploy"))).toEqual({
		kind: "known",
		receipt: { status: "succeeded", data: { stopped: true } },
	});
});

test("the first terminal result stands and a duplicate finish cannot rewrite it", () => {
	const ledger = new RemoteReceiptLedger();
	const done = scope();
	ledger.accept(done, prompt("send the release note"));
	ledger.finish(done, { status: "succeeded", data: null });
	// A timeout path firing after the real reply must not flip success to rejected:
	// the client would treat a delivered prompt as failed and send it a second time.
	ledger.finish(done, { status: "rejected", error: { code: "timeout" } });
	expect(ledger.lookup(done)).toEqual({ status: "succeeded", data: null });

	const failed = scope();
	ledger.accept(failed, prompt("stop the run"));
	ledger.finish(failed, { status: "rejected", error: { code: "stale_run" } });
	ledger.finish(failed, { status: "succeeded", data: "late" });
	expect(ledger.lookup(failed)).toEqual({ status: "rejected", error: { code: "stale_run" } });
});

test("a reused action ID with a different command conflicts and leaves the register intact", () => {
	const ledger = new RemoteReceiptLedger();
	const action = scope();
	ledger.accept(action, prompt("delete the staging bucket"));
	expect(ledger.accept(action, prompt("delete the production bucket"))).toEqual({ kind: "conflict" });
	// Neither overwritten nor re-dispatched: the first register still says accepted.
	expect(ledger.lookup(action)).toEqual({ status: "accepted" });
	expect(ledger.accept(action, prompt("delete the staging bucket"))).toEqual({
		kind: "known",
		receipt: { status: "accepted" },
	});

	// Approval bodies are compared recursively, so a swapped answer for the same
	// requestId cannot be mistaken for the action already registered.
	const approval = scope();
	ledger.accept(approval, respond("req-1", { approved: false, note: { scope: "once" } }));
	expect(ledger.accept(approval, respond("req-1", { approved: true, note: { scope: "always" } }))).toEqual({
		kind: "conflict",
	});
});

test("the command digest ignores property order at every depth", () => {
	const ledger = new RemoteReceiptLedger();
	const flat = scope();
	ledger.accept(flat, prompt("same text"));
	// A client that reserializes with reordered keys is the same action, not a conflict.
	expect(ledger.accept(flat, { message: "same text", type: "prompt" })).toEqual({
		kind: "known",
		receipt: { status: "accepted" },
	});
	const nested = scope();
	ledger.accept(nested, respond("req-9", { approved: true, meta: { a: 1, b: { c: 2, d: 3 } } }));
	expect(ledger.accept(nested, respond("req-9", { meta: { b: { d: 3, c: 2 }, a: 1 }, approved: true }))).toEqual({
		kind: "known",
		receipt: { status: "accepted" },
	});
});

test("distinct action IDs, sessions and epochs never share a receipt", () => {
	const ledger = new RemoteReceiptLedger();
	const first = scope({ actionId: "action-1" });
	const siblings = [
		scope({ actionId: "action-2" }),
		scope({ sessionId: "session-2", actionId: "action-1" }),
		scope({ epoch: "epoch-2", actionId: "action-1" }),
	];
	expect(ledger.accept(first, prompt("same text"))).toEqual({ kind: "new" });
	for (const sibling of siblings) expect(ledger.accept(sibling, prompt("same text"))).toEqual({ kind: "new" });
	ledger.finish(first, { status: "succeeded", data: null });
	// Finishing one action cannot settle another, including in another session or epoch.
	for (const sibling of siblings) expect(ledger.lookup(sibling)).toEqual({ status: "accepted" });
	// A scope that was never written proves nothing about anything.
	expect(ledger.lookup(scope({ actionId: "action-unregistered" }))).toEqual({ status: "unknown" });
});

test("scope keys cannot collide, and IDs that would forge a collision are refused", () => {
	const ledger = new RemoteReceiptLedger();
	const left = { epoch: "a:b", sessionId: "c", actionId: "d" };
	const right = { epoch: "a", sessionId: "b:c", actionId: "d" };
	expect(ledger.accept(left, prompt("one"))).toEqual({ kind: "new" });
	// A naive separator join would call this a conflicting reuse and refuse a valid prompt.
	expect(ledger.accept(right, prompt("two"))).toEqual({ kind: "new" });
	ledger.finish(right, { status: "rejected", error: { code: "invalid_request" } });
	expect(ledger.lookup(left)).toEqual({ status: "accepted" });
	expect(ledger.lookup(right)).toEqual({ status: "rejected", error: { code: "invalid_request" } });

	expect(() => ledger.accept({ epoch: "a\u0000b", sessionId: "c", actionId: "d" }, prompt("x"))).toThrow(TypeError);
	expect(() => ledger.lookup({ epoch: "a", sessionId: "", actionId: "d" })).toThrow(TypeError);
	expect(() => ledger.lookup({ epoch: "a", sessionId: "b".repeat(REMOTE_MAX_ID_LENGTH + 1), actionId: "d" })).toThrow(
		TypeError,
	);
	expect(() =>
		ledger.finish({ epoch: "a", sessionId: "c\n", actionId: "d" }, { status: "succeeded", data: null }),
	).toThrow(TypeError);
});

test("receipts expire from their terminal time and in-flight registers never do", () => {
	const time = clock();
	const ledger = new RemoteReceiptLedger({ now: time.now, retentionMs: 1_000 });
	const done = scope();
	ledger.accept(done, prompt("old prompt"));
	time.advance(900);
	ledger.finish(done, { status: "succeeded", data: "ok" });
	time.advance(500);
	// Retention runs from the result, so a long-running action is not erased while
	// its receipt is still the only proof that it ran.
	expect(ledger.lookup(done)).toEqual({ status: "succeeded", data: "ok" });
	time.advance(500);
	expect(ledger.lookup(done)).toEqual({ status: "unknown" });
	// Past the window the ID is reusable, exactly as the wire promises.
	expect(ledger.accept(done, prompt("a different prompt"))).toEqual({ kind: "new" });

	const running = scope();
	ledger.accept(running, prompt("still running"));
	time.advance(60 * 60 * 1000);
	// Dropping an in-flight register would turn the next retry into a second execution.
	expect(ledger.lookup(running)).toEqual({ status: "accepted" });
});

test("a full ledger refuses new work without evicting anything", () => {
	const time = clock();
	const ledger = new RemoteReceiptLedger({ now: time.now, maxEntries: 2, retentionMs: 1_000 });
	const first = scope({ actionId: "action-1" });
	const second = scope({ actionId: "action-2" });
	const third = scope({ actionId: "action-3" });
	expect(ledger.accept(first, prompt("one"))).toEqual({ kind: "new" });
	expect(ledger.accept(second, prompt("two"))).toEqual({ kind: "new" });
	// Nothing is evicted to make room — losing a receipt would make a retry a
	// second side effect — so capacity is reported instead.
	expect(ledger.accept(third, prompt("three"))).toEqual({ kind: "full" });
	expect(ledger.lookup(first)).toEqual({ status: "accepted" });
	expect(ledger.lookup(second)).toEqual({ status: "accepted" });
	// Capacity pressure never turns a duplicate into an error or a fresh run.
	expect(ledger.accept(first, prompt("one"))).toEqual({ kind: "known", receipt: { status: "accepted" } });
	// A finished receipt still holds its slot for the whole retention window...
	ledger.finish(second, { status: "succeeded", data: null });
	expect(ledger.accept(third, prompt("three"))).toEqual({ kind: "full" });
	// ...and only an expired one frees it, leaving the in-flight register alone.
	time.advance(1_000);
	expect(ledger.accept(third, prompt("three"))).toEqual({ kind: "new" });
	expect(ledger.lookup(first)).toEqual({ status: "accepted" });
	expect(ledger.lookup(third)).toEqual({ status: "accepted" });
});

test("a receipt handed back to a peer carries no command body", () => {
	const ledger = new RemoteReceiptLedger();
	const action = scope();
	ledger.accept(action, respond("req-7", { token: "hunter2", approved: true }));
	// The in-flight reply a peer receives is the status alone: the approval body
	// it carried stays in the digest.
	expect(ledger.accept(action, respond("req-7", { token: "hunter2", approved: true }))).toEqual({
		kind: "known",
		receipt: { status: "accepted" },
	});
	ledger.finish(action, { status: "succeeded", data: { approved: true } });
	const receipt = ledger.lookup(action);
	expect(receipt).toEqual({ status: "succeeded", data: { approved: true } });
	expect(JSON.stringify(receipt)).not.toContain("hunter2");

	const failed = scope();
	ledger.accept(failed, prompt("stop the run"));
	ledger.finish(failed, { status: "rejected", error: { code: "stale_run" } });
	// The error object handed to an encoder is a copy, not the stored register.
	const rejected = ledger.lookup(failed) as Extract<RemoteReceipt, { status: "rejected" }>;
	rejected.error.code = "timeout";
	expect(ledger.lookup(failed)).toEqual({ status: "rejected", error: { code: "stale_run" } });
});

test("nonsense limits fail loudly instead of silently disabling the ledger", () => {
	expect(() => new RemoteReceiptLedger({ maxEntries: 0 })).toThrow(RangeError);
	expect(() => new RemoteReceiptLedger({ retentionMs: -1 })).toThrow(RangeError);
});
