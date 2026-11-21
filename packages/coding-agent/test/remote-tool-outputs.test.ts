import { expect, test } from "bun:test";
import { RemoteToolOutputs, type RemoteToolOutputView } from "../src/modes/rpc/remote-tool-outputs";
import { REMOTE_MAX_OUTPUT_PAGE_BYTES } from "../src/modes/rpc/remote-wire";

function start(toolCallId: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
	return { type: "tool_execution_start", toolCallId, toolName: "bash", ...extra };
}

function update(toolCallId: string, text: string): Record<string, unknown> {
	return {
		type: "tool_execution_update",
		toolCallId,
		toolName: "bash",
		partialResult: { content: [{ type: "text", text }], details: { lines: text.length } },
	};
}

function end(
	toolCallId: string,
	text: string | undefined,
	extra: Record<string, unknown> = {},
): Record<string, unknown> {
	return {
		type: "tool_execution_end",
		toolCallId,
		toolName: "bash",
		result: text === undefined ? { details: { exit: 0 } } : { content: [{ type: "text", text }] },
		...extra,
	};
}

/** Walks the cursor chain to the end; a page vanishing mid-chain is a defect, not an empty result. */
function collect(view: RemoteToolOutputView, outputId: string): { data: string; truncated: boolean } {
	let data = "";
	let cursor: string | undefined;
	for (;;) {
		const page = view.page(view.view, outputId, cursor);
		if (page === null) throw new Error(`page ${outputId}@${cursor ?? "start"} vanished`);
		data += page.data;
		if (page.cursor === null) return { data, truncated: page.truncated };
		cursor = page.cursor;
		if (data.length > 1_000_000) throw new Error("cursor did not advance");
	}
}

test("growing snapshots replace retained output; replays and metadata-only updates never corrupt it", () => {
	const outputs = new RemoteToolOutputs();
	outputs.observe(start("t1", { args: { command: "ls -la" }, intent: "list files" }));
	// Each update is a full growing snapshot. An implementation that appended
	// would serve "HelloHello worldHello world!…" to the client.
	outputs.observe(update("t1", "Hello"));
	outputs.observe(update("t1", "Hello world"));
	outputs.observe(update("t1", "Hello world!"));
	outputs.observe(update("t1", "Hello world!"));
	// Metadata-only updates carry no text: the last defined text must survive,
	// not blank the progress the remote GUI is rendering.
	outputs.observe({
		type: "tool_execution_update",
		toolCallId: "t1",
		toolName: "bash",
		partialResult: { details: {} },
	});
	outputs.observe({ type: "tool_execution_update", toolCallId: "t1", toolName: "bash" });
	outputs.observe({ type: "turn_start" });

	const view = outputs.capture();
	// Metadata only, with the outputId the client pages by; output bytes never
	// ride inside the snapshot frame.
	expect(view.currentTools).toEqual([
		{
			toolCallId: "t1",
			toolName: "bash",
			done: false,
			outputId: "t1",
			args: { command: "ls -la" },
			intent: "list files",
		},
	]);
	expect(JSON.parse(JSON.stringify(view.currentTools))).toEqual(view.currentTools);
	expect(collect(view, "t1")).toEqual({ data: "Hello world!", truncated: false });
});

test("end settles the output; a text-free result keeps the last partial and a later final replaces it", () => {
	const outputs = new RemoteToolOutputs();
	outputs.observe(start("t1"));
	outputs.observe(update("t1", "partial progress"));
	// The run settled while no peer could see it, with a result carrying only
	// details: the captured page must still show the last defined progress.
	outputs.observe(end("t1", undefined));
	outputs.observe(end("t1", undefined)); // replayed end changes nothing

	let view = outputs.capture();
	expect(view.currentTools).toEqual([{ toolCallId: "t1", toolName: "bash", done: true, outputId: "t1" }]);
	expect(collect(view, "t1")).toEqual({ data: "partial progress", truncated: false });

	// A late settled frame carrying the real result replaces the snapshot wholesale.
	outputs.observe(end("t1", "FINAL"));
	view = outputs.capture();
	expect(collect(view, "t1")).toEqual({ data: "FINAL", truncated: false });
});

test("finalized references carry isError and done; references keep start order; never-run output is empty, not truncated", () => {
	const outputs = new RemoteToolOutputs();
	outputs.observe(start("a"));
	outputs.observe(start("b"));
	outputs.observe(update("a", "A"));
	outputs.observe(end("a", "A done", { isError: true }));
	const view = outputs.capture();
	expect(view.currentTools).toEqual([
		{ toolCallId: "a", toolName: "bash", done: true, outputId: "a", isError: true },
		{ toolCallId: "b", toolName: "bash", done: false, outputId: "b" },
	]);
	expect(collect(view, "a")).toEqual({ data: "A done", truncated: false });
	// b produced no bytes at all: an empty page with truncated=false is how the
	// client distinguishes a genuinely empty result from a trimmed one.
	expect(collect(view, "b")).toEqual({ data: "", truncated: false });
});

test("capture is detached: later growth, finalize, and clear never change an existing view", () => {
	const outputs = new RemoteToolOutputs();
	outputs.observe(start("t1"));
	outputs.observe(update("t1", "abc"));
	const first = outputs.capture();
	outputs.observe(update("t1", "abcdef"));
	outputs.observe(end("t1", "abcdef done"));
	outputs.clear();

	// The view captured mid-run still serves exactly what was visible then.
	expect(first.currentTools).toEqual([{ toolCallId: "t1", toolName: "bash", done: false, outputId: "t1" }]);
	expect(collect(first, "t1")).toEqual({ data: "abc", truncated: false });

	const cleared = outputs.capture();
	expect(cleared.currentTools).toEqual([]);
	expect(cleared.page(cleared.view, "t1")).toBeNull();

	outputs.observe(start("t2"));
	outputs.observe(update("t2", "xyz"));
	const third = outputs.capture();
	expect(collect(third, "t2")).toEqual({ data: "xyz", truncated: false });
	// …and only the newer reference sees the new tool; the old view stays frozen.
	expect(first.page(first.view, "t2")).toBeNull();
});

test("pages split only on UTF-8 boundaries and reassemble exactly", () => {
	const outputs = new RemoteToolOutputs({ maxPageBytes: 2 });
	outputs.observe(start("t1"));
	const original = "a😀b";
	outputs.observe(update("t1", original));
	const view = outputs.capture();

	const datas: string[] = [];
	let cursor: string | undefined;
	for (;;) {
		const page = view.page(view.view, "t1", cursor);
		if (page === null) throw new Error("page vanished");
		expect(page.view).toBe(view.view);
		expect(page.outputId).toBe("t1");
		// A split surrogate or truncated sequence would decode to U+FFFD.
		expect(page.data).not.toContain("\uFFFD");
		datas.push(page.data);
		if (page.cursor === null) break;
		cursor = page.cursor;
		if (datas.length > 16) throw new Error("cursor did not advance");
	}
	// The 4-byte emoji exceeds the 2-byte page budget; it is served whole on its
	// own page rather than split, and every page still made progress.
	expect(datas).toEqual(["a", "😀", "b"]);
	expect(datas.join("")).toBe(original);
});

test("aggregate byte bound truncates explicitly and evicts whole finalized outputs before shrinking running ones", () => {
	const outputs = new RemoteToolOutputs({ maxOutputBytes: 48 });
	outputs.observe(start("a"));
	outputs.observe(update("a", "A".repeat(30)));
	outputs.observe(start("b"));
	// Both a and b are still running, so b cannot evict a: b keeps only the
	// 18-byte prefix and is marked truncated instead of the cap being exceeded.
	outputs.observe(update("b", "B".repeat(30)));

	const before = outputs.capture();
	expect(collect(before, "a")).toEqual({ data: "A".repeat(30), truncated: false });
	expect(collect(before, "b")).toEqual({ data: "B".repeat(18), truncated: true });

	// b settles: the retained prefix stands (nothing else is evictable yet).
	outputs.observe(end("b", "B".repeat(30)));
	// A new running output may now drop the whole finalized b instead of being
	// truncated itself.
	outputs.observe(start("c"));
	outputs.observe(update("c", "C".repeat(10)));

	const after = outputs.capture();
	expect(collect(after, "a")).toEqual({ data: "A".repeat(30), truncated: false });
	expect(after.page(after.view, "b")).toBeNull();
	expect(collect(after, "c")).toEqual({ data: "C".repeat(10), truncated: false });
	// The pre-eviction view still answers exactly what it captured.
	expect(collect(before, "b")).toEqual({ data: "B".repeat(18), truncated: true });
});

test("retained finalized count is finite; the running tool is never dropped for capacity", () => {
	const outputs = new RemoteToolOutputs({ maxRetainedTools: 2 });
	outputs.observe(start("t1"));
	outputs.observe(end("t1", "t1"));
	const seen = outputs.capture();
	outputs.observe(start("t2"));
	outputs.observe(end("t2", "t2"));
	outputs.observe(start("t3"));
	outputs.observe(end("t3", "t3"));
	outputs.observe(start("live"));

	const view = outputs.capture();
	expect(view.currentTools).toEqual([
		{ toolCallId: "t2", toolName: "bash", done: true, outputId: "t2" },
		{ toolCallId: "t3", toolName: "bash", done: true, outputId: "t3" },
		{ toolCallId: "live", toolName: "bash", done: false, outputId: "live" },
	]);
	// The evicted id stops resolving on fresh captures…
	expect(view.page(view.view, "t1")).toBeNull();
	// …while the capture taken while it was retained keeps serving it.
	expect(collect(seen, "t1")).toEqual({ data: "t1", truncated: false });
});

test("foreign views, outputs, and cursors page to null instead of guessing", () => {
	const outputs = new RemoteToolOutputs({ maxPageBytes: 8 });
	outputs.observe(start("x"));
	outputs.observe(update("x", "x".repeat(40)));
	outputs.observe(start("y"));
	outputs.observe(update("y", "y".repeat(40)));
	const view = outputs.capture();

	expect(view.page("some-other-view", "x")).toBeNull();
	expect(view.page(view.view, "no-such-output")).toBeNull();
	expect(view.page(view.view, "x", "not a cursor!!")).toBeNull();

	const first = view.page(view.view, "x");
	if (first?.cursor === null || first === null) throw new Error("expected a continuation cursor");
	// The byte offset would be valid on the other output/capture; provenance is what rejects it.
	expect(view.page(view.view, "y", first.cursor)).toBeNull();
	const fresh = outputs.capture();
	expect(fresh.page(fresh.view, "x", first.cursor)).toBeNull();
	// Its own output resumes at exactly the captured bytes.
	const second = view.page(view.view, "x", first.cursor);
	expect(second?.data).toBe("x".repeat(8));
});

test("default page bound is the wire constant for full-size frames", () => {
	const outputs = new RemoteToolOutputs();
	outputs.observe(start("d"));
	outputs.observe(update("d", "z".repeat(300_000)));
	const view = outputs.capture();
	const first = view.page(view.view, "d");
	expect(first?.data.length).toBe(REMOTE_MAX_OUTPUT_PAGE_BYTES);
	if (first?.cursor === null || first === null) throw new Error("expected a continuation cursor");
	const rest = view.page(view.view, "d", first.cursor);
	expect(rest?.data.length).toBe(300_000 - REMOTE_MAX_OUTPUT_PAGE_BYTES);
	expect(rest?.cursor).toBeNull();
});
