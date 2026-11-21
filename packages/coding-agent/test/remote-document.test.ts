import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import {
	openRemoteDocumentWorkspace,
	readRemoteDocument,
	remoteDocumentSupported,
} from "../src/modes/rpc/remote-document";
import {
	REMOTE_MAX_DOCUMENT_BYTES,
	type RemoteDocumentResult,
	type RemoteErrorCode,
} from "../src/modes/rpc/remote-wire";

/**
 * The walker drives Darwin's `openat` and `readlinkat`, so every case below is
 * a macOS case: off macOS the whole file skips rather than asserting the
 * `unsupported` reply it can only fake by stubbing `process.platform`.
 */
const darwinOnly = process.platform === "darwin" ? describe : describe.skip;

const GUIDE = "# Guide\n\nread from the remote host\n";
const PLAIN = "plain text\n";
const LINKED = "through the link\n";
/** Newlines JSON doubles: this many bytes will not fit one frame. */
const WIDE_LINES = REMOTE_MAX_DOCUMENT_BYTES;
/** Same trick, small enough that the frame still carries it. */
const FITTING_LINES = 300 * 1024;

let parent = "";
let ws = "";
let sibling = "";
const physical = (target: string): string => path.join(fs.realpathSync(parent), target);

function write(relative: string, content: string | Buffer): string {
	const file = path.join(ws, relative);
	fs.mkdirSync(path.dirname(file), { recursive: true });
	fs.writeFileSync(file, content);
	return file;
}

function link(target: string, relative: string): void {
	fs.symlinkSync(target, path.join(ws, relative));
}

function openDescriptors(): string[] {
	return fs.readdirSync("/dev/fd").sort();
}

/** A refused read, as its code: an `ok` reply fails with the data it leaked. */
async function expectCode(target: string, code: RemoteErrorCode): Promise<void> {
	const result = await readRemoteDocument(ws, target);
	expect(result.ok ? `read ${JSON.stringify(result.data.content.slice(0, 32))}` : result.code).toBe(code);
}

/** A read that worked, down to the bytes and the path echoed back. */
async function expectText(
	target: string,
	content: string,
	mediaType: RemoteDocumentResult["mediaType"],
): Promise<void> {
	const result = await readRemoteDocument(ws, target);
	if (!result.ok) throw new Error(`expected a document, got ${result.code}`);
	expect(result.data.content).toBe(content);
	expect(result.data.mediaType).toBe(mediaType);
	expect(result.data.path.startsWith(`${ws}/`)).toBe(true);
}

darwinOnly("readRemoteDocument", () => {
	beforeAll(() => {
		// A real path under a symlinked scratch root (`/var` → `/private/var`)
		// is the shape a macOS workspace has, so resolve it first: the anchor
		// refuses a workspace that is itself a link.
		parent = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "remote-document-"));
		ws = path.join(parent, "ws");
		sibling = path.join(parent, "ws-evil");
		fs.mkdirSync(ws, { recursive: true });
		fs.mkdirSync(sibling, { recursive: true });
		fs.writeFileSync(path.join(sibling, "secret.md"), "outside, sharing the workspace prefix\n");
		fs.writeFileSync(path.join(parent, "outside.md"), "outside, above the workspace root\n");

		write("docs/guide.md", GUIDE);
		write("docs/sub/deep.md", "deep in the tree\n");
		write("notes.txt", PLAIN);
		write("empty.md", "");
		write("LICENSE", "text with no extension at all\n");
		write("data.bin", Buffer.from([0x00, 0x01, 0x02, 0xff]));
		write("broken.md", Buffer.from([0xff, 0xfe, 0x41]));
		write("control.md", "a terminal escape \u001b[31m and a NUL \u0000\n");
		write("locked.md", "not for the preview\n");
		fs.chmodSync(path.join(ws, "locked.md"), 0o000);
		write("huge.md", Buffer.alloc(REMOTE_MAX_DOCUMENT_BYTES + 1, 0x61));
		write("newlines.md", "\n".repeat(WIDE_LINES));
		write("partial.txt", "# line\n".repeat(Math.ceil(FITTING_LINES / 7)).slice(0, FITTING_LINES));
		// Named like a document, so the reply is about the type and not the name.
		fs.mkdirSync(path.join(ws, "notes-dir.md"));
		link("inner", "notes-dir-link.md");

		link("inner/target.md", "alias.md");
		write("inner/target.md", LINKED);
		write("inner/sub/deep.md", "through the linked directory\n");
		link("inner", "dirlink");
		link(path.join(ws, "docs", "guide.md"), "abs.md");
		link(physical("ws/docs/guide.md"), "canonical.md");
		link("/etc/hosts", "out.md");
		link("../outside.md", "up.md");
		link(path.join(sibling, "secret.md"), "sameprefix.md");
		link(`${physical("ws")}-evil/secret.md`, "canon-prefix.md");
		link("hop2.md", "hop.md");
		link("/etc/hosts", "hop2.md");
		link("loopb.md", "loop.md");
		link("loop.md", "loopb.md");
		link("dirloopb", "dirloop");
		link("dirloop", "dirloopb");
		if (fs.existsSync("/usr/bin/mkfifo")) execFileSync("/usr/bin/mkfifo", [path.join(ws, "pipe")]);
		fs.symlinkSync(ws, path.join(parent, "wslink"));
	});

	afterAll(() => {
		fs.chmodSync(path.join(ws, "locked.md"), 0o600);
		fs.rmSync(parent, { recursive: true, force: true });
	});

	test("the capability is offered on a macOS host", () => {
		expect(remoteDocumentSupported).toBe(true);
	});

	test("reads a workspace-relative document", async () => {
		await expectText("docs/guide.md", GUIDE, "text/markdown");
		const result = await readRemoteDocument(ws, "docs/guide.md");
		if (!result.ok) throw new Error("expected a document");
		expect(result.data.path).toBe(path.join(ws, "docs/guide.md"));
	});

	test("reads plain text, an empty file, and an absolute path with noise", async () => {
		await expectText("notes.txt", PLAIN, "text/plain");
		await expectText("empty.md", "", "text/markdown");
		await expectText(`${ws}/./docs//guide.md`, GUIDE, "text/markdown");
	});

	test("follows links that stay inside: relative, directory, absolute, physical", async () => {
		await expectText("alias.md", LINKED, "text/markdown");
		await expectText("dirlink/target.md", LINKED, "text/markdown");
		await expectText("abs.md", GUIDE, "text/markdown");
		await expectText("canonical.md", GUIDE, "text/markdown");
		await expectText("dirlink/sub/deep.md", "through the linked directory\n", "text/markdown");
	});

	test("answers `..` where it is reached, so an internal link cannot trap the walk", async () => {
		await expectText("docs/../notes.txt", PLAIN, "text/plain");
		await expectText("docs/sub/../guide.md", GUIDE, "text/markdown");
		// `dirlink` is `inner`, so this is `inner/../notes.txt`: the workspace
		// file, exactly as the kernel resolves it.
		await expectText("dirlink/../notes.txt", PLAIN, "text/plain");
	});

	test("refuses to leave the workspace, by every route a path can take", async () => {
		await expectCode("../outside.md", "preview_outside_workspace");
		await expectCode("docs/../../outside.md", "preview_outside_workspace");
		await expectCode(path.join(parent, "outside.md"), "preview_outside_workspace");
		// A sibling whose name starts with the workspace name: same prefix, and
		// not the same directory.
		await expectCode(path.join(sibling, "secret.md"), "preview_outside_workspace");
	});

	test("refuses a link out, absolute, relative, multi-hop or same-prefix", async () => {
		await expectCode("out.md", "preview_outside_workspace");
		await expectCode("up.md", "preview_outside_workspace");
		await expectCode("sameprefix.md", "preview_outside_workspace");
		await expectCode("canon-prefix.md", "preview_outside_workspace");
		await expectCode("hop.md", "preview_outside_workspace");
		// The escape is only visible once the link is followed: the reply must
		// not be the plain file's text.
		await expectCode("dirlink/../../outside.md", "preview_outside_workspace");
	});

	test("reports nothing to read for a type the preview has no view for", async () => {
		await expectCode("docs", "preview_unsupported");
		await expectCode("docs/", "preview_unsupported");
		// A directory and a link to one, both named like a document: refused for
		// the type the pinned descriptor holds, past the name check.
		await expectCode("notes-dir.md", "preview_unsupported");
		await expectCode("notes-dir-link.md", "preview_unsupported");
		await expectCode("LICENSE", "preview_unsupported");
		await expectCode("data.bin", "preview_unsupported");
		await expectCode("broken.md", "preview_unsupported");
		await expectCode("control.md", "preview_unsupported");
		if (fs.existsSync("/usr/bin/mkfifo")) {
			// `O_NONBLOCK` is what keeps this a reply instead of a hang: the
			// fifo has no writer and never will.
			await expectCode("pipe", "preview_unsupported");
		}
	});

	test("separates what is not there from what is not readable", async () => {
		await expectCode("missing.md", "preview_not_found");
		await expectCode("docs/missing/deep.md", "preview_not_found");
		// A component that stops resolving: `notes.txt` is a file.
		await expectCode("notes.txt/deep.md", "preview_not_found");
		if (process.getuid?.() !== 0) await expectCode("locked.md", "preview_permission_denied");
	});

	test("bounds the document before reading and while reading", async () => {
		await expectCode("huge.md", "preview_too_large");
		// Fits the document bound to the byte; will not fit one frame once the
		// newlines are escaped.
		expect(fs.statSync(path.join(ws, "newlines.md")).size).toBe(REMOTE_MAX_DOCUMENT_BYTES);
		await expectCode("newlines.md", "preview_too_large");
		await expectText(
			"partial.txt",
			"# line\n".repeat(Math.ceil(FITTING_LINES / 7)).slice(0, FITTING_LINES),
			"text/plain",
		);
	});

	test("reports a link chain that never settles instead of walking it", async () => {
		await expectCode("loop.md", "preview_changed");
		await expectCode("dirloop/deep.md", "preview_changed");
	});

	test("rejects a malformed request and an anchor that cannot anchor", async () => {
		await expectCode("", "invalid_request");
		// A NUL is what a client that never validated its own string sends.
		await expectCode(`a${String.fromCharCode(0)}b.md`, "invalid_request");
		await expectCode(`${"x".repeat(300)}.md`, "invalid_request");
		await expectCode(Array.from({ length: 2100 }, () => "ab").join("/"), "invalid_request");
		// A workspace this module could only anchor by resolving it first.
		for (const workspace of ["relative/workspace", `${parent}/../ws`]) {
			const result = await readRemoteDocument(workspace, "docs/guide.md");
			expect(result.ok).toBe(false);
			if (!result.ok) expect(result.code).toBe("internal_error");
		}
		// The workspace itself is a link, so there is nothing to anchor to.
		const linked = await readRemoteDocument(path.join(parent, "wslink"), "docs/guide.md");
		expect(linked.ok).toBe(false);
		if (!linked.ok) expect(linked.code).toBe("internal_error");
	});

	test("re-reads current bytes instead of the last reply", async () => {
		await expectText("notes.txt", PLAIN, "text/plain");
		write("notes.txt", "changed on the remote host\n");
		await expectText("notes.txt", "changed on the remote host\n", "text/plain");
		write("notes.txt", PLAIN);
	});

	test("keeps the original workspace pinned after a replacement directory is linked at its name", () => {
		const anchor = path.join(parent, "anchor");
		const moved = path.join(parent, "moved");
		fs.mkdirSync(anchor);
		fs.writeFileSync(path.join(anchor, "secret.md"), "original workspace\n");
		const reader = openRemoteDocumentWorkspace(anchor);
		if (!reader) throw new Error("failed to pin test workspace");
		try {
			fs.renameSync(anchor, moved);
			fs.symlinkSync(sibling, anchor);
			expect(reader.read("secret.md")).toMatchObject({ ok: true, data: { content: "original workspace\n" } });
			expect(openRemoteDocumentWorkspace(anchor)).toBeNull();
		} finally {
			reader.close();
		}
	});

	test("leaves no descriptor behind, whatever the reply was", async () => {
		// One pass to warm every path below, so the baseline is taken with
		// nothing left to lazily open, then a measured pass over a mix of every
		// failure shape: the error paths are where a descriptor gets forgotten.
		const mix = async (): Promise<void> => {
			await readRemoteDocument(ws, "docs/guide.md");
			await readRemoteDocument(ws, "missing.md");
			await readRemoteDocument(ws, "out.md");
			await readRemoteDocument(ws, "docs");
			await readRemoteDocument(ws, "locked.md");
			await readRemoteDocument(ws, "loop.md");
			await readRemoteDocument(ws, "huge.md");
			await readRemoteDocument(ws, "notes.txt/deep.md");
			await readRemoteDocument(path.join(parent, "wslink"), "docs/guide.md");
		};
		await mix();
		const before = openDescriptors();
		for (let attempt = 0; attempt < 8; attempt++) await mix();
		expect(openDescriptors()).toEqual(before);
	});
});
