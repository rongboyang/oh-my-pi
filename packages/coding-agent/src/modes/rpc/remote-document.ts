/**
 * The `read_document` capability: one document, read from the instance's own
 * workspace and nothing else.
 *
 * The request is a path string that arrived from another machine, and the tree
 * it names belongs to an account that can run anything on this host. So the
 * checks cannot be a `realpath` followed by a `readFile`: between the
 * resolution and the open, any component can be swapped for a symlink to
 * `~/.ssh/id_rsa`, and the second lookup follows it happily. This module never
 * resolves a name twice for the same component. It keeps one descriptor per
 * directory it has consented to enter and opens every next component with
 * `openat` against that descriptor, so a swap can only ever yield another
 * component these same rules would have to accept.
 *
 * The rules, in short:
 * - **Anchor.** One root fd, opened `O_NOFOLLOW | O_DIRECTORY`: the workspace
 *   itself must be a real directory, not a link pointed at one.
 * - **Descend.** Each component opens `O_NOFOLLOW`, plus `O_DIRECTORY` when
 *   more components follow, so the type is decided at open time rather than
 *   against a second lookup. `O_NONBLOCK` keeps a planted fifo or device from
 *   blocking the Agent's thread; `O_CLOEXEC` keeps preview descriptors out of
 *   every build omp spawns.
 * - **Contain.** A symlink is read with `readlinkat` against that same parent
 *   descriptor and re-walked from there. An absolute target must name the
 *   workspace on a component boundary — `/ws-evil/x` shares `/ws` as a string
 *   prefix and is still outside — and `..` pops the descriptor stack, refusing
 *   at the root instead of opening `..` above it. Links cannot leave; the
 *   sanctioned internal link still works.
 * - **Pin.** Only the final descriptor is `fstat`ed and read, as a regular file
 *   of a bounded size, and `fstat`ed again afterwards, so a file truncated or
 *   grown mid-read is reported changed instead of served half-written.
 * - **Classify.** A failure becomes one of the `preview_*` codes, never a
 *   message: the client renders a reason, and a path is not worth echoing.
 *
 * macOS only, deliberately: it drives Darwin's `openat` and `readlinkat`
 * through `bun:ffi`, the way `raiseInteractiveQos` in `@oh-my-pi/pi-utils`
 * drives libSystem for QoS. {@link remoteDocumentSupported} is false elsewhere,
 * and no dylib is opened on a platform whose ABI this walker does not know.
 */
import { dlopen, FFIType, ptr } from "bun:ffi";
import * as fs from "node:fs";
import {
	REMOTE_MAX_DOCUMENT_BYTES,
	REMOTE_MAX_FRAME_BYTES,
	REMOTE_MAX_PATH_LENGTH,
	type RemoteDocumentResult,
	type RemoteErrorCode,
} from "./remote-wire";

/** The document, or the stable code that says why there is none. */
export type RemoteDocumentRead = { ok: true; data: RemoteDocumentResult } | { ok: false; code: RemoteErrorCode };

// Darwin `fcntl.h`. These are the values Darwin's ABI reads, not the host's
// `fs.constants`, so the flags keep their meaning wherever this file is
// bundled — even where the FFI below is never reached.
const O_RDONLY = 0;
/** Never block opening a fifo or a device another local user planted. */
const O_NONBLOCK = 0x4;
/** Refuse a symlinked final component: ELOOP at open, never a follow. */
const O_NOFOLLOW = 0x100;
const O_NOFOLLOW_ANY = 0x2000_0000;
/** Settle "is a directory" inside the open, not against a second lookup. */
const O_DIRECTORY = 0x100_000;
/** Keep preview descriptors out of the processes omp spawns. */
const O_CLOEXEC = 0x0100_0000;
/** `AT_FDCWD`, for the one open that starts from the workspace path itself. */
const AT_FDCWD = -2;

/** APFS/HFS+ component limit: a longer name cannot exist, so do not try it. */
const MAX_NAME_BYTES = 255;
/** `PATH_MAX`: the buffer `readlinkat` gets, and the truncation tell. */
const LINK_BUFFER_BYTES = 1024;
/**
 * Opens and link resolutions allowed per read. Both bounds only bite on a tree
 * built to spend them: a path of at most {@link REMOTE_MAX_PATH_LENGTH} bytes
 * walks in a few dozen components, and a repository chains a handful of links.
 */
const MAX_WALK_STEPS = 512;
const MAX_LINK_RESOLVES = 16;
/** Read granularity inside the document bound. */
const READ_CHUNK_BYTES = 64 * 1024;
/**
 * Room left in a frame for the envelope, the ids and the echoed path, so a
 * reply this module accepts is one the negotiated frame limit can carry.
 */
const FRAME_SLACK_BYTES = 8 * 1024;

/**
 * The names the GUI has a view for, by extension. Everything else — an archive,
 * an image, a lock file, a file with no extension at all — is
 * `preview_unsupported`, because guessing from bytes would either render a
 * binary or read a type the renderer was never meant to see.
 */
const MEDIA_TYPE_BY_EXTENSION: Record<string, "text/markdown" | "text/plain"> = {
	adoc: "text/plain",
	asc: "text/plain",
	bash: "text/plain",
	c: "text/plain",
	cc: "text/plain",
	cfg: "text/plain",
	cjs: "text/plain",
	conf: "text/plain",
	cpp: "text/plain",
	csv: "text/plain",
	diff: "text/plain",
	fish: "text/plain",
	go: "text/plain",
	graphql: "text/plain",
	h: "text/plain",
	hpp: "text/plain",
	ini: "text/plain",
	java: "text/plain",
	js: "text/plain",
	json: "text/plain",
	jsonc: "text/plain",
	jsonl: "text/plain",
	kt: "text/plain",
	log: "text/plain",
	markdown: "text/markdown",
	md: "text/markdown",
	mdown: "text/markdown",
	mdk: "text/markdown",
	mjs: "text/plain",
	mts: "text/plain",
	mdx: "text/markdown",
	ndjson: "text/plain",
	patch: "text/plain",
	php: "text/plain",
	pl: "text/plain",
	properties: "text/plain",
	proto: "text/plain",
	py: "text/plain",
	rb: "text/plain",
	rs: "text/plain",
	sh: "text/plain",
	sql: "text/plain",
	swift: "text/plain",
	text: "text/plain",
	ts: "text/plain",
	tsv: "text/plain",
	tsx: "text/plain",
	txt: "text/plain",
	xml: "text/plain",
	yaml: "text/plain",
	yml: "text/plain",
	zsh: "text/plain",
};

interface DarwinFs {
	/** The new descriptor, or -1. `mode` is only read alongside `O_CREAT`. */
	openat(dirfd: number, path: string, flags: number, mode: number): number;
	/** Target length in bytes, or -1; a full buffer means a truncated target. */
	readlinkat(dirfd: number, path: string, buffer: Uint8Array): number;
}

let darwin: DarwinFs | null | undefined;

function loadDarwin(): DarwinFs | null {
	if (darwin !== undefined) return darwin;
	darwin = null;
	if (process.platform !== "darwin") return darwin;
	try {
		// Kept for the process's life: `close()` invalidates the call pointers
		// bun:ffi built for them, and libSystem never unloads anyway.
		const lib = dlopen("/usr/lib/libSystem.B.dylib", {
			openat: { args: [FFIType.i32, FFIType.cstring, FFIType.i32, FFIType.i32], returns: FFIType.i32 },
			readlinkat: { args: [FFIType.i32, FFIType.cstring, FFIType.ptr, FFIType.u64], returns: FFIType.i64 },
		});
		darwin = {
			openat(dirfd, path, flags, mode) {
				// `openat` takes `const char *`: hand it NUL-terminated UTF-8.
				// No path reaches here with a NUL of its own — the request
				// validator and the kernel both rule that out.
				const name = Buffer.from(`${path}\0`, "utf8");
				return lib.symbols.openat(dirfd, ptr(name), flags, mode);
			},
			readlinkat(dirfd, path, buffer) {
				const name = Buffer.from(`${path}\0`, "utf8");
				// `ssize_t` arrives as a BigInt; a link target always fits a
				// number, and -1 stays -1.
				return Number(lib.symbols.readlinkat(dirfd, ptr(name), ptr(buffer), buffer.length));
			},
		};
	} catch {
		// No bun:ffi, or no libSystem under that name: capability absent.
		darwin = null;
	}
	return darwin;
}

/**
 * Whether this host can serve `read_document`. False off macOS, and on macOS
 * only once libSystem's `openat` and `readlinkat` have resolved — so a caller
 * offering the capability is a caller whose offer the host can honor.
 */
export const remoteDocumentSupported: boolean = loadDarwin() !== null;

const fail = (code: RemoteErrorCode): RemoteDocumentRead => ({ ok: false, code });

const closeQuietly = (fd: number): void => {
	try {
		fs.closeSync(fd);
	} catch {
		// Already closed, or never opened: nothing is left either way.
	}
};

/** `/ws` for a workspace, `/` for the whole disk; null when unusable. */
function normalizeWorkspace(workspace: string): string | null {
	if (typeof workspace !== "string" || workspace.length === 0 || workspace.includes("\0")) return null;
	if (!workspace.startsWith("/")) return null;
	const parts = workspace.split("/").filter(part => part !== "" && part !== ".");
	// An anchor with a `..` in it has no fixed meaning until something resolves
	// it, and resolving it means trusting a lookup of the very thing to pin.
	if (parts.includes("..")) return null;
	return parts.length === 0 ? "/" : `/${parts.join("/")}`;
}

/** `root/…`, without the doubled slash a root workspace would get. */
function joinWorkspace(root: string, parts: readonly string[]): string {
	return parts.length === 0 ? root : `${root === "/" ? "" : root}/${parts.join("/")}`;
}

/**
 * Strip the workspace off an absolute path, on a component boundary only.
 * Null when it is elsewhere: sharing `/ws` as a prefix with `/ws-evil` is not
 * sharing the workspace.
 */
function stripWorkspace(root: string, absolute: string): string | null {
	if (root === "/") return absolute.slice(1);
	if (absolute === root) return "";
	if (!absolute.startsWith(root)) return null;
	const rest = absolute.slice(root.length);
	return rest.startsWith("/") ? rest.slice(1) : null;
}

/**
 * Components to walk: empty and `.` parts gone, `..` kept so the descriptor
 * stack answers it where it is reached — after any link before it has been
 * followed, which is the order the kernel resolves a path in. Null when a
 * component is longer than the filesystem allows.
 */
function splitParts(body: string): string[] | null {
	const parts: string[] = [];
	for (const part of body.split("/")) {
		if (part === "" || part === ".") continue;
		if (Buffer.byteLength(part, "utf8") > MAX_NAME_BYTES) return null;
		parts.push(part);
	}
	return parts;
}

/**
 * Whether the mode grants this process the read the open refused. `lstat` only
 * needs the directories along the way searched, so an entry can be nameable and
 * still unopenable; the triad picks the bits that applied to us.
 */
function readableByMode(stat: fs.Stats): boolean {
	const uid = process.getuid?.();
	// Root overrides the mode, and a host without an id cannot be blamed for
	// whatever the open said.
	if (uid === undefined || uid === 0) return true;
	if (stat.uid === uid) return (stat.mode & 0o400) !== 0;
	const gid = process.getgid?.();
	const groups = process.getgroups?.() ?? [];
	if (gid !== undefined && stat.gid === gid) return (stat.mode & 0o040) !== 0;
	if (groups.includes(stat.gid)) return (stat.mode & 0o040) !== 0;
	return (stat.mode & 0o004) !== 0;
}

/** What the refused component had to be to keep the path resolving. */
type LookupKind = "root" | "dir" | "file";

/**
 * The wire code for a failed lookup. The open already refused this path; this
 * only labels the refusal, the way `assertOwnerPrivateDir` lstats a path it has
 * already rejected. A swap afterwards cannot widen what was granted, because
 * this grants nothing.
 */
function classifyLookup(path: string, kind: LookupKind): RemoteErrorCode {
	let stat: fs.Stats;
	try {
		stat = fs.lstatSync(path);
	} catch (err) {
		switch ((err as NodeJS.ErrnoException).code) {
			case "ENOENT":
			case "ENOTDIR":
			case "EISDIR":
			case "ENXIO":
				return "preview_not_found";
			case "EACCES":
			case "EPERM":
				return "preview_permission_denied";
			case "ENAMETOOLONG":
				return "invalid_request";
			default:
				return "internal_error";
		}
	}
	// It is there, so the open failed for a reason the codes can name: a link
	// swapped in under us, a read the mode denies, or an entry with no view.
	if (stat.isSymbolicLink()) return kind === "root" ? "internal_error" : "preview_changed";
	if (!readableByMode(stat)) return "preview_permission_denied";
	if (kind === "root") return "internal_error";
	// Along the way, a plain file is a path that stops resolving: not there.
	if (kind === "dir") return stat.isFile() ? "preview_not_found" : "preview_unsupported";
	// At the end, a directory has no bytes to show, and a readable regular file
	// that still refused to open failed on us, not on the request: descriptor
	// pressure is not something the user should be told to fix about the file.
	if (stat.isDirectory()) return "preview_unsupported";
	return "internal_error";
}

/** The view a name maps to, or null when the GUI has none for it. */
function mediaTypeFor(name: string): "text/markdown" | "text/plain" | null {
	const base = name.slice(name.lastIndexOf("/") + 1).toLowerCase();
	const dot = base.lastIndexOf(".");
	// A leading dot is a name, not an extension: `.gitignore` gets no type.
	if (dot <= 0) return null;
	return MEDIA_TYPE_BY_EXTENSION[base.slice(dot + 1)] ?? null;
}

/**
 * Extra bytes JSON spends on `bytes`, or -1 when they are not previewable text.
 * Tab, LF and CR are the only controls let through: the rest of C0, NUL and ESC
 * included, plus DEL, marks a binary or terminal-hostile stream, which is a
 * type with no view rather than a read that failed. UTF-8 continuation bytes are
 * >= 0x80, so this pass over bytes is exact.
 */
function jsonWidening(bytes: Buffer): number {
	let widening = 0;
	for (let index = 0; index < bytes.length; index++) {
		const byte = bytes[index] as number;
		if (byte === 0x22 || byte === 0x5c) widening += 1;
		else if (byte < 0x20) {
			if (byte !== 0x09 && byte !== 0x0a && byte !== 0x0d) return -1;
			widening += 1;
		} else if (byte === 0x7f) return -1;
	}
	return widening;
}

/**
 * Read the pinned descriptor: a regular file with a previewable name and a
 * bounded size, strict UTF-8, and only if it held still for the whole read.
 * `path` is the workspace path the caller asked for, echoed as read.
 */
function readPinned(fd: number, name: string, path: string, maxFrameBytes: number): RemoteDocumentRead {
	const mediaType = mediaTypeFor(name);
	if (mediaType === null) return fail("preview_unsupported");
	let before: fs.Stats;
	try {
		before = fs.fstatSync(fd);
	} catch {
		return fail("internal_error");
	}
	// The type question the final open could not ask. Only a regular file has
	// previewable bytes, and this is the inode that open named — a fifo, a
	// device or a socket opened under `O_NONBLOCK` ends here, not in a hang.
	if (!before.isFile()) return fail("preview_unsupported");
	if (before.size > REMOTE_MAX_DOCUMENT_BYTES) return fail("preview_too_large");

	const chunks: Buffer[] = [];
	let total = 0;
	for (;;) {
		const room = REMOTE_MAX_DOCUMENT_BYTES + 1 - total;
		if (room <= 0) return fail("preview_too_large");
		const chunk = Buffer.allocUnsafe(Math.min(READ_CHUNK_BYTES, room));
		const read = fs.readSync(fd, chunk, 0, chunk.length, null);
		if (read === 0) break;
		total += read;
		// Grown past the bound while it was being read.
		if (total > REMOTE_MAX_DOCUMENT_BYTES) return fail("preview_too_large");
		chunks.push(chunk.subarray(0, read));
	}
	let after: fs.Stats;
	try {
		after = fs.fstatSync(fd);
	} catch {
		return fail("internal_error");
	}
	// The inode is pinned, so a moved size or mtime is a write read straight
	// through: half a document must not render as a whole one.
	if (after.size !== before.size || after.mtimeMs !== before.mtimeMs || total !== before.size) {
		return fail("preview_changed");
	}

	const bytes = chunks.length === 1 ? (chunks[0] as Buffer) : Buffer.concat(chunks);
	const widening = jsonWidening(bytes);
	if (widening < 0) return fail("preview_unsupported");
	if (bytes.length + widening > maxFrameBytes - FRAME_SLACK_BYTES) return fail("preview_too_large");
	let content: string;
	try {
		content = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
	} catch {
		return fail("preview_unsupported");
	}
	return { ok: true, data: { path, content, mediaType } };
}

/**
 * Read one workspace document. `workspace` is the instance's own root, taken
 * from Core and not from the request; `requestedPath` is either absolute, in
 * which case it must name that workspace, or relative to it.
 *
 * The walk, the type check and the read are synchronous on purpose: no await
 * sits between the descriptor that was validated and the bytes that were read,
 * so nothing in this process can move a component between them, and the whole
 * read is bounded by {@link REMOTE_MAX_DOCUMENT_BYTES}.
 */
function readUnder(
	api: DarwinFs,
	root: string,
	rootFd: number,
	requestedPath: string,
	maxFrameBytes: number,
): RemoteDocumentRead {
	if (typeof requestedPath !== "string" || requestedPath.length === 0) return fail("invalid_request");
	if (requestedPath.includes("\0") || requestedPath.includes("\r") || requestedPath.includes("\n")) {
		return fail("invalid_request");
	}
	if (Buffer.byteLength(requestedPath, "utf8") > REMOTE_MAX_PATH_LENGTH) return fail("invalid_request");

	const absolute = requestedPath.startsWith("/");
	const contained = (target: string): string | null => stripWorkspace(root, target);

	let body = requestedPath;
	if (absolute) {
		const stripped = contained(requestedPath);
		if (stripped === null) return fail("preview_outside_workspace");
		body = stripped;
	}
	const initial = splitParts(body);
	if (initial === null) return fail("invalid_request");
	const name = initial[initial.length - 1] ?? "";

	// Every descriptor this call owns, deepest last, with the components that
	// led to each: the stack is the containment proof, the trail only labels a
	// lookup for {@link classifyLookup}.
	const stack: number[] = [rootFd];
	const trail: string[] = [];
	let file = -1;
	let steps = 0;
	let links = 0;
	try {
		let pending = initial;
		while (pending.length > 0) {
			const component = pending[0] as string;
			pending = pending.slice(1);
			if (++steps > MAX_WALK_STEPS) return fail("overflow");
			if (component === "..") {
				// The parent of the root is outside it, and `..` is the one
				// component no `O_NOFOLLOW` can make safe: settle it here.
				if (stack.length === 1 && root !== "/") return fail("preview_outside_workspace");
				if (stack.length > 1) closeQuietly(stack.pop() as number);
				trail.pop();
				continue;
			}
			const dirfd = stack[stack.length - 1] as number;
			const mustBeDir = pending.length > 0;
			const kind: LookupKind = mustBeDir ? "dir" : "file";
			const flags = O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | (mustBeDir ? O_DIRECTORY : 0);
			const opened = api.openat(dirfd, component, flags, 0);
			if (opened >= 0) {
				trail.push(component);
				if (mustBeDir) {
					stack.push(opened);
					continue;
				}
				file = opened;
				continue;
			}
			// Not an open: either a link, which this very descriptor can read,
			// or a real failure. Only a `readlinkat` that succeeded grants a
			// follow, so a lookup that cannot say what it is never gets walked.
			const target = readLink(api, dirfd, component);
			if (target === null) return fail(classifyLookup(joinWorkspace(root, trail.concat(component)), kind));
			if (++links > MAX_LINK_RESOLVES) return fail("preview_changed");
			const applied = applyLink(target);
			if (!applied.ok) return applied;
			pending = applied.segments.concat(pending);
		}
		// Nothing left to open without a file in hand: the path, or a link along
		// it, named a directory, and a directory has no bytes to preview.
		if (file < 0) return fail("preview_unsupported");
		return readPinned(file, name, joinWorkspace(root, trail), maxFrameBytes);
	} catch (err) {
		// Nothing here may throw across the dispatch: the reply is a code.
		const code = (err as NodeJS.ErrnoException).code;
		if (code === "ENOENT" || code === "ENOTDIR") return fail("preview_not_found");
		if (code === "EACCES" || code === "EPERM") return fail("preview_permission_denied");
		if (code === "ENAMETOOLONG") return fail("invalid_request");
		return fail("internal_error");
	} finally {
		if (file >= 0) closeQuietly(file);
		for (let i = 1; i < stack.length; i++) closeQuietly(stack[i]);
	}

	/**
	 * The target of `component` under `dirfd`, or null when it is not a readable
	 * link. A full buffer means a target at least that long: refused rather than
	 * walked as a truncated name that points somewhere else.
	 */
	function readLink(at: DarwinFs, dirfd: number, component: string): string | null {
		const buffer = new Uint8Array(LINK_BUFFER_BYTES);
		const length = at.readlinkat(dirfd, component, buffer);
		if (length < 0 || length >= buffer.length) return null;
		try {
			return new TextDecoder("utf-8", { fatal: true }).decode(buffer.subarray(0, length));
		} catch {
			return null;
		}
	}

	/**
	 * Where a link leads, as components to walk from the descriptor the walk
	 * stands on. An absolute target restarts at the root, after proving it names
	 * this workspace; a relative one keeps standing in the directory that held
	 * the link, and its `..` parts stay in the list for the stack to answer with
	 * the root as their floor.
	 */
	function applyLink(target: string): { ok: true; segments: string[] } | { ok: false; code: RemoteErrorCode } {
		if (target.length === 0) return { ok: false, code: "preview_unsupported" };
		let body = target;
		if (target.startsWith("/")) {
			const stripped = contained(target);
			if (stripped === null) return { ok: false, code: "preview_outside_workspace" };
			while (stack.length > 1) closeQuietly(stack.pop() as number);
			trail.length = 0;
			body = stripped;
		}
		const segments = splitParts(body);
		if (segments === null) return { ok: false, code: "preview_unsupported" };
		return { ok: true, segments };
	}
}

export interface RemoteDocumentWorkspace {
	read(path: string, maxFrameBytes?: number): RemoteDocumentRead;
	close(): void;
}

/** Pin the instance's workspace before it is advertised to any client. */
export function openRemoteDocumentWorkspace(workspace: string): RemoteDocumentWorkspace | null {
	const api = loadDarwin();
	const root = normalizeWorkspace(workspace);
	if (api === null || root === null) return null;
	let fd = api.openat(AT_FDCWD, root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW_ANY | O_CLOEXEC | O_NONBLOCK, 0);
	if (fd < 0) return null;
	return {
		read: (path, maxFrameBytes = REMOTE_MAX_FRAME_BYTES) =>
			fd >= 0 ? readUnder(api, root, fd, path, maxFrameBytes) : fail("disconnected"),
		close() {
			if (fd >= 0) closeQuietly(fd);
			fd = -1;
		},
	};
}

export async function readRemoteDocument(workspace: string, path: string): Promise<RemoteDocumentRead> {
	if (!remoteDocumentSupported) return fail("unsupported");
	const reader = openRemoteDocumentWorkspace(workspace);
	if (!reader) return fail("internal_error");
	try {
		return reader.read(path);
	} finally {
		reader.close();
	}
}
