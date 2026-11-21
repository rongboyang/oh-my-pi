import * as fs from "node:fs/promises";
import * as path from "node:path";
import type { SessionInfo } from "../../session/session-listing";
import { SessionManager } from "../../session/session-manager";
import { REMOTE_MAX_PAGE_ITEMS, type RemoteSessionPage } from "./remote-wire";

/**
 * A Core-owned session target. The path crosses the instance boundary into the
 * RPC dispatch loop only; it never appears in a wire frame.
 */
export interface RemoteSessionTarget {
	readonly path: string;
}

/**
 * Core-owned lookup for the sessions of one bound workspace. Ids on the wire are
 * opaque Core session ids; resolving one re-reads the stored header and re-checks
 * the pinned directory, so a client can never name a path or another workspace.
 */
export interface RemoteSessionCatalog {
	/** One page of workspace session metadata, in Core order (pinned first). */
	list(cursor?: number): Promise<RemoteSessionPage>;
	/** Maps one opaque session id to its workspace-local target, or undefined. */
	resolve(sessionId: string): Promise<RemoteSessionTarget | undefined>;
}

export interface RemoteSessionCatalogOptions {
	/** Pinned session dir for this workspace; defaults to the canonical cwd-derived dir. */
	sessionDir?: string;
	/** Seam for reading stored sessions; defaults to Core's `SessionManager.list`. */
	listStored?: (cwd: string, sessionDir: string) => Promise<SessionInfo[]>;
}

/**
 * Sessions of exactly one workspace: the session dir is pinned at construction,
 * a row only counts when its header `cwd` resolves (`realpath`) to the canonical
 * workspace, and rows whose file escapes the pinned dir are dropped. A missing or
 * stale header cwd cannot prove workspace membership, so it never lists or opens.
 */
export class WorkspaceSessionCatalog implements RemoteSessionCatalog {
	readonly #workspace: string;
	readonly #sessionDir: string;
	readonly #listStored: (cwd: string, sessionDir: string) => Promise<SessionInfo[]>;

	constructor(workspace: string, options: RemoteSessionCatalogOptions = {}) {
		this.#workspace = workspace;
		this.#sessionDir = path.resolve(options.sessionDir ?? SessionManager.getDefaultSessionDir(workspace));
		this.#listStored =
			options.listStored ?? ((cwd, sessionDir) => SessionManager.list(cwd, sessionDir ?? this.#sessionDir));
	}

	async list(cursor?: number): Promise<RemoteSessionPage> {
		const rows = await this.#rows();
		const start = cursor ?? 0;
		const end = Math.min(start + REMOTE_MAX_PAGE_ITEMS, rows.length);
		return {
			sessions: rows.slice(start, end).map(row => ({
				sessionId: row.info.id,
				name: row.info.title ?? null,
				updatedAt: row.info.modified.toISOString(),
			})),
			cursor: end < rows.length ? String(end) : null,
		};
	}

	async resolve(sessionId: string): Promise<RemoteSessionTarget | undefined> {
		const rows = await this.#rows();
		const matches = rows.filter(row => row.info.id === sessionId);
		// An ambiguous id is a Core-side integrity problem; refusing is the only
		// way to keep "open this session" from meaning two different files.
		if (matches.length !== 1) return undefined;
		return { path: matches[0]!.info.path };
	}

	async #rows(): Promise<Array<{ info: SessionInfo }>> {
		const infos = await this.#listStored(this.#workspace, this.#sessionDir);
		const canonical = new Map<string, Promise<string | undefined>>();
		const resolveCwd = (cwd: string): Promise<string | undefined> => {
			if (!path.isAbsolute(cwd)) return Promise.resolve(undefined);
			let value = canonical.get(cwd);
			if (!value) {
				value = fs.realpath(cwd).catch(() => undefined);
				canonical.set(cwd, value);
			}
			return value;
		};
		const kept: Array<{ info: SessionInfo }> = [];
		for (const info of infos) {
			if (typeof info.id !== "string" || info.id.length === 0) continue;
			if (!this.#insidePinnedDir(info.path)) continue;
			if ((await resolveCwd(info.cwd)) !== this.#workspace) continue;
			kept.push({ info });
		}
		return kept;
	}

	#insidePinnedDir(candidate: string): boolean {
		if (!candidate.endsWith(".jsonl")) return false;
		const resolved = path.resolve(candidate);
		const relative = path.relative(this.#sessionDir, resolved);
		return (
			relative.length > 0 && relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative)
		);
	}
}

export function createWorkspaceSessionCatalog(
	workspace: string,
	options: RemoteSessionCatalogOptions = {},
): RemoteSessionCatalog {
	return new WorkspaceSessionCatalog(workspace, options);
}
