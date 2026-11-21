import { afterAll, describe, expect, test } from "bun:test";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import type { SessionInfo } from "../src/session/session-listing";
import { createWorkspaceSessionCatalog } from "../src/modes/rpc/remote-sessions";
import { REMOTE_MAX_PAGE_ITEMS } from "../src/modes/rpc/remote-wire";

const roots: string[] = [];
afterAll(async () => {
	await Promise.all(roots.map(root => fs.rm(root, { recursive: true, force: true })));
});

async function workspace(tag: string): Promise<string> {
	const root = await fs.realpath(await fs.mkdtemp(path.join(os.tmpdir(), `omp-remote-catalog-${tag}-`)));
	roots.push(root);
	return root;
}

function info(overrides: Partial<SessionInfo> & Pick<SessionInfo, "id" | "path">): SessionInfo {
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

describe("remote session catalog", () => {
	test("lists only header-verified workspace sessions, as metadata only", async () => {
		const home = await workspace("home");
		const foreign = await workspace("foreign");
		const pinned = path.join(home, "pinned");
		const catalog = createWorkspaceSessionCatalog(home, {
			sessionDir: pinned,
			listStored: async (cwd, sessionDir) => {
				expect(cwd).toBe(home);
				expect(sessionDir).toBe(pinned);
				return [
					info({ id: "ses-a", cwd: home, path: path.join(pinned, "a.jsonl"), title: "Alpha" }),
					// Header cwd names the workspace through a path that only resolves equal after realpath.
					info({ id: "ses-b", cwd: `${home}/.`, path: path.join(pinned, "b.jsonl") }),
					info({ id: "ses-c", cwd: foreign, path: path.join(pinned, "c.jsonl"), title: "Foreign" }),
					info({ id: "ses-d", cwd: "", path: path.join(pinned, "d.jsonl") }),
					info({ id: "ses-e", cwd: "/nonexistent-workspace-xyz", path: path.join(pinned, "e.jsonl") }),
					// Escapes the pinned dir; a symlinked name would not count either.
					info({ id: "ses-f", cwd: home, path: path.join(home, "escape.jsonl") }),
					// Lives inside the pinned dir but is not a session file.
					info({ id: "ses-g", cwd: home, path: path.join(pinned, "notes.txt") }),
				];
			},
		});
		const page = await catalog.list();
		expect(page.cursor).toBeNull();
		// Metadata only: never a path, never a message body, in Core listing order.
		expect(page.sessions).toEqual([
			{ sessionId: "ses-a", name: "Alpha", updatedAt: "2026-01-02T00:00:00.000Z" },
			{ sessionId: "ses-b", name: null, updatedAt: "2026-01-02T00:00:00.000Z" },
		]);
		expect(await catalog.resolve("ses-c")).toBeUndefined();
		expect(await catalog.resolve("ses-d")).toBeUndefined();
		expect(await catalog.resolve("ses-e")).toBeUndefined();
		expect(await catalog.resolve("ses-f")).toBeUndefined();
		expect(await catalog.resolve("ses-x")).toBeUndefined();
	});

	test("resolves an opaque id to the pinned-dir file an open needs, nothing else", async () => {
		const home = await workspace("resolve");
		const pinned = path.join(home, "pinned");
		const target = path.join(pinned, "real.jsonl");
		const catalog = createWorkspaceSessionCatalog(home, {
			sessionDir: pinned,
			listStored: async () => [info({ id: "ses-1", cwd: home, path: target })],
		});
		expect(await catalog.resolve("ses-1")).toEqual({ path: target });
	});

	test("an ambiguous id resolves to nothing rather than guessing a file", async () => {
		const home = await workspace("dup");
		const pinned = path.join(home, "pinned");
		const catalog = createWorkspaceSessionCatalog(home, {
			sessionDir: pinned,
			listStored: async () => [
				info({ id: "ses-dup", cwd: home, path: path.join(pinned, "one.jsonl") }),
				info({ id: "ses-dup", cwd: home, path: path.join(pinned, "two.jsonl") }),
			],
		});
		expect(await catalog.resolve("ses-dup")).toBeUndefined();
	});

	test("pages by cursor and stops exactly at the bound", async () => {
		const home = await workspace("page");
		const pinned = path.join(home, "pinned");
		const total = REMOTE_MAX_PAGE_ITEMS + 2;
		const catalog = createWorkspaceSessionCatalog(home, {
			sessionDir: pinned,
			listStored: async () =>
				Array.from({ length: total }, (_, index) =>
					info({
						id: `ses-${index}`,
						cwd: home,
						path: path.join(pinned, `${index}.jsonl`),
						modified: new Date(Date.UTC(2026, 0, 1, 0, 0, index)),
					}),
				),
		});
		const first = await catalog.list();
		expect(first.sessions).toHaveLength(REMOTE_MAX_PAGE_ITEMS);
		expect(first.sessions[0]?.sessionId).toBe("ses-0");
		expect(first.cursor).toBe(String(REMOTE_MAX_PAGE_ITEMS));
		const second = await catalog.list(REMOTE_MAX_PAGE_ITEMS);
		expect(second.sessions.map(row => row.sessionId)).toEqual([
			`ses-${REMOTE_MAX_PAGE_ITEMS}`,
			`ses-${REMOTE_MAX_PAGE_ITEMS + 1}`,
		]);
		expect(second.cursor).toBeNull();
		const past = await catalog.list(total);
		expect(past.sessions).toEqual([]);
		expect(past.cursor).toBeNull();
	});
});
