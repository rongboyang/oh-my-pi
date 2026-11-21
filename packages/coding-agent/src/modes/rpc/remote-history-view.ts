// Remote v1 read side (C3): views captured synchronously at sync. Everything
// here bounds the bytes that leave the instance per page or event, so a giant
// tool result stays an explicitly truncated, pageable state — never a frame
// the transport cannot serve and must not answer by dropping the attach.
import { REMOTE_MAX_OUTPUT_BYTES, REMOTE_MAX_PAGE_ITEMS, type RemoteOutputPage } from "./remote-wire";

/** One page of captured history; `cursor` is opaque and only valid for its view. */
export interface RemoteHistoryPage {
	items: readonly unknown[];
	cursor: string | null;
}

/** Maps a tool call to an archive id the `output` op can page, when one exists. */
export type RemoteOutputIdResolver = (toolCallId: string) => string | undefined;

/** Envelope, ids and separators kept outside the per-item byte budget. */
const WRAPPER_BYTES = 512;

const byteLength = (value: unknown): number => Buffer.byteLength(JSON.stringify(value) ?? "null");

function fitText(text: string, budgetBytes: number): string {
	if (budgetBytes <= 0) return "";
	// Measure the embedded form, not the raw string: JSON escapes grow the frame.
	if (byteLength(text) <= budgetBytes) return text;
	let low = 0;
	let high = Math.min(text.length, budgetBytes);
	while (low < high) {
		const mid = Math.ceil((low + high) / 2);
		if (byteLength(text.slice(0, mid)) <= budgetBytes) low = mid;
		else high = mid - 1;
	}
	const next = text.charCodeAt(low);
	return text.slice(0, next >= 0xdc00 && next <= 0xdfff ? Math.max(0, low - 1) : low);
}

function textOf(content: unknown): string {
	if (typeof content === "string") return content;
	if (Array.isArray(content))
		return content
			.map(block => {
				const value = (block ?? {}) as Record<string, unknown>;
				return value.type === "text" && typeof value.text === "string" ? value.text : `[${String(value.type)}]`;
			})
			.join("");
	return JSON.stringify(content ?? "");
}

function fields(value: unknown): Record<string, unknown> {
	return typeof value === "object" && value !== null ? (value as Record<string, unknown>) : {};
}

/** Copy identity scalars; bodies are replaced by the caller's bounded preview. */
function identity(item: Record<string, unknown>, keys: readonly string[]): Record<string, unknown> {
	const shell: Record<string, unknown> = {};
	for (const key of keys) {
		const value = item[key];
		if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") shell[key] = value;
	}
	return shell;
}

/**
 * Replace an item that cannot fit a page with an explicit truncated preview.
 * Tool results carry an archive reference when the output side has one; every
 * other oversized item degrades to a bounded fragment. Never returns an item
 * that silently omits content without saying so.
 */
export function boundedHistoryItem(item: unknown, maxBytes: number, resolveOutputId?: RemoteOutputIdResolver): unknown {
	if (byteLength(item) <= maxBytes) return item;
	const message = fields(item);
	const preview: Record<string, unknown> = {
		...identity(message, ["id", "role", "toolCallId", "toolName", "isError", "timestamp"]),
		truncated: true,
	};
	if (message.role === "toolResult" && typeof message.toolCallId === "string") {
		const outputId = resolveOutputId?.(message.toolCallId);
		if (outputId !== undefined) preview.outputId = outputId;
	}
	preview.preview = "";
	const budget = Math.max(0, maxBytes - byteLength(preview) - WRAPPER_BYTES);
	preview.preview = fitText(textOf(message.content ?? message.text ?? message), budget);
	return preview;
}

/**
 * Truncated stand-in for a live event whose body exceeds the peer's frame
 * limit. Identity fields survive so the reducer can still close the right
 * slot; the body becomes an explicit preview plus, for tools whose output the
 * archive recorded, a reference the `output` op resolves.
 */
export function boundedRemoteEvent(
	event: unknown,
	maxBytes: number,
	resolveOutputId?: RemoteOutputIdResolver,
): Record<string, unknown> {
	const source = fields(event);
	const shell: Record<string, unknown> = {
		...identity(source, [
			"type",
			"messageId",
			"toolCallId",
			"toolName",
			"runId",
			"isError",
			"messageCount",
			"outputId",
			"isTerminal",
		]),
		truncated: true,
	};
	if (typeof source.toolCallId === "string") {
		const outputId = resolveOutputId?.(source.toolCallId);
		if (outputId !== undefined) shell.outputId = outputId;
	}
	const message = fields(source.message);
	if (typeof message.role === "string") shell.role = message.role;
	shell.preview = "";
	const budget = Math.max(0, maxBytes - byteLength(shell));
	const body =
		message.content ??
		source.text ??
		source.messages ??
		source.partialResult ??
		source.result ??
		source.update ??
		source.delta ??
		source;
	shell.preview = fitText(textOf(body), budget);
	return shell;
}

function offsetAfter(cursor: string, prefix: string): number | null {
	if (!cursor.startsWith(prefix) || cursor.length > prefix.length + 16) return null;
	const raw = cursor.slice(prefix.length);
	if (!/^(0|[1-9][0-9]*)$/.test(raw)) return null;
	const offset = Number(raw);
	return Number.isSafeInteger(offset) ? offset : null;
}

/**
 * A captured transcript view. The message array is read exactly once, at sync;
 * appends to the live transcript afterwards never shift or invalidate a page
 * already served from this view.
 */
export class RemoteHistoryView {
	readonly #messages: readonly unknown[];

	constructor(
		private readonly view: string,
		messages: readonly unknown[],
	) {
		this.#messages = [...messages];
	}

	/** Next page for `cursor` (absent = first page), or null when the cursor is not this view's. */
	page(
		cursor: string | undefined,
		maxBytes: number,
		resolveOutputId?: RemoteOutputIdResolver,
	): RemoteHistoryPage | null {
		const prefix = `${this.view}.history.`;
		const start = cursor === undefined ? 0 : offsetAfter(cursor, prefix);
		if (start === null || start > this.#messages.length) return null;
		let offset = start;
		const itemBudget = Math.max(256, maxBytes - WRAPPER_BYTES);
		const items: unknown[] = [];
		let bytes = WRAPPER_BYTES;
		while (offset < this.#messages.length && items.length < REMOTE_MAX_PAGE_ITEMS) {
			const message = this.#messages[offset];
			const fallback = `history.${offset}`;
			const item = boundedHistoryItem(message, itemBudget, id => resolveOutputId?.(id) ?? fallback);
			const size = byteLength(item) + 1;
			if (items.length > 0 && bytes + size > maxBytes) break;
			items.push(item);
			bytes += size;
			offset++;
		}
		return { items, cursor: offset < this.#messages.length ? `${prefix}${offset}` : null };
	}

	/** An older tool result is never fetched from live memory or the client. */
	output(outputId: string, cursor: string | undefined, maxBytes: number): RemoteOutputPage | null {
		const index = offsetAfter(outputId, "history.");
		if (index === null || index >= this.#messages.length) return null;
		const message = fields(this.#messages[index]);
		if (message.role !== "toolResult") return null;
		const text = textOf(message.content ?? "");
		const bytes = Buffer.byteLength(text);
		const truncated = bytes > REMOTE_MAX_OUTPUT_BYTES;
		const prefix = `${this.view}.${outputId}.`;
		const start = cursor === undefined ? 0 : offsetAfter(cursor, prefix);
		if (start === null || start > text.length || start > REMOTE_MAX_OUTPUT_BYTES) return null;
		const next = text.charCodeAt(start);
		if (next >= 0xdc00 && next <= 0xdfff) return null;
		// Page against JSON size as well as raw bytes. Only scan up to the retained
		// prefix on continuation rather than allocating a second oversized buffer.
		const consumed = truncated && start > 0 ? Buffer.byteLength(text.slice(0, start)) : 0;
		const available = truncated ? Math.max(0, REMOTE_MAX_OUTPUT_BYTES - consumed) : maxBytes;
		let cutoff = Math.min(start + maxBytes, text.length);
		const following = text.charCodeAt(cutoff);
		if (following >= 0xdc00 && following <= 0xdfff) cutoff--;
		const data = fitText(text.slice(start, cutoff), Math.min(maxBytes, available));
		const end = start + data.length;
		return {
			view: this.view,
			outputId,
			data,
			cursor:
				end > start &&
				end < text.length &&
				(!truncated || consumed + Buffer.byteLength(data) < REMOTE_MAX_OUTPUT_BYTES)
					? `${prefix}${end}`
					: null,
			truncated,
		};
	}
}
