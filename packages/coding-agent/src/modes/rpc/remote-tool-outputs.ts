import { randomUUID } from "node:crypto";
import {
	REMOTE_MAX_ID_LENGTH,
	REMOTE_MAX_OUTPUT_BYTES,
	REMOTE_MAX_OUTPUT_PAGE_BYTES,
	REMOTE_MAX_PAGE_ITEMS,
	type RemoteOutputPage,
} from "./remote-wire";

/**
 * Bounded Core-side record of tool output for the remote surface. Some output
 * exists only as live `tool_execution_*` events: with no peer attached the bytes
 * would vanish before anyone could page them. This class retains running
 * progress and final results under a byte aggregate and a finite finalized
 * count, serves them through opaque, UTF-8-safe pages, and logs nothing —
 * neither output bodies nor paths ever leave this module except via `page()`.
 */
export interface RemoteToolOutputsOptions {
	/** Aggregate bound over every retained output (default `REMOTE_MAX_OUTPUT_BYTES`). */
	maxOutputBytes?: number;
	/** Maximum UTF-8 bytes in one page (default `REMOTE_MAX_OUTPUT_PAGE_BYTES`). */
	maxPageBytes?: number;
	/** Finalized outputs retained before the oldest is dropped (default `REMOTE_MAX_PAGE_ITEMS`). */
	maxRetainedTools?: number;
}

/**
 * One capture of the retained tool state, handed to the snapshot builder. The
 * `view` id must travel in `RemoteSnapshot.view`; `page` answers only under
 * that exact id, so a client holding a superseded view is forced to re-sync.
 * Captured data is detached: later `observe` growth, eviction, or `clear` never
 * changes what an existing view serves.
 */
export interface RemoteToolOutputView {
	readonly view: string;
	/**
	 * Current and finalized tool references, in start order: the frame's
	 * wire-safe fields (`toolCallId`, `toolName`, `args`, `intent`, and
	 * `isError` when settled) plus `done` and the `outputId` to page. Output
	 * bytes stay out of the snapshot; the client pages them.
	 */
	readonly currentTools: readonly Record<string, unknown>[];
	/** One bounded page, or null when the view, output, or cursor is not this view's. */
	page(view: string, outputId: string, cursor?: string, pageBytes?: number): RemoteOutputPage | null;
}

interface TrackedOutput {
	readonly toolCallId: string;
	readonly toolName: string;
	args: unknown;
	intent?: string;
	isError?: boolean;
	/** Retained snapshot text (a truncated prefix once the byte bound bit). */
	text: string;
	bytes: Uint8Array;
	done: boolean;
	truncated: boolean;
}

interface CapturedOutput {
	readonly index: number;
	readonly bytes: Uint8Array;
	readonly truncated: boolean;
}

const EMPTY_BYTES = new Uint8Array(0);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/**
 * Accumulates `tool_execution_start` / `_update` / `_end` frames. Text content
 * on those frames is a growing full snapshot, so it is always replaced, never
 * appended; an update whose text is undefined (metadata-only) keeps the last
 * defined text instead of blanking progress. State is retained unconditionally:
 * a final result arriving while no peer is attached stays pageable.
 */
export class RemoteToolOutputs {
	readonly #maxOutputBytes: number;
	readonly #maxPageBytes: number;
	readonly #maxRetainedTools: number;
	/** Insertion order is start order; finalized entries stay in place until evicted. */
	readonly #tools = new Map<string, TrackedOutput>();
	#finalized = 0;
	#usedBytes = 0;

	constructor(options: RemoteToolOutputsOptions = {}) {
		this.#maxOutputBytes = positive(options.maxOutputBytes ?? REMOTE_MAX_OUTPUT_BYTES, "maxOutputBytes");
		this.#maxPageBytes = positive(options.maxPageBytes ?? REMOTE_MAX_OUTPUT_PAGE_BYTES, "maxPageBytes");
		this.#maxRetainedTools = positive(options.maxRetainedTools ?? REMOTE_MAX_PAGE_ITEMS, "maxRetainedTools");
	}

	observe(frame: Record<string, unknown>): void {
		const type = frame.type;
		if (type !== "tool_execution_start" && type !== "tool_execution_update" && type !== "tool_execution_end") return;
		const toolCallId = frame.toolCallId;
		const toolName = frame.toolName;
		// Malformed ids cannot key a retained output; a frame that the wire
		// validator itself would reject is dropped, not stored under a guess.
		if (
			typeof toolCallId !== "string" ||
			toolCallId.length === 0 ||
			toolCallId.length > REMOTE_MAX_ID_LENGTH ||
			typeof toolName !== "string" ||
			toolName.length === 0 ||
			toolName.length > REMOTE_MAX_ID_LENGTH
		)
			return;
		if (!this.#tools.has(toolCallId) && this.#tools.size >= REMOTE_MAX_PAGE_ITEMS) {
			const victim = this.#oldestFinalized();
			if (!victim) return;
			this.#drop(victim);
		}
		const entry = this.#track(toolCallId, toolName);
		if (type === "tool_execution_start" && Object.hasOwn(frame, "args") && frame.args !== entry.args) {
			const json = JSON.stringify(frame.args);
			entry.args = json && Buffer.byteLength(json) <= 1024 ? frame.args : undefined;
		}
		if (typeof frame.intent === "string") entry.intent = frame.intent.slice(0, 256);
		const text = extractText(type === "tool_execution_end" ? frame.result : frame.partialResult);
		if (text !== null) this.#setText(entry, text);
		if (type === "tool_execution_end" && !entry.done) {
			entry.done = true;
			if (typeof frame.isError === "boolean") entry.isError = frame.isError;
			this.#finalized += 1;
			this.#enforceRetainedCount();
		}
	}

	/** Drops everything; retained views keep serving their own captured bytes. */
	clear(): void {
		this.#tools.clear();
		this.#finalized = 0;
		this.#usedBytes = 0;
	}

	/**
	 * Detaches the current state. References, not copies: output text is
	 * replaced wholesale (never appended in place), so an already-captured
	 * bytes reference cannot change underneath the view.
	 */
	capture(): RemoteToolOutputView {
		const outputs = new Map<string, CapturedOutput>();
		const currentTools: Record<string, unknown>[] = [];
		for (const entry of this.#tools.values()) {
			outputs.set(entry.toolCallId, { index: outputs.size, bytes: entry.bytes, truncated: entry.truncated });
			const reference: Record<string, unknown> = {
				toolCallId: entry.toolCallId,
				toolName: entry.toolName,
				done: entry.done,
				outputId: entry.toolCallId,
			};
			if (entry.args !== undefined) reference.args = entry.args;
			if (entry.intent !== undefined) reference.intent = entry.intent;
			if (entry.done && entry.isError !== undefined) reference.isError = entry.isError;
			currentTools.push(Object.freeze(reference));
		}
		return new CapturedToolOutputView(randomUUID(), Object.freeze(currentTools), outputs, this.#maxPageBytes);
	}

	#track(toolCallId: string, toolName: string): TrackedOutput {
		let entry = this.#tools.get(toolCallId);
		if (entry === undefined) {
			entry = { toolCallId, toolName, args: undefined, text: "", bytes: EMPTY_BYTES, done: false, truncated: false };
			this.#tools.set(toolCallId, entry);
		}
		return entry;
	}

	#setText(entry: TrackedOutput, text: string): void {
		// Stable state must not churn: an identical (or replayed) snapshot keeps
		// the existing text and bytes references instead of copying output.
		if (text === entry.text) return;
		const owned = entry.bytes.byteLength;
		const textBytes = Buffer.byteLength(text);
		// Free whole finalized outputs (oldest first) before shrinking this one.
		while (this.#usedBytes - owned + textBytes > this.#maxOutputBytes) {
			const victim = this.#oldestFinalized(entry);
			if (victim === undefined) break;
			this.#drop(victim);
		}
		const available = this.#maxOutputBytes - (this.#usedBytes - owned);
		let bytes: Uint8Array;
		if (textBytes > available) {
			const buffer = new Uint8Array(available);
			const { written } = textEncoder.encodeInto(text, buffer);
			bytes = buffer.subarray(0, written);
			entry.truncated = true;
			entry.text = textDecoder.decode(bytes);
		} else {
			bytes = textEncoder.encode(text);
			entry.text = text;
		}
		entry.bytes = bytes;
		this.#usedBytes += bytes.byteLength - owned;
	}

	#oldestFinalized(except?: TrackedOutput): TrackedOutput | undefined {
		for (const entry of this.#tools.values()) {
			if (entry.done && entry !== except) return entry;
		}
		return undefined;
	}

	#drop(entry: TrackedOutput): void {
		this.#tools.delete(entry.toolCallId);
		this.#usedBytes -= entry.bytes.byteLength;
		if (entry.done) this.#finalized -= 1;
	}

	#enforceRetainedCount(): void {
		while (this.#finalized > this.#maxRetainedTools) {
			const victim = this.#oldestFinalized();
			if (victim === undefined) break;
			this.#drop(victim);
		}
	}
}

class CapturedToolOutputView implements RemoteToolOutputView {
	constructor(
		readonly view: string,
		readonly currentTools: readonly Record<string, unknown>[],
		private readonly _outputs: Map<string, CapturedOutput>,
		private readonly _maxPageBytes: number,
	) {}

	page(view: string, outputId: string, cursor?: string, pageBytes = this._maxPageBytes): RemoteOutputPage | null {
		// A stale or forged view id must not read this capture; the client has
		// to take a fresh snapshot instead of paging on faith.
		if (view !== this.view) return null;
		const output = this._outputs.get(outputId);
		if (output === undefined) return null;
		const bytes = output.bytes;
		const prefix = `${this.view}.${output.index}.`;
		const start = cursor === undefined ? 0 : decodeCursor(cursor, prefix);
		if (start === null || start > bytes.byteLength) return null;
		// A cursor into the middle of a character is not a position we issued.
		if (start > 0 && start < bytes.byteLength && (bytes[start] & 0xc0) === 0x80) return null;
		const end = pageEnd(bytes, start, Math.min(pageBytes, this._maxPageBytes));
		const exhausted = end >= bytes.byteLength;
		return {
			view: this.view,
			outputId,
			data: textDecoder.decode(bytes.subarray(start, end)),
			cursor: exhausted ? null : `${prefix}${end}`,
			// Retention truncation only — more pages are carried by the cursor.
			truncated: output.truncated,
		};
	}
}

/**
 * Page end offset from `start`: at most `pageBytes`, never splitting a UTF-8
 * sequence. A character larger than the page is taken whole so the cursor
 * always advances and paging cannot stall.
 */
function pageEnd(bytes: Uint8Array, start: number, pageBytes: number): number {
	let end = Math.min(bytes.byteLength, start + pageBytes);
	if (end >= bytes.byteLength) return bytes.byteLength;
	// Back off continuation bytes (10xxxxxx): a character straddling the limit
	// moves whole to the next page.
	while (end > start && (bytes[end] & 0xc0) === 0x80) end -= 1;
	if (end === start) {
		do end += 1;
		while (end < bytes.byteLength && (bytes[end] & 0xc0) === 0x80);
	}
	return end;
}

function decodeCursor(cursor: string, prefix: string): number | null {
	if (!cursor.startsWith(prefix) || cursor.length > prefix.length + 16) return null;
	const offset = cursor.slice(prefix.length);
	if (!/^[1-9][0-9]*$/.test(offset)) return null;
	const value = Number(offset);
	return Number.isSafeInteger(value) ? value : null;
}

/**
 * Joined text parts of a result-shaped frame value. Null means the frame
 * carried no defined text (absent or non-text-only payload) and the retained
 * output must survive; "" is a defined empty snapshot and replaces.
 */
function extractText(value: unknown): string | null {
	if (typeof value === "string") return value;
	if (typeof value !== "object" || value === null || !("content" in value)) return null;
	const content = value.content;
	if (!Array.isArray(content)) return null;
	let text = "";
	let sawText = false;
	for (const part of content) {
		if (
			typeof part === "object" &&
			part !== null &&
			"type" in part &&
			part.type === "text" &&
			"text" in part &&
			typeof part.text === "string"
		) {
			text += part.text;
			sawText = true;
		}
	}
	return sawText ? text : null;
}

function positive(value: number, name: string): number {
	if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`Invalid remote tool output ${name}`);
	return value;
}
