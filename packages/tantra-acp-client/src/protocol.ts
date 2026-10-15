export const PROTOCOL_VERSION = 2;
export const PROFILE_VERSION = 1;
export const MAX_FRAME = 1024 * 1024;

export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };
export type Meta = JsonObject;
export type ContentBlock = { type: string } & JsonObject;
export type Mode = "read" | "write";

interface Extensible {
  _meta?: Meta;
}

export interface StateBody {
  state: "running" | "idle" | "requires_action";
  stopReason?: string;
  error?: { code: number; message: string };
}

export interface MessageUpdate extends Extensible {
  sessionUpdate: "user_message" | "agent_message" | "agent_thought";
  messageId: string;
  content: ContentBlock[];
}

export interface MessageChunk extends Extensible {
  sessionUpdate: "agent_message_chunk" | "agent_thought_chunk";
  messageId: string;
  content: ContentBlock;
}

export interface SessionMessage extends Extensible {
  sessionUpdate: "session_message";
  messageId: string;
  senderSessionId?: string;
  recipientSessionId?: string;
  content: ContentBlock[];
}

export interface ToolCallUpdate extends Extensible {
  sessionUpdate: "tool_call_update";
  toolCallId: string;
  name?: string;
  title?: string;
  kind?: string;
  status?: string;
  content?: JsonObject[];
  rawInput?: JsonValue;
  rawOutput?: JsonValue;
}

export interface ToolCallContentChunk extends Extensible {
  sessionUpdate: "tool_call_content_chunk";
  toolCallId: string;
  content: { type: "content"; content: ContentBlock };
}

export interface SubagentUpdate extends Extensible {
  sessionUpdate: "subagent_update";
  sessionId: string;
  title?: string;
  state?: StateBody;
}

export interface UsageUpdate extends Extensible {
  sessionUpdate: "usage_update";
  used: number;
  size: number;
}

export interface CompactionUpdate extends Extensible {
  sessionUpdate: "compaction_update";
  compactionId: string;
  status: string;
  summary?: ContentBlock[];
}

export interface CompactionSummaryChunk extends Extensible {
  sessionUpdate: "compaction_summary_chunk";
  compactionId: string;
  content: ContentBlock;
}

export interface SessionInfoUpdate extends Extensible {
  sessionUpdate: "session_info_update";
  title?: string;
  updatedAt?: string;
}

export interface ConfigValue {
  value: string;
  name: string;
}

export interface ConfigOption {
  configId: string;
  name: string;
  category?: string;
  type: "select";
  currentValue: string;
  options: ConfigValue[];
}

export interface ConfigOptionUpdate extends Extensible {
  sessionUpdate: "config_option_update";
  configOptions: ConfigOption[];
}

export interface StateUpdate extends Extensible, StateBody {
  sessionUpdate: "state_update";
}

export interface WriterUpdate extends Extensible {
  sessionUpdate: "_tantra_writer";
  writer: boolean;
  reason: "claimed" | "replaced" | "lost";
}

export interface QueueEntry {
  commandId: string;
  content: ContentBlock[];
  submittedBy?: string;
}

export interface QueueUpdate extends Extensible {
  sessionUpdate: "_tantra_queue_update";
  entries: QueueEntry[];
}

export interface NoticeUpdate extends Extensible {
  sessionUpdate: "_tantra_notice";
  level: "warning";
  code: string;
  message: string;
}

export type SessionUpdate =
  | MessageUpdate
  | MessageChunk
  | SessionMessage
  | ToolCallUpdate
  | ToolCallContentChunk
  | SubagentUpdate
  | UsageUpdate
  | CompactionUpdate
  | CompactionSummaryChunk
  | SessionInfoUpdate
  | ConfigOptionUpdate
  | StateUpdate
  | WriterUpdate
  | QueueUpdate
  | NoticeUpdate;

export interface SessionNotification extends Extensible {
  sessionId: string;
  update: SessionUpdate;
}

export interface PermissionOption {
  optionId: string;
  name: string;
  kind: string;
}

export interface RequestPermissionParams extends Extensible {
  sessionId: string;
  title: string;
  description?: string;
  subject?: JsonObject;
  options: PermissionOption[];
}

export interface ElicitationParams extends Extensible {
  sessionId: string;
  toolCallId?: string;
  mode: "form";
  message: string;
  requestedSchema: JsonObject;
}

export type PermissionResult = Extensible & {
  outcome: { outcome: "selected"; optionId: string } | { outcome: "cancelled" };
};

export type ElicitationResult = Extensible &
  (
    | { action: "accept"; content: JsonObject }
    | { action: "decline" }
    | { action: "cancel" }
  );

export interface InitializeResult extends Extensible {
  protocolVersion: number;
  capabilities?: { _meta?: { tantra?: { version?: number } } };
  info?: { name: string; version: string };
}

export interface NewSessionResult extends Extensible {
  sessionId: string;
  configOptions?: ConfigOption[];
}

export interface ResumeSessionResult extends Extensible {
  configOptions?: ConfigOption[];
}

export interface SessionInfo extends Extensible {
  sessionId: string;
  cwd: string;
  title?: string;
  updatedAt?: string;
}

export interface ListSessionsResult extends Extensible {
  sessions: SessionInfo[];
  nextCursor?: string;
}

export interface PromptResult extends Extensible {
  messageId: string;
}

export type EmptyResult = Extensible;

export type ServerErrorCode =
  | "writer_required"
  | "command_conflict"
  | "unavailable"
  | "model_busy"
  | "not_found"
  | "invalid_params"
  | "internal";

const PROFILE_CODES: ReadonlySet<string> = new Set([
  "writer_required",
  "command_conflict",
  "unavailable",
  "model_busy",
]);

export function errorCode(code: number, data: unknown): ServerErrorCode {
  const tantra = (data as { tantra?: { code?: unknown } } | null | undefined)
    ?.tantra?.code;
  if (typeof tantra === "string" && PROFILE_CODES.has(tantra))
    return tantra as ServerErrorCode;
  if (code === -32002) return "not_found";
  if (code === -32602) return "invalid_params";
  return "internal";
}

export function cursorOf(update: SessionUpdate): string | undefined {
  const tantra = update._meta?.["tantra"];
  if (typeof tantra !== "object" || tantra === null || Array.isArray(tantra))
    return undefined;
  const cursor = tantra["cursor"];
  return typeof cursor === "string" ? cursor : undefined;
}

export function isAfter(cursor: string, than: string): boolean {
  const [seq = 0, index = 0] = cursor.split(".").map(Number);
  const [otherSeq = 0, otherIndex = 0] = than.split(".").map(Number);
  return seq > otherSeq || (seq === otherSeq && index > otherIndex);
}
