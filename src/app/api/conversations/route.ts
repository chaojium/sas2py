import { NextResponse } from "next/server";
import { randomUUID } from "node:crypto";
import { discussConversion, type ConversationMessageInput } from "@/lib/codex";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";

export const runtime = "nodejs";
export const maxDuration = 300;

type ConversationRow = {
  id: string;
  code_entry_id: string;
  user_id: string;
  created_at: string;
};

type ConversationMessageRow = {
  id: string;
  conversation_id: string;
  role: string;
  content: string;
  created_at: string;
};

async function getConversationBundle(conversationId: string, userId: string) {
  const conversationRows = await execute<ConversationRow>(
    `SELECT id, code_entry_id, user_id, created_at
     FROM ${table("code_conversations")}
     WHERE id = ? AND user_id = ?
     LIMIT 1`,
    [conversationId, userId],
  );
  const conversation = conversationRows[0];
  if (!conversation) {
    return null;
  }

  const messageRows = await execute<ConversationMessageRow>(
    `SELECT id, conversation_id, role, content, created_at
     FROM ${table("code_conversation_messages")}
     WHERE conversation_id = ?
     ORDER BY created_at ASC`,
    [conversationId],
  );

  return {
    id: conversation.id,
    codeEntryId: conversation.code_entry_id,
    title: null,
    createdAt: conversation.created_at,
    updatedAt: null,
    messages: messageRows.map((message) => ({
      id: message.id,
      role: message.role,
      content: message.content,
      messageType: "chat",
      createdAt: message.created_at,
    })),
  };
}

export async function GET(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { searchParams } = new URL(request.url);
  const codeEntryId = searchParams.get("codeEntryId")?.trim() || "";
  if (!codeEntryId) {
    return NextResponse.json(
      { error: "codeEntryId is required." },
      { status: 400 },
    );
  }

  const conversationRows = await execute<ConversationRow>(
    `SELECT id, code_entry_id, user_id, created_at
     FROM ${table("code_conversations")}
     WHERE code_entry_id = ? AND user_id = ?
     ORDER BY created_at DESC`,
    [codeEntryId, user.appUserId],
  );

  const conversations = await Promise.all(
    conversationRows.map((row) => getConversationBundle(row.id, user.appUserId)),
  );

  return NextResponse.json({
    conversations: conversations.filter(Boolean),
  });
}

export async function POST(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const codeEntryId =
    typeof body?.codeEntryId === "string" ? body.codeEntryId.trim() : "";
  const message =
    typeof body?.message === "string" ? body.message.trim() : "";
  let conversationId =
    typeof body?.conversationId === "string" ? body.conversationId.trim() : "";

  if (!codeEntryId || !message) {
    return NextResponse.json(
      { error: "codeEntryId and message are required." },
      { status: 400 },
    );
  }

  const entryRows = await execute<Record<string, unknown>>(
    `SELECT id, user_id, language, sas_code, python_code, name, additional_guidance, reference_url
     FROM ${table("code_entries")}
     WHERE id = ? AND user_id = ?
     LIMIT 1`,
    [codeEntryId, user.appUserId],
  );
  const entry = entryRows[0];

  if (!entry) {
    return NextResponse.json(
      { error: "Code entry not found." },
      { status: 404 },
    );
  }

  if (!conversationId) {
    conversationId = randomUUID();
    await execute(
      `INSERT INTO ${table(
        "code_conversations",
      )} (id, code_entry_id, user_id, created_at)
       VALUES (?, ?, ?, current_timestamp())`,
      [conversationId, codeEntryId, user.appUserId],
    );
  }

  const userMessageId = randomUUID();
  await execute(
    `INSERT INTO ${table(
      "code_conversation_messages",
    )} (id, conversation_id, role, content, created_at)
     VALUES (?, ?, ?, ?, current_timestamp())`,
    [
      userMessageId,
      conversationId,
      "user",
      message,
    ],
  );

  const historyRows = await execute<ConversationMessageRow>(
    `SELECT id, conversation_id, role, content, created_at
     FROM ${table("code_conversation_messages")}
     WHERE conversation_id = ?
     ORDER BY created_at ASC`,
    [conversationId],
  );

  const assistantContent = await discussConversion({
    sasCode: String(entry.sas_code || ""),
    convertedCode: String(entry.python_code || ""),
    language: String(entry.language || "PYTHON") === "R" ? "R" : "PYTHON",
    additionalGuidance: String(entry.additional_guidance || ""),
    referenceUrl: String(entry.reference_url || ""),
    messages: historyRows.map(
      (row) =>
        ({
          role: row.role === "assistant" ? "assistant" : "user",
          content: row.content,
        }) satisfies ConversationMessageInput,
    ),
  });

  const assistantMessageId = randomUUID();
  await execute(
    `INSERT INTO ${table(
      "code_conversation_messages",
    )} (id, conversation_id, role, content, created_at)
     VALUES (?, ?, ?, ?, current_timestamp())`,
    [
      assistantMessageId,
      conversationId,
      "assistant",
      assistantContent,
    ],
  );

  const conversation = await getConversationBundle(conversationId, user.appUserId);
  return NextResponse.json({ conversation });
}
