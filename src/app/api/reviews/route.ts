import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import { execute, table } from "@/lib/databricks";
import { randomUUID } from "node:crypto";

export async function POST(request: Request) {
  const session = await getServerSession(authOptions);
  if (!session?.user?.id) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const codeEntryId =
    typeof body?.codeEntryId === "string" ? body.codeEntryId : "";
  const comments =
    typeof body?.comments === "string" ? body.comments.trim() : "";
  const summary = typeof body?.summary === "string" ? body.summary.trim() : "";
  const rating =
    typeof body?.rating === "number" ? Number(body.rating) : null;

  if (!codeEntryId || !comments) {
    return NextResponse.json(
      { error: "Code entry and comments are required." },
      { status: 400 },
    );
  }

  if (rating !== null && (Number.isNaN(rating) || rating < 1 || rating > 5)) {
    return NextResponse.json(
      { error: "Rating must be between 1 and 5." },
      { status: 400 },
    );
  }

  const entryRows = await execute<Record<string, unknown>>(
    `SELECT id FROM ${table("code_entries")} WHERE id = ? AND user_id = ? LIMIT 1`,
    [codeEntryId, session.user.id],
  );
  const entry = entryRows[0];

  if (!entry) {
    return NextResponse.json(
      { error: "Code entry not found." },
      { status: 404 },
    );
  }

  const id = randomUUID();
  await execute(
    `INSERT INTO ${table(
      "code_reviews",
    )} (id, code_entry_id, reviewer_id, rating, summary, comments, created_at)
     VALUES (?, ?, ?, ?, ?, ?, current_timestamp())`,
    [id, codeEntryId, session.user.id, rating, summary || null, comments],
  );

  return NextResponse.json({
    review: {
      id,
      codeEntryId,
      reviewerId: session.user.id,
      rating,
      summary: summary || null,
      comments,
      createdAt: new Date().toISOString(),
    },
  });
}
