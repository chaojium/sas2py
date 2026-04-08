import { NextResponse } from "next/server";
import { analyzeSasCode } from "@/lib/codex";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";

export const runtime = "nodejs";

export async function POST(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const body = await request.json();
  const entryId = typeof body?.entryId === "string" ? body.entryId.trim() : "";
  let sasCode = typeof body?.sasCode === "string" ? body.sasCode.trim() : "";

  if (!sasCode && entryId) {
    const entryRows = await execute<Record<string, unknown>>(
      `SELECT sas_code FROM ${table("code_entries")} WHERE id = ? AND user_id = ? LIMIT 1`,
      [entryId, user.appUserId],
    );
    const entry = entryRows[0];
    sasCode = typeof entry?.sas_code === "string" ? entry.sas_code.trim() : "";
  }

  if (!sasCode) {
    return NextResponse.json(
      { error: "SAS code or entryId is required." },
      { status: 400 },
    );
  }

  try {
    const analysis = await analyzeSasCode(sasCode);
    return NextResponse.json({ analysis });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "SAS analysis failed.",
      },
      { status: 500 },
    );
  }
}
