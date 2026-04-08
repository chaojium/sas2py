import { NextResponse } from "next/server";
import { execute, table } from "@/lib/databricks";
import { getAuthUser } from "@/lib/firebase/server";

export const runtime = "nodejs";

export async function GET(request: Request) {
  const user = await getAuthUser(request);
  if (!user?.appUserId) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const conversionRows = await execute<Record<string, unknown>>(
    `SELECT COUNT(*) AS count FROM ${table("code_entries")} WHERE user_id = ?`,
    [user.appUserId],
  );
  const reviewRows = await execute<Record<string, unknown>>(
    `SELECT COUNT(*) AS count FROM ${table("code_reviews")} WHERE reviewer_id = ?`,
    [user.appUserId],
  );

  const conversions = Number(conversionRows[0]?.count || 0);
  const reviews = Number(reviewRows[0]?.count || 0);

  return NextResponse.json({ conversions, reviews });
}
