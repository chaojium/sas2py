import { NextResponse } from "next/server";
import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth";
import { execute, table } from "@/lib/databricks";

export async function GET() {
  const session = await getServerSession(authOptions);
  if (!session?.user?.id) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const conversionRows = await execute<Record<string, unknown>>(
    `SELECT COUNT(*) AS count FROM ${table("code_entries")} WHERE user_id = ?`,
    [session.user.id],
  );
  const reviewRows = await execute<Record<string, unknown>>(
    `SELECT COUNT(*) AS count FROM ${table("code_reviews")} WHERE reviewer_id = ?`,
    [session.user.id],
  );

  const conversions = Number(conversionRows[0]?.count || 0);
  const reviews = Number(reviewRows[0]?.count || 0);

  return NextResponse.json({ conversions, reviews });
}
