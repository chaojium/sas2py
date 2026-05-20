import { NextResponse } from "next/server";
import { execute } from "@/lib/databricks";

export const runtime = "nodejs";
export const maxDuration = 60;

function isDebugEnabled() {
  return process.env.DEBUG_ROUTES_ENABLED?.trim() === "true";
}

function serializeError(error: unknown) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack:
        process.env.NODE_ENV === "development" ? error.stack ?? null : null,
      cause:
        error.cause instanceof Error
          ? {
              name: error.cause.name,
              message: error.cause.message,
            }
          : error.cause ?? null,
    };
  }

  return {
    name: "UnknownError",
    message: String(error),
    stack: null,
    cause: null,
  };
}

export async function GET() {
  if (!isDebugEnabled()) {
    return NextResponse.json({ error: "Not found." }, { status: 404 });
  }

  try {
    const startedAt = Date.now();
    const rows = await execute<{ ok: number }>("SELECT 1 AS ok");
    return NextResponse.json({
      ok: true,
      durationMs: Date.now() - startedAt,
      rows,
    });
  } catch (error) {
    return NextResponse.json(
      {
        ok: false,
        error: serializeError(error),
      },
      { status: 500 },
    );
  }
}
