import { createRemoteJWKSet, jwtVerify } from "jose";

const projectId = process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID?.trim();
const allowDevAuthBypass =
  process.env.NODE_ENV !== "production" &&
  process.env.DEV_AUTH_BYPASS?.trim() === "true";

if (!projectId) {
  throw new Error("Missing NEXT_PUBLIC_FIREBASE_PROJECT_ID.");
}

const issuer = `https://securetoken.google.com/${projectId}`;
const jwks = createRemoteJWKSet(
  new URL(
    "https://www.googleapis.com/service_accounts/v1/jwk/securetoken@system.gserviceaccount.com",
  ),
);

export type AuthUser = {
  uid: string;
  email: string;
  name: string | null;
  image: string | null;
  appUserId: string;
};

function getAuthUserFromPayload(payload: Record<string, unknown>): AuthUser | null {
  const uid =
    typeof payload.user_id === "string"
      ? payload.user_id
      : typeof payload.sub === "string"
        ? payload.sub
        : "";
  const email =
    typeof payload.email === "string" ? payload.email.toLowerCase() : "";

  if (!uid || !email) {
    return null;
  }

  return {
    uid,
    email,
    name: typeof payload.name === "string" ? payload.name : null,
    image: typeof payload.picture === "string" ? payload.picture : null,
    appUserId: email,
  };
}

function decodeJwtPayload(token: string): Record<string, unknown> | null {
  const segments = token.split(".");
  if (segments.length < 2) {
    return null;
  }

  try {
    const json = Buffer.from(segments[1], "base64url").toString("utf8");
    const payload = JSON.parse(json);
    return payload && typeof payload === "object"
      ? (payload as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

export async function getAuthUser(request: Request): Promise<AuthUser | null> {
  const authorization = request.headers.get("authorization")?.trim();
  if (!authorization?.startsWith("Bearer ")) {
    if (process.env.NODE_ENV === "development") {
      console.warn("Firebase auth missing bearer token.");
    }
    return null;
  }

  const token = authorization.slice("Bearer ".length).trim();
  if (!token) {
    return null;
  }

  if (allowDevAuthBypass) {
    const payload = decodeJwtPayload(token);
    const user = payload ? getAuthUserFromPayload(payload) : null;

    if (process.env.NODE_ENV === "development") {
      console.warn("DEV_AUTH_BYPASS is enabled. Firebase tokens are not verified.");
    }

    return user;
  }

  try {
    const { payload } = await jwtVerify(token, jwks, {
      issuer,
      audience: projectId,
    });
    return getAuthUserFromPayload(payload as Record<string, unknown>);
  } catch (error) {
    if (process.env.NODE_ENV === "development") {
      console.error("Firebase token verification failed:", error);
    }
    return null;
  }
}
