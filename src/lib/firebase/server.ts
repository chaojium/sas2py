import { getServerSession } from "next-auth";
import { authOptions } from "@/lib/auth/options";

export type AuthUser = {
  uid: string;
  email: string;
  name: string | null;
  image: string | null;
  appUserId: string;
};

export async function getAuthUser(request: Request): Promise<AuthUser | null> {
  void request;
  const session = await getServerSession(authOptions);
  const sessionUser = session?.user;
  const uid = sessionUser?.id ?? "";
  const email = sessionUser?.email?.toLowerCase() ?? "";

  if (!uid || !email) {
    return null;
  }

  return {
    uid,
    email,
    name: sessionUser?.name ?? null,
    image: sessionUser?.image ?? null,
    appUserId: email,
  };
}
