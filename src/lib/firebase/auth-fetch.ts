"use client";

import { firebaseAuth } from "@/lib/firebase/client";

export async function authFetch(
  input: RequestInfo | URL,
  init: RequestInit = {},
) {
  await firebaseAuth.authStateReady();

  const user = firebaseAuth.currentUser;
  if (!user) {
    throw new Error("Unauthorized");
  }

  const token = await user.getIdToken();
  const headers = new Headers(init.headers);
  headers.set("Authorization", `Bearer ${token}`);

  if (init.body && !(init.body instanceof FormData) && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  return fetch(input, {
    ...init,
    headers,
  });
}
