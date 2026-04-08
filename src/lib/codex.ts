import "server-only";
import { readFileSync } from "node:fs";
import { request as httpsRequest } from "node:https";
import OpenAI from "openai";
import type { ClientOptions } from "openai";

const DEFAULT_TIMEOUT_MS = 90_000;
const DEFAULT_OPENAI_MODEL = "gpt-5.2";

type OpenAIResponse = {
  output_text?: string;
  output?: Array<{
    content?: Array<{
      type?: string;
      text?: string;
    }>;
  }>;
  error?: {
    message?: string;
  };
};

export type SasAnalysis = {
  interpretation: string;
  expectedOutput: string;
  validationChecks: string[];
};

function createCustomOpenAIFetch(
  ca: string,
): NonNullable<ClientOptions["fetch"]> {
  return async (input, init) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const url = new URL(request.url);
    const bodyBuffer = Buffer.from(await request.arrayBuffer());

    return new Promise<Response>((resolve, reject) => {
      const req = httpsRequest(
        {
          protocol: url.protocol,
          hostname: url.hostname,
          port: url.port || undefined,
          path: `${url.pathname}${url.search}`,
          method: request.method,
          headers: Object.fromEntries(request.headers.entries()),
          ca,
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk) =>
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)),
          );
          res.on("end", () => {
            const responseHeaders = new Headers();
            for (const [key, value] of Object.entries(res.headers)) {
              if (Array.isArray(value)) {
                for (const item of value) {
                  responseHeaders.append(key, item);
                }
              } else if (value !== undefined) {
                responseHeaders.set(key, value);
              }
            }
            resolve(
              new Response(Buffer.concat(chunks), {
                status: res.statusCode || 500,
                statusText: res.statusMessage || "",
                headers: responseHeaders,
              }),
            );
          });
        },
      );

      req.on("error", reject);

      if (bodyBuffer && bodyBuffer.length > 0) {
        req.write(bodyBuffer);
      }

      req.end();
    });
  };
}

function getApiConfig() {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    throw new Error("Missing OPENAI_API_KEY.");
  }
  const model = process.env.OPENAI_MODEL?.trim() || DEFAULT_OPENAI_MODEL;
  const timeoutMs = Number(process.env.OPENAI_TIMEOUT_MS || DEFAULT_TIMEOUT_MS);
  const caCertPath = process.env.OPENAI_CA_CERT_PATH?.trim();
  const caCert = caCertPath ? readFileSync(caCertPath, "utf8") : null;
  return { apiKey, model, timeoutMs, caCert, caCertPath };
}

function extractOutputText(payload: OpenAIResponse) {
  if (payload.output_text?.trim()) {
    return payload.output_text.trim();
  }

  const chunks: string[] = [];
  for (const item of payload.output || []) {
    for (const content of item.content || []) {
      if (content.type === "output_text" || content.type === "text") {
        if (content.text?.trim()) {
          chunks.push(content.text.trim());
        }
      }
    }
  }
  return chunks.join("\n").trim();
}

function extractJsonObject(text: string) {
  const trimmed = text.trim();
  const fencedMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  const candidate = fencedMatch ? fencedMatch[1].trim() : trimmed;
  const start = candidate.indexOf("{");
  const end = candidate.lastIndexOf("}");
  if (start < 0 || end < start) {
    throw new Error("OpenAI API returned non-JSON analysis output.");
  }
  return candidate.slice(start, end + 1);
}

async function generateWithOpenAI(prompt: string) {
  const { apiKey, model, timeoutMs, caCert, caCertPath } = getApiConfig();
  const client = new OpenAI({
    apiKey,
    timeout: timeoutMs,
    ...(caCert ? { fetch: createCustomOpenAIFetch(caCert) } : {}),
  });

  try {
    const response = await client.responses.create({
      model,
      input: [
        {
          role: "user",
          content: [{ type: "input_text", text: prompt }],
        },
      ],
    });
    const payload = response as OpenAIResponse;
    const output = extractOutputText(payload);
    if (!output) {
      throw new Error("OpenAI API returned empty output.");
    }
    return output;
  } catch (error) {
    const code =
      error &&
      typeof error === "object" &&
      "cause" in error &&
      error.cause &&
      typeof error.cause === "object" &&
      "code" in error.cause
        ? String(error.cause.code)
        : "";

    if (
      error instanceof Error &&
      (error.name === "AbortError" || error.message.toLowerCase().includes("timeout"))
    ) {
      throw new Error("OpenAI request timed out.");
    }

    if (code === "SELF_SIGNED_CERT_IN_CHAIN") {
      throw new Error(
        caCertPath
          ? `OpenAI TLS validation failed even with OPENAI_CA_CERT_PATH=${caCertPath}. Confirm that the file contains the correct corporate root/intermediate CA.`
          : "OpenAI TLS validation failed because a self-signed certificate was presented in the network chain. Set OPENAI_CA_CERT_PATH to your corporate CA PEM file, or configure NODE_EXTRA_CA_CERTS for the Node process.",
      );
    }

    throw error;
  }
}

export async function convertSasToPython(sasCode: string) {
  const prompt = [
    "Convert the following SAS code to idiomatic, production-ready Python.",
    "Use pandas where needed and preserve logic and comments.",
    "Return only Python code, no markdown or commentary.",
    "",
    sasCode,
  ].join("\n");
  return generateWithOpenAI(prompt);
}

export async function convertSasToR(sasCode: string) {
  const prompt = [
    "Convert the following SAS code to idiomatic, production-ready R.",
    "Use tidyverse or data.table where appropriate and preserve logic and comments.",
    "Return only R code, no markdown or commentary.",
    "",
    sasCode,
  ].join("\n");
  return generateWithOpenAI(prompt);
}

export async function refineConversion(
  sasCode: string,
  convertedCode: string,
  instruction: string,
  language: "PYTHON" | "R",
) {
  const target = language === "R" ? "R" : "Python";
  const prompt = [
    `You are improving an existing SAS to ${target} conversion.`,
    "Apply the user's instruction while preserving the SAS logic.",
    `Return only the updated ${target} code, no markdown or commentary.`,
    "",
    "User instruction:",
    instruction,
    "",
    "SAS source:",
    sasCode,
    "",
    `Current ${target} conversion:`,
    convertedCode,
  ].join("\n");
  return generateWithOpenAI(prompt);
}

export async function analyzeSasCode(sasCode: string) {
  const prompt = [
    "You are analyzing SAS code for validation planning.",
    "Explain what the SAS program is intended to do and what output a reviewer should expect.",
    "Return valid JSON only with this exact shape:",
    '{"interpretation":"string","expectedOutput":"string","validationChecks":["string"]}',
    "Keep the interpretation concise but specific.",
    "Describe expected output in business/data terms, not code terms.",
    "List 3 to 6 concrete validation checks.",
    "",
    sasCode,
  ].join("\n");

  const raw = await generateWithOpenAI(prompt);
  const parsed = JSON.parse(extractJsonObject(raw)) as Partial<SasAnalysis>;
  return {
    interpretation: String(parsed.interpretation || "").trim(),
    expectedOutput: String(parsed.expectedOutput || "").trim(),
    validationChecks: Array.isArray(parsed.validationChecks)
      ? parsed.validationChecks
          .map((value) => String(value || "").trim())
          .filter(Boolean)
      : [],
  } satisfies SasAnalysis;
}
