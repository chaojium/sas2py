import "server-only";
import OpenAI from "openai";
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

function getApiConfig() {
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) {
    throw new Error("Missing OPENAI_API_KEY.");
  }
  const model = process.env.OPENAI_MODEL?.trim() || DEFAULT_OPENAI_MODEL;
  const timeoutMs = Number(process.env.OPENAI_TIMEOUT_MS || DEFAULT_TIMEOUT_MS);
  return { apiKey, model, timeoutMs };
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

async function generateWithOpenAI(prompt: string) {
  const { apiKey, model, timeoutMs } = getApiConfig();
  const client = new OpenAI({
    apiKey,
    timeout: timeoutMs,
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
    if (
      error instanceof Error &&
      (error.name === "AbortError" || error.message.toLowerCase().includes("timeout"))
    ) {
      throw new Error("OpenAI request timed out.");
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
