import "server-only";
import { readFileSync } from "node:fs";
import { request as httpsRequest } from "node:https";
import { ClientSecretCredential, getBearerTokenProvider } from "@azure/identity";
import { AzureOpenAI } from "openai";
import type { ClientOptions } from "openai";

const DEFAULT_TIMEOUT_MS = 300_000;
const DEFAULT_OPENAI_MODEL = "gpt5.5-dgw-default";
const DEFAULT_AZURE_OPENAI_API_VERSION = "2025-03-01-preview";
type OpenAITask = "conversion" | "analysis" | "conversation";

type ApiConfig = ReturnType<typeof getApiConfig>;

const PERCENTILE_EXAMPLE_SAS_PATH =
  "C:\\Users\\ugc8\\Documents\\apps\\sas2py\\examples\\Percentile calculation_NER website_20260113.sas";
const PERCENTILE_EXAMPLE_PYTHON_PATH =
  "C:\\Users\\ugc8\\Documents\\apps\\sas2py\\examples\\Percentile_calculation_NER_website_20260113_sudaan_like.py";
const PERCENTILE_EXAMPLE_R_PATH =
  "C:\\Users\\ugc8\\Documents\\apps\\sas2py\\examples\\Percentile_calculation_NER_website_20260113.R";

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

export type ConversationMessageInput = {
  role: "user" | "assistant";
  content: string;
};

type ConversionContext = {
  additionalGuidance?: string;
  referenceUrl?: string;
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

function resolveModelForTask(task: OpenAITask) {
  if (task === "conversion") {
    return (
      process.env.AZURE_OPENAI_MODEL_CONVERSION?.trim() ||
      process.env.AZURE_OPENAI_MODEL?.trim() ||
      DEFAULT_OPENAI_MODEL
    );
  }

  if (task === "analysis") {
    return (
      process.env.AZURE_OPENAI_MODEL_ANALYSIS?.trim() ||
      process.env.AZURE_OPENAI_MODEL?.trim() ||
      DEFAULT_OPENAI_MODEL
    );
  }

  return (
    process.env.AZURE_OPENAI_MODEL_CONVERSATION?.trim() ||
    process.env.AZURE_OPENAI_MODEL?.trim() ||
    DEFAULT_OPENAI_MODEL
  );
}

function resolveFallbackModelForTask(task: OpenAITask) {
  if (task !== "conversion") {
    return null;
  }

  return process.env.AZURE_OPENAI_MODEL_CONVERSION_FALLBACK?.trim() || null;
}

function getApiConfig(task: OpenAITask) {
  const tenantId = process.env.AZURE_TENANT_ID?.trim();
  const clientId = process.env.AZURE_CLIENT_ID?.trim();
  const clientSecret = process.env.AZURE_CLIENT_SECRET?.trim();
  const endpoint = process.env.AZURE_OPENAI_ENDPOINT?.trim().replace(/\/+$/, "");
  const scope = process.env.AZURE_OPENAI_SCOPE?.trim();
  const subscriptionKey = process.env.APIM_SUBSCRIPTION_KEY?.trim();
  const model = resolveModelForTask(task);
  const apiVersion =
    process.env.AZURE_OPENAI_API_VERSION?.trim() ||
    DEFAULT_AZURE_OPENAI_API_VERSION;
  const timeoutMs = Number(process.env.OPENAI_TIMEOUT_MS || DEFAULT_TIMEOUT_MS);
  const caCertPath = process.env.OPENAI_CA_CERT_PATH?.trim();
  const caCert = caCertPath ? readFileSync(caCertPath, "utf8") : null;

  if (!tenantId) {
    throw new Error("Missing AZURE_TENANT_ID.");
  }
  if (!clientId) {
    throw new Error("Missing AZURE_CLIENT_ID.");
  }
  if (!clientSecret) {
    throw new Error("Missing AZURE_CLIENT_SECRET.");
  }
  if (!endpoint) {
    throw new Error("Missing AZURE_OPENAI_ENDPOINT.");
  }
  if (!scope) {
    throw new Error("Missing AZURE_OPENAI_SCOPE.");
  }
  if (!subscriptionKey) {
    throw new Error("Missing APIM_SUBSCRIPTION_KEY.");
  }

  return {
    tenantId,
    clientId,
    clientSecret,
    endpoint,
    scope,
    subscriptionKey,
    model,
    apiVersion,
    timeoutMs,
    caCert,
    caCertPath,
  };
}

function shouldUsePercentileExample(sasCode: string) {
  const normalized = sasCode.toLowerCase();
  return [
    "percentile",
    "pctl",
    "proc univariate",
    "proc surveymeans",
    "confidence interval",
    "conf_int",
    "sudaan",
  ].some((pattern) => normalized.includes(pattern));
}

function getPercentileExamplePrompt(language: "PYTHON" | "R") {
  try {
    const exampleSas = readFileSync(PERCENTILE_EXAMPLE_SAS_PATH, "utf8").trim();
    const exampleTarget = readFileSync(
      language === "R" ? PERCENTILE_EXAMPLE_R_PATH : PERCENTILE_EXAMPLE_PYTHON_PATH,
      "utf8",
    ).trim();
    return [
      "",
      "Use the following known-good example as a reference for translation style, statistical approach, deterministic implementation, lowercase normalization, and confidence-interval handling.",
      "Do not copy names or hardcode unrelated details from the example into the new translation, but follow the same quality bar and implementation patterns when the new SAS code has similar percentile or CI logic.",
      "",
      "Reference SAS example:",
      exampleSas,
      "",
      `Reference ${language === "R" ? "R" : "Python"} translation example:`,
      exampleTarget,
    ].join("\n");
  } catch {
    return "";
  }
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

async function generateWithOpenAI(prompt: string, task: OpenAITask) {
  const {
    tenantId,
    clientId,
    clientSecret,
    endpoint,
    scope,
    subscriptionKey,
    model,
    apiVersion,
    timeoutMs,
    caCert,
    caCertPath,
  } = getApiConfig(task);
  const credential = new ClientSecretCredential(
    tenantId,
    clientId,
    clientSecret,
  );
  const azureADTokenProvider = getBearerTokenProvider(credential, scope);
  const client = new AzureOpenAI({
    endpoint,
    apiVersion,
    deployment: model,
    azureADTokenProvider,
    timeout: timeoutMs,
    defaultHeaders: { "Ocp-Apim-Subscription-Key": subscriptionKey },
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
          : "Azure OpenAI TLS validation failed because a self-signed certificate was presented in the network chain. Set OPENAI_CA_CERT_PATH to your corporate CA PEM file, or configure NODE_EXTRA_CA_CERTS for the Node process.",
      );
    }

    throw error;
  }
}

async function generateWithConfig(prompt: string, config: ApiConfig) {
  const {
    tenantId,
    clientId,
    clientSecret,
    endpoint,
    scope,
    subscriptionKey,
    model,
    apiVersion,
    timeoutMs,
    caCert,
    caCertPath,
  } = config;
  const credential = new ClientSecretCredential(
    tenantId,
    clientId,
    clientSecret,
  );
  const azureADTokenProvider = getBearerTokenProvider(credential, scope);
  const client = new AzureOpenAI({
    endpoint,
    apiVersion,
    deployment: model,
    azureADTokenProvider,
    timeout: timeoutMs,
    defaultHeaders: { "Ocp-Apim-Subscription-Key": subscriptionKey },
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
          : "Azure OpenAI TLS validation failed because a self-signed certificate was presented in the network chain. Set OPENAI_CA_CERT_PATH to your corporate CA PEM file, or configure NODE_EXTRA_CA_CERTS for the Node process.",
      );
    }

    throw error;
  }
}

async function generateWithOpenAIFallback(prompt: string, task: OpenAITask) {
  const primaryConfig = getApiConfig(task);
  const fallbackModel = resolveFallbackModelForTask(task);

  try {
    return await generateWithConfig(prompt, primaryConfig);
  } catch (error) {
    if (
      fallbackModel &&
      error instanceof Error &&
      error.message === "OpenAI request timed out." &&
      fallbackModel !== primaryConfig.model
    ) {
      return generateWithConfig(prompt, {
        ...primaryConfig,
        model: fallbackModel,
      });
    }

    throw error;
  }
}

export async function convertSasToPython(sasCode: string) {
  return convertSasToPythonWithContext(sasCode, {});
}

export async function convertSasToPythonWithContext(
  sasCode: string,
  context: ConversionContext,
) {
  const prompt = [
    "Convert the following SAS code to idiomatic, production-ready Python.",
    "Use pandas where needed and preserve logic and comments.",
    "Preserve SAS documentation headers and block comments, including banner-style comment sections such as /* ***** ... */.",
    "Rewrite every SAS comment as an equivalent Python comment instead of dropping or summarizing it.",
    "If the SAS file begins with a top-of-file documentation banner or header block, reproduce that header at the top of the Python file as Python comments.",
    "Do not omit file metadata sections such as File, Purpose, Date, Date Revised, Note, Input Datasets, or Programmer when they appear in the SAS header.",
    "Treat the opening SAS documentation block as required output, not optional context.",
    "Ensure the numerical results, especially confidence intervals such as 95% CI, match the SAS output as closely as possible.",
    "Do not simplify or hard-code intermediate values or estimated quantities.",
    "Generate deterministic code so repeated runs on the same input data produce the same results.",
    "Do not introduce randomness, sampling, bootstrapping, or randomized approximations unless the SAS source explicitly uses them.",
    "If the SAS source explicitly requires randomness, set fixed seeds and document them in code comments.",
    "Preserve the same statistical logic used in SAS, including weighting, subpopulation or domain analysis, variance estimation method, degrees of freedom, and distribution assumptions such as t versus normal.",
    "Match SAS procedures as closely as possible, including PROC SURVEYMEANS, PROC DESCRIPT, and PROC UNIVARIATE behavior when applicable.",
    "If exact Python equivalents do not exist, document any approximation clearly in code comments near the relevant step.",
    "Use appropriate Python packages such as pandas, numpy, scipy, and statsmodels when the SAS code uses statistical procedures, and use suitable survey-analysis tooling or clearly documented approximations for complex survey design.",
    "Ensure subpopulation or domain analysis is handled correctly and not by naive pre-filtering unless that matches the SAS logic.",
    "Match percentile or quantile definitions as closely as possible when used.",
    "If results may differ from SAS, add brief comments explaining the most likely causes of discrepancies.",
    "SAS identifiers are often case-insensitive, but Python references may be case-sensitive.",
    "At the start of the generated code, normalize input dataframe or dataset column names to lowercase.",
    "After normalizing input columns, use lowercase variable, column, and field references consistently throughout the generated Python code.",
    "When reading files into pandas dataframes, include a step such as df.columns = df.columns.str.lower() or the equivalent lowercase normalization for every relevant input dataframe.",
    "Prefer explicit string-based column access such as df['seqn'] instead of attribute-style access like df.seqn.",
    "If multiple dataframes are used, make sure each relevant dataframe's columns are normalized before later lowercase references are used.",
    "If the SAS uses month variables, month names, month abbreviations, or labels such as jan through dec, treat them as ordered calendar categories rather than plain strings.",
    "For Python plots, tables, and grouped summaries, explicitly enforce calendar month ordering such as Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec or the full-month equivalent when applicable.",
    "Do not leave month-like axes or grouping variables in default lexical or arbitrary order when SAS implies calendar order.",
    "If the SAS references local files or uploaded files, never hardcode absolute local machine paths in the generated code.",
    "Use only the filename or a relative/path-variable style reference instead of direct local paths.",
    "Return only Python code, no markdown or commentary.",
    context.additionalGuidance?.trim()
      ? `Additional user-provided guidance:\n${context.additionalGuidance.trim()}`
      : "",
    context.referenceUrl?.trim()
      ? `Reference URL provided by the user (use it as methodological context if relevant):\n${context.referenceUrl.trim()}`
      : "",
    shouldUsePercentileExample(sasCode)
      ? getPercentileExamplePrompt("PYTHON")
      : "",
    "",
    sasCode,
  ].join("\n");
  return generateWithOpenAIFallback(prompt, "conversion");
}

export async function convertSasToR(sasCode: string) {
  return convertSasToRWithContext(sasCode, {});
}

export async function convertSasToRWithContext(
  sasCode: string,
  context: ConversionContext,
) {
  const prompt = [
    "Convert the following SAS code to idiomatic, production-ready R.",
    "Use tidyverse or data.table where appropriate and preserve logic and comments.",
    "Preserve SAS documentation headers and block comments, including banner-style comment sections such as /* ***** ... */.",
    "Rewrite every SAS comment as an equivalent R comment instead of dropping or summarizing it.",
    "If the SAS file begins with a top-of-file documentation banner or header block, reproduce that header at the top of the R file as R comments.",
    "Do not omit file metadata sections such as File, Purpose, Date, Date Revised, Note, Input Datasets, or Programmer when they appear in the SAS header.",
    "Treat the opening SAS documentation block as required output, not optional context.",
    "Ensure the numerical results, especially confidence intervals such as 95% CI, match the SAS output as closely as possible.",
    "Do not simplify or hard-code intermediate values or estimated quantities.",
    "Generate deterministic code so repeated runs on the same input data produce the same results.",
    "Do not introduce randomness, sampling, bootstrapping, or randomized approximations unless the SAS source explicitly uses them.",
    "If the SAS source explicitly requires randomness, set fixed seeds and document them in code comments.",
    "Preserve the same statistical logic used in SAS, including weighting, subpopulation or domain analysis, variance estimation method, degrees of freedom, and distribution assumptions such as t versus normal.",
    "Match SAS procedures as closely as possible, including PROC SURVEYMEANS, PROC DESCRIPT, and PROC UNIVARIATE behavior when applicable.",
    "If exact R equivalents do not exist, document any approximation clearly in code comments near the relevant step.",
    "Use appropriate R packages such as survey when the SAS code uses complex survey design.",
    "Ensure subpopulation or domain analysis is handled correctly and not by naive pre-filtering unless that matches the SAS logic.",
    "Match percentile or quantile definitions as closely as possible when used.",
    "If results may differ from SAS, add brief comments explaining the most likely causes of discrepancies.",
    "SAS identifiers are often case-insensitive, but R references may be case-sensitive depending on the data frame and tooling.",
    "At the start of the generated code, normalize input dataframe or dataset column names to lowercase.",
    "After normalizing input columns, use lowercase variable, column, and field references consistently throughout the generated R code.",
    "When reading files into data frames, include a step such as names(df) <- tolower(names(df)) or the equivalent lowercase normalization for every relevant input dataframe.",
    "Prefer string-safe dataframe column access patterns such as df[['seqn']] when needed.",
    "If multiple dataframes are used, make sure each relevant dataframe's columns are normalized before later lowercase references are used.",
    "If the SAS uses month variables, month names, month abbreviations, or labels such as jan through dec, treat them as ordered calendar categories rather than plain strings.",
    "For R plots, tables, and grouped summaries, explicitly enforce calendar month ordering such as Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec or the full-month equivalent when applicable, for example with an ordered factor.",
    "Do not leave month-like axes or grouping variables in default lexical or arbitrary order when SAS implies calendar order.",
    "If the SAS references local files or uploaded files, never hardcode absolute local machine paths in the generated code.",
    "Use only the filename or a relative/path-variable style reference instead of direct local paths.",
    "Return only R code, no markdown or commentary.",
    context.additionalGuidance?.trim()
      ? `Additional user-provided guidance:\n${context.additionalGuidance.trim()}`
      : "",
    context.referenceUrl?.trim()
      ? `Reference URL provided by the user (use it as methodological context if relevant):\n${context.referenceUrl.trim()}`
      : "",
    shouldUsePercentileExample(sasCode)
      ? getPercentileExamplePrompt("R")
      : "",
    "",
    sasCode,
  ].join("\n");
  return generateWithOpenAIFallback(prompt, "conversion");
}

export async function refineConversion(
  sasCode: string,
  convertedCode: string,
  instruction: string,
  language: "PYTHON" | "R",
  context: ConversionContext = {},
) {
  const target = language === "R" ? "R" : "Python";
  const prompt = [
    `You are improving an existing SAS to ${target} conversion.`,
    "Apply the user's instruction while preserving the SAS logic.",
    "Preserve all SAS documentation comments and banner/header comment blocks in the updated code.",
    `Keep any top-of-file SAS documentation banner at the top of the updated ${target} file as target-language comments.`,
    "Keep the updated code deterministic so repeated runs on the same input data produce the same results.",
    "Do not introduce randomness, sampling, bootstrapping, or randomized approximations unless the SAS source explicitly uses them.",
    "SAS identifiers are case-insensitive, but the updated target-language code must normalize input dataset columns to lowercase and then use lowercase references consistently.",
    "If the current code does not already normalize relevant input dataframe or dataset column names to lowercase, add that normalization step before later field references.",
    language === "R"
      ? "Prefer string-safe dataframe column access such as df[['seqn']] after lowercase normalization."
      : "Prefer explicit string-based dataframe column access such as df['seqn'] instead of attribute-style access like df.seqn after lowercase normalization.",
    language === "R"
      ? "If the code uses month-like variables or month labels, preserve or add explicit calendar ordering such as an ordered factor from Jan through Dec instead of relying on default string ordering."
      : "If the code uses month-like variables or month labels, preserve or add explicit calendar ordering such as a categorical dtype from Jan through Dec instead of relying on default string ordering.",
    language === "R"
      ? "Preserve or add SAS-matching statistical logic for weighting, domain analysis, variance estimation, degrees of freedom, distribution assumptions, and 95% confidence interval calculations."
      : "Preserve or add SAS-matching statistical logic for weighting, domain analysis, variance estimation, degrees of freedom, distribution assumptions, and 95% confidence interval calculations.",
    "Do not introduce absolute local file paths; keep file references as filenames or relative/path-variable style references only.",
    `Return only the updated ${target} code, no markdown or commentary.`,
    "",
    "User instruction:",
    instruction,
    "",
    context.additionalGuidance?.trim()
      ? `Additional user-provided guidance:\n${context.additionalGuidance.trim()}`
      : "",
    context.referenceUrl?.trim()
      ? `Reference URL provided by the user (use it as methodological context if relevant):\n${context.referenceUrl.trim()}`
      : "",
    "",
    "SAS source:",
    sasCode,
    "",
    `Current ${target} conversion:`,
    convertedCode,
  ].join("\n");
  return generateWithOpenAI(prompt, "conversion");
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

  const raw = await generateWithOpenAI(prompt, "analysis");
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

export async function discussConversion(params: {
  sasCode: string;
  convertedCode: string;
  language: "PYTHON" | "R";
  messages: ConversationMessageInput[];
  additionalGuidance?: string;
  referenceUrl?: string;
}) {
  const target = params.language === "R" ? "R" : "Python";
  const transcript = params.messages
    .map(
      (message) =>
        `${message.role === "assistant" ? "Assistant" : "User"}:\n${message.content}`,
    )
    .join("\n\n");

  const prompt = [
    `You are helping a user debug and refine a SAS to ${target} conversion.`,
    "Answer conversationally and practically.",
    "Explain likely causes, suggested fixes, and what the user should change.",
    "Do not rewrite the full code unless the user explicitly asks for a code rewrite.",
    "If an error message is provided, focus on diagnosing that error against the current converted code.",
    "Keep the answer concise but actionable.",
    "",
    params.additionalGuidance?.trim()
      ? `Additional user-provided guidance:\n${params.additionalGuidance.trim()}`
      : "",
    params.referenceUrl?.trim()
      ? `Reference URL provided by the user (use it as methodological context if relevant):\n${params.referenceUrl.trim()}`
      : "",
    "",
    "SAS source:",
    params.sasCode,
    "",
    `Current ${target} conversion:`,
    params.convertedCode,
    "",
    "Conversation so far:",
    transcript,
  ].join("\n");

  return generateWithOpenAI(prompt, "conversation");
}
