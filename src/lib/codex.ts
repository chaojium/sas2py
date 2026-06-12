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

const PYTHON_FILE_PATH_PROMPT = [
  "Treat SAS LIBNAME folder assignments as source-data location hints, not runtime paths to reproduce.",
  "If SAS has a statement such as LIBNAME rss \"C:\\...\\Data\" and later references rss.member, do not create a Python variable for that local folder and do not preserve the local folder path.",
  "For uploaded execution data, derive the physical filename from the SAS member name, for example rss.nhis_class should read nhis_class.sas7bdat from the uploaded input directory.",
  "Use os.environ.get('SAS2PY_INPUT_DIR', '.') or pathlib.Path(os.environ.get('SAS2PY_INPUT_DIR', '.')) as the input directory, then join only the dataset filename.",
  "Do not set input directory variables to an empty string just to remove a SAS local path; use the execution input directory with current-directory fallback.",
  "Use relative filenames for outputs unless the user explicitly asks for a storage location.",
].join("\n");

const R_FILE_PATH_PROMPT = [
  "Treat SAS LIBNAME folder assignments as source-data location hints, not runtime paths to reproduce.",
  "If SAS has a statement such as LIBNAME rss \"C:\\...\\Data\" and later references rss.member, do not create an R variable for that local folder and do not preserve the local folder path.",
  "For uploaded execution data, derive the physical filename from the SAS member name, for example rss.nhis_class should read nhis_class.sas7bdat from the uploaded input directory.",
  "Use input_dir <- Sys.getenv('SAS2PY_INPUT_DIR', unset = getwd()), then file.path(input_dir, '<dataset>.sas7bdat') for SAS dataset inputs.",
  "Do not set input directory variables to an empty string just to remove a SAS local path; use the execution input directory with current-directory fallback.",
  "Use relative filenames for outputs unless the user explicitly asks for a storage location.",
].join("\n");

const PYTHON_COLUMN_NAME_PROMPT = [
  "Normalize input dataset column names to target-safe canonical names immediately after reading each dataset, then use only those canonical names in the rest of the Python code.",
  "Canonical names should be lowercase, should replace spaces or punctuation with underscores, should remove leading underscores, and should be valid Python identifiers.",
  "For SAS variables that start with underscores, use the same name without the leading underscores when there is no collision, for example use racegr for SAS variable _racegr.",
  "If a canonical name would start with a digit, prefix it with x; if two SAS variables map to the same canonical name, make the names unique deterministically and use the chosen names consistently.",
  "After canonicalizing columns, do not reference the old SAS column name such as _racegr in dataframe operations.",
].join("\n");

const R_COLUMN_NAME_PROMPT = [
  "Normalize input dataset column names to target-safe canonical names immediately after reading each dataset, then use only those canonical names in the rest of the R code.",
  "Canonical names should be lowercase, should replace spaces or punctuation with underscores, should remove leading underscores, and should be syntactically valid R names.",
  "For SAS variables that start with underscores, use the same name without the leading underscores when there is no collision, for example use racegr for SAS variable _racegr.",
  "If a canonical name would start with a digit, prefix it with x; if two SAS variables map to the same canonical name, make the names unique deterministically and use the chosen names consistently.",
  "After canonicalizing columns, do not reference the old SAS column name such as _racegr in dataframe operations or formulas.",
].join("\n");

const R_FACTOR_LEVEL_PROMPT = [
  "When translating SAS CLASS variables, FORMAT values, PARAM=REF, REF=, model terms, or prediction/newdata code to R factors, do not hard-code a reference level such as ref = '1' unless that exact level is guaranteed to exist after the column's type and labels are normalized.",
  "Before using relevel(), explicitly create factors with stable levels derived from the training/input data or from the SAS class/ref specification, and verify the requested reference is present in levels(f).",
  "If SAS numeric codes are kept as numeric values, use numeric factor levels consistently; if SAS labels are converted to strings, use the label text as the reference instead of the original numeric code.",
  "Prefer helper logic that safely chooses an existing reference level, for example by using the SAS REF= value only when it exists and otherwise falling back to the first observed level with a clear code comment.",
  "For predict() newdata and predictive margins, reuse the same factor levels as the model-fitting data; do not recompute factor levels independently inside each map/apply iteration.",
  "Avoid patterns like relevel(factor(sex), ref = '1') inside mutate() unless '1' is confirmed to be an existing factor level for that data.",
].join("\n");

const R_PREDICTIVE_MARGIN_PROMPT = [
  "When translating SAS predicted margins, PREDMARG, LSMEANS, marginal means, or repeated predict() calls in R, always align prediction vectors, standard-error vectors, and weights from the same rows.",
  "Do not call weighted.mean(as.numeric(pred), w = newdata$weight_column) unless length(pred) and length(newdata$weight_column) are explicitly known to match after missing-value handling.",
  "Before predict(), build a complete-case model/prediction dataset using the model variables and weight variable; use that same filtered dataset for newdata, prediction, and weights.",
  "If predict(..., se.fit = TRUE) returns a list, use pred$fit for fitted values and pred$se.fit for standard errors; otherwise use the returned vector as fitted values.",
  "After prediction, create weights <- newdata$weight_column and apply the same finite/non-missing mask to pred_fit and weights before weighted.mean().",
  "Only return NA for a margin when there are no valid aligned prediction/weight rows or the quantity is genuinely not estimable; do not use NA as a shortcut when a valid SAS-equivalent estimate, SE, or CI can be computed.",
].join("\n");

const R_CONFIDENCE_INTERVAL_PROMPT = [
  "For R translations, preserve the SAS/SUDAAN confidence-interval method and scale; do not change CI formulas merely to make the code run.",
  "For survey means, proportions, row percentages, domain estimates, and weighted totals, use design-based estimates and standard errors from the survey design where possible, not unweighted binomial or simple Wald formulas unless the SAS source uses those.",
  "When reporting percentages, multiply the estimate, standard error, lower CI, and upper CI by 100 consistently; do not mix proportion-scale SE/CI with percent-scale estimates.",
  "Use the same critical-value family as the SAS procedure: t-based intervals with survey/design degrees of freedom when SAS/SUDAAN uses t intervals, and normal intervals only when the source procedure or a documented approximation supports it.",
  "For logistic regression odds ratios, compute confidence limits as exp(beta +/- critical_value * SE(beta)) using the same model covariance and degrees-of-freedom logic as the translated model.",
  "For predicted margins or marginal effects, compute SEs and CIs from the model variance-covariance matrix with a delta-method style calculation when feasible; do not set SE or CI to NA if the covariance information is available.",
  "Runtime guards should prevent crashes but must not replace valid SE or CI calculations with zero, NA, or placeholder values except for empty domains, singular/non-estimable terms, or missing covariance information, with a brief code comment.",
].join("\n");

const PYTHON_OUTPUT_FORMAT_PROMPT = [
  "Preserve SAS output file formats and workbook structure whenever practical.",
  "If SAS uses PROC EXPORT with DBMS=XLS, DBMS=XLSX, DBMS=EXCEL, an OUTFILE ending in .xls or .xlsx, or multiple SHEET= outputs to the same workbook, generate one Excel workbook with matching sheet names rather than replacing it with separate CSV files.",
  "For Python, prefer pandas.ExcelWriter with openpyxl or xlsxwriter for multi-sheet Excel output.",
  "Use relative output paths in the current working directory so the app can collect generated files as artifacts.",
  "Only generate CSV files when the SAS source explicitly writes CSV output or when Excel output is impossible; if using a CSV fallback, explain the fallback in a code comment and do not claim that an Excel workbook was created unless it was actually saved.",
].join("\n");

const R_OUTPUT_FORMAT_PROMPT = [
  "Preserve SAS output file formats and workbook structure whenever practical.",
  "If SAS uses PROC EXPORT with DBMS=XLS, DBMS=XLSX, DBMS=EXCEL, an OUTFILE ending in .xls or .xlsx, or multiple SHEET= outputs to the same workbook, generate one Excel workbook with matching sheet names rather than replacing it with separate CSV files.",
  "For R, prefer openxlsx::createWorkbook(), openxlsx::addWorksheet(), openxlsx::writeData(), and openxlsx::saveWorkbook() for multi-sheet Excel output.",
  "Use relative output paths in the current working directory so the app can collect generated files as artifacts.",
  "Only generate CSV files when the SAS source explicitly writes CSV output or when Excel output is impossible; if using a CSV fallback, explain the fallback in a code comment and do not claim that an Excel workbook was created unless it was actually saved.",
].join("\n");

const R_TABLE_SCHEMA_PROMPT = [
  "When building R output tables with tibble(), transmute(), bind_rows(), map_dfr(), or map2_dfr(), every row/table must have unique column names before binding.",
  "Do not create a dynamic column with `:=`(!!g, value) and also create explicit columns that may have the same name, such as sex, age, or racegr, in the same tibble() call.",
  "For crosstab or BY-group output that needs fixed columns such as sex, age, and racegr, create the fixed schema once with those columns initialized to NA, then assign the active group column value after creation, for example row[[g]] <- gl.",
  "Avoid relying on `.name_repair` to hide duplicated names; generate unique names by construction instead.",
  "Before finalizing table-building code, check loops where the grouping variable name can equal one of the output column names, and ensure no duplicate names are produced.",
].join("\n");

const PYTHON_SYNTAX_PROMPT = [
  "The returned Python must be syntactically valid and executable as-is.",
  "Before finalizing, mentally validate that the code would pass ast.parse with balanced parentheses, brackets, braces, quotes, and complete function calls.",
  "Do not emit stray prose, partial words, translation artifacts, non-English fragments, or non-ASCII tokens outside Python comments or quoted strings.",
  "Use ASCII in executable code except where non-ASCII text is explicitly required by source data labels inside quoted strings or comments.",
].join("\n");

const R_SYNTAX_PROMPT = [
  "The returned R must be syntactically valid and executable as-is.",
  "Before finalizing, mentally validate that the code would pass parse(text = code) with balanced parentheses, brackets, braces, quotes, pipes, and complete function calls.",
  "Do not emit stray prose, partial words, translation artifacts, non-English fragments, or non-ASCII tokens outside R comments or quoted strings.",
  "Use ASCII in executable code except where non-ASCII text is explicitly required by source data labels inside quoted strings or comments.",
].join("\n");

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
    PYTHON_SYNTAX_PROMPT,
    PYTHON_OUTPUT_FORMAT_PROMPT,
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
    PYTHON_COLUMN_NAME_PROMPT,
    "Prefer explicit string-based column access such as df['seqn'] instead of attribute-style access like df.seqn.",
    "If multiple dataframes are used, make sure each relevant dataframe's columns are normalized before later lowercase references are used.",
    "If the SAS uses month variables, month names, month abbreviations, or labels such as jan through dec, treat them as ordered calendar categories rather than plain strings.",
    "For Python plots, tables, and grouped summaries, explicitly enforce calendar month ordering such as Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec or the full-month equivalent when applicable.",
    "Do not leave month-like axes or grouping variables in default lexical or arbitrary order when SAS implies calendar order.",
    "If the SAS references local files or uploaded files, never hardcode absolute local machine paths in the generated code.",
    PYTHON_FILE_PATH_PROMPT,
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
    R_SYNTAX_PROMPT,
    R_OUTPUT_FORMAT_PROMPT,
    R_TABLE_SCHEMA_PROMPT,
    "Preserve the same statistical logic used in SAS, including weighting, subpopulation or domain analysis, variance estimation method, degrees of freedom, and distribution assumptions such as t versus normal.",
    "Match SAS procedures as closely as possible, including PROC SURVEYMEANS, PROC DESCRIPT, and PROC UNIVARIATE behavior when applicable.",
    "If exact R equivalents do not exist, document any approximation clearly in code comments near the relevant step.",
    "Use appropriate R packages such as survey when the SAS code uses complex survey design.",
    "Ensure subpopulation or domain analysis is handled correctly and not by naive pre-filtering unless that matches the SAS logic.",
    "Match percentile or quantile definitions as closely as possible when used.",
    "If results may differ from SAS, add brief comments explaining the most likely causes of discrepancies.",
    R_CONFIDENCE_INTERVAL_PROMPT,
    R_FACTOR_LEVEL_PROMPT,
    R_PREDICTIVE_MARGIN_PROMPT,
    "SAS identifiers are often case-insensitive, but R references may be case-sensitive depending on the data frame and tooling.",
    "At the start of the generated code, normalize input dataframe or dataset column names to lowercase.",
    "After normalizing input columns, use lowercase variable, column, and field references consistently throughout the generated R code.",
    "When reading files into data frames, include a step such as names(df) <- tolower(names(df)) or the equivalent lowercase normalization for every relevant input dataframe.",
    R_COLUMN_NAME_PROMPT,
    "Prefer string-safe dataframe column access patterns such as df[['seqn']] when needed.",
    "If multiple dataframes are used, make sure each relevant dataframe's columns are normalized before later lowercase references are used.",
    "If the SAS uses month variables, month names, month abbreviations, or labels such as jan through dec, treat them as ordered calendar categories rather than plain strings.",
    "For R plots, tables, and grouped summaries, explicitly enforce calendar month ordering such as Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec or the full-month equivalent when applicable, for example with an ordered factor.",
    "Do not leave month-like axes or grouping variables in default lexical or arbitrary order when SAS implies calendar order.",
    "If the SAS references local files or uploaded files, never hardcode absolute local machine paths in the generated code.",
    R_FILE_PATH_PROMPT,
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
    language === "R" ? R_SYNTAX_PROMPT : PYTHON_SYNTAX_PROMPT,
    language === "R" ? R_OUTPUT_FORMAT_PROMPT : PYTHON_OUTPUT_FORMAT_PROMPT,
    language === "R" ? R_TABLE_SCHEMA_PROMPT : "",
    "SAS identifiers are case-insensitive, but the updated target-language code must normalize input dataset columns to lowercase and then use lowercase references consistently.",
    "If the current code does not already normalize relevant input dataframe or dataset column names to lowercase, add that normalization step before later field references.",
    language === "R" ? R_COLUMN_NAME_PROMPT : PYTHON_COLUMN_NAME_PROMPT,
    language === "R"
      ? "Prefer string-safe dataframe column access such as df[['seqn']] after lowercase normalization."
      : "Prefer explicit string-based dataframe column access such as df['seqn'] instead of attribute-style access like df.seqn after lowercase normalization.",
    language === "R"
      ? "If the code uses month-like variables or month labels, preserve or add explicit calendar ordering such as an ordered factor from Jan through Dec instead of relying on default string ordering."
      : "If the code uses month-like variables or month labels, preserve or add explicit calendar ordering such as a categorical dtype from Jan through Dec instead of relying on default string ordering.",
    language === "R"
      ? "Preserve or add SAS-matching statistical logic for weighting, domain analysis, variance estimation, degrees of freedom, distribution assumptions, and 95% confidence interval calculations."
      : "Preserve or add SAS-matching statistical logic for weighting, domain analysis, variance estimation, degrees of freedom, distribution assumptions, and 95% confidence interval calculations.",
    language === "R" ? R_CONFIDENCE_INTERVAL_PROMPT : "",
    language === "R" ? R_FACTOR_LEVEL_PROMPT : "",
    language === "R" ? R_PREDICTIVE_MARGIN_PROMPT : "",
    "Do not introduce absolute local file paths.",
    language === "R" ? R_FILE_PATH_PROMPT : PYTHON_FILE_PATH_PROMPT,
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
    params.language === "R" ? R_SYNTAX_PROMPT : PYTHON_SYNTAX_PROMPT,
    params.language === "R" ? R_OUTPUT_FORMAT_PROMPT : PYTHON_OUTPUT_FORMAT_PROMPT,
    params.language === "R" ? R_TABLE_SCHEMA_PROMPT : "",
    "When suggesting code changes, keep dataset column names target-safe and canonicalized after input loading.",
    params.language === "R" ? R_COLUMN_NAME_PROMPT : PYTHON_COLUMN_NAME_PROMPT,
    params.language === "R" ? R_CONFIDENCE_INTERVAL_PROMPT : "",
    params.language === "R" ? R_FACTOR_LEVEL_PROMPT : "",
    params.language === "R" ? R_PREDICTIVE_MARGIN_PROMPT : "",
    params.language === "R" ? R_FILE_PATH_PROMPT : PYTHON_FILE_PATH_PROMPT,
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
