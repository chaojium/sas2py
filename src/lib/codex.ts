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

const PYTHON_SAS_READER_PROMPT = [
  "When reading .sas7bdat files in Python, generate reader code that is compatible with common pandas and pyreadstat versions.",
  "Do not pass apply_value_formats to pyreadstat.read_sas7bdat(); that keyword is not supported by some installed pyreadstat versions and can raise TypeError.",
  "Prefer a small read_sas_dataset(path) helper that tries pyreadstat.read_sas7bdat(path) first, falls back to pandas.read_sas(path, format='sas7bdat'), and only uses optional keyword arguments after verifying they are supported or catching TypeError and retrying without them.",
  "Do not rely on pyreadstat automatically applying SAS formats from a .sas7bdat alone; if display labels are needed, translate PROC FORMAT and FORMAT statements from the SAS source into explicit Python mapping dictionaries.",
  "Keep raw coded values for calculations and use explicit format-label mappings only for display/output columns.",
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

const PYTHON_FACTOR_LEVEL_PROMPT = [
  "When translating SAS CLASS variables, FORMAT values, PARAM=REF, REF=, model terms, or prediction/newdata code to Python, do not hard-code a reference level such as reference='1' unless that exact level is guaranteed to exist after the column's type and labels are normalized.",
  "Before using pandas Categorical, statsmodels C(..., Treatment(reference=...)), get_dummies(drop_first=...), or manually selected baseline columns, explicitly create stable levels derived from the training/input data or from the SAS class/ref specification, and verify the requested reference is present.",
  "If SAS numeric codes are kept as numeric values, use numeric category levels consistently; if SAS labels are converted to strings, use the label text as the reference instead of the original numeric code.",
  "Prefer helper logic that safely chooses an existing reference level, for example by using the SAS REF= value only when it exists and otherwise falling back to the first observed level with a clear code comment.",
  "For predict() newdata, predictive margins, and marginal effects, reuse the same category levels and encoded columns as the model-fitting data; do not recompute category levels independently inside each loop.",
  "Avoid patterns that assume a baseline category such as '1' unless '1' is confirmed to be an existing category for that data.",
].join("\n");

const R_PREDICTIVE_MARGIN_PROMPT = [
  "When translating SAS predicted margins, PREDMARG, LSMEANS, marginal means, or repeated predict() calls in R, always align prediction vectors, standard-error vectors, and weights from the same rows.",
  "Do not call weighted.mean(as.numeric(pred), w = newdata$weight_column) unless length(pred) and length(newdata$weight_column) are explicitly known to match after missing-value handling.",
  "Before predict(), build a complete-case model/prediction dataset using the model variables and weight variable; use that same filtered dataset for newdata, prediction, and weights.",
  "If predict(..., se.fit = TRUE) returns a list, use pred$fit for fitted values and pred$se.fit for standard errors; otherwise use the returned vector as fitted values.",
  "After prediction, create weights <- newdata$weight_column and apply the same finite/non-missing mask to pred_fit and weights before weighted.mean().",
  "Only return NA for a margin when there are no valid aligned prediction/weight rows or the quantity is genuinely not estimable; do not use NA as a shortcut when a valid SAS-equivalent estimate, SE, or CI can be computed.",
].join("\n");

const PYTHON_PREDICTIVE_MARGIN_PROMPT = [
  "When translating SAS predicted margins, PREDMARG, LSMEANS, marginal means, or repeated prediction calls in Python, always align prediction vectors, standard-error vectors, and weights from the same rows.",
  "Do not call numpy.average(pred, weights=df[weight_column]) unless len(pred) and len(weights) are explicitly known to match after missing-value handling.",
  "Before prediction, build a complete-case model/prediction dataset using the model variables and weight variable; use that same filtered dataset for newdata, prediction, and weights.",
  "If statsmodels returns prediction results, use get_prediction(...).predicted_mean or summary_frame() for fitted values and available standard errors; otherwise use the returned vector as fitted values.",
  "After prediction, create weights from the exact newdata rows used for prediction and apply the same finite/non-missing mask to predictions, standard errors, and weights before weighted averages.",
  "Only return NaN for a margin when there are no valid aligned prediction/weight rows or the quantity is genuinely not estimable; do not use NaN as a shortcut when a valid SAS-equivalent estimate, SE, or CI can be computed.",
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

const PYTHON_CONFIDENCE_INTERVAL_PROMPT = [
  "For Python translations, preserve the SAS/SUDAAN confidence-interval method and scale; do not change CI formulas merely to make the code run.",
  "For survey means, proportions, row percentages, domain estimates, and weighted totals, use design-based estimates and standard errors from the survey design where possible, not unweighted binomial or simple Wald formulas unless the SAS source uses those.",
  "When reporting percentages, multiply the estimate, standard error, lower CI, and upper CI by 100 consistently; do not mix proportion-scale SE/CI with percent-scale estimates.",
  "Use the same critical-value family as the SAS procedure: t-based intervals with survey/design degrees of freedom when SAS/SUDAAN uses t intervals, and normal intervals only when the source procedure or a documented approximation supports it.",
  "For logistic regression odds ratios, compute confidence limits as exp(beta +/- critical_value * SE(beta)) using the same model covariance and degrees-of-freedom logic as the translated model.",
  "For predicted margins or marginal effects, compute SEs and CIs from the model variance-covariance matrix with a delta-method style calculation when feasible; do not set SE or CI to NaN if the covariance information is available.",
  "Runtime guards should prevent crashes but must not replace valid SE or CI calculations with zero, NaN, or placeholder values except for empty domains, singular/non-estimable terms, or missing covariance information, with a brief code comment.",
].join("\n");

const PYTHON_SUDAAN_PROMPT = [
  "When SAS code uses SUDAAN procedures or SUDAAN-style syntax, translate them as complex survey analyses, not as ordinary pandas summaries.",
  "Treat PROC DESCRIPT, PROC CROSSTAB, PROC RLOGIST, PROC REGRESS, NEST, WEIGHT, SUBPOPN, SUBGROUP, LEVELS, CLASS, TABLES, MODEL, PREDMARG, PRINT, SETENV, DESIGN=, and related SUDAAN statements as statistical method specifications that must drive the Python implementation.",
  "Always carry the survey design variables from NEST or equivalent statements: strata variables, PSU/cluster variables, and weight variables. Do not drop them after reading or recoding data.",
  "Do not use plain pandas value_counts(), crosstab(), groupby().mean(), scipy chi-square, or unweighted statsmodels models as the final SUDAAN replacement when weights, NEST, SUBPOPN, or survey design statements are present.",
  "For PROC DESCRIPT, compute weighted means/proportions/totals and design-based standard errors using Taylor-linearization-style PSU-by-stratum aggregation where possible; if a limited approximation is required, state the approximation in a code comment next to the helper.",
  "For PROC CROSSTAB, compute weighted counts, row/column percentages, standard errors, confidence limits, and tests using the survey design. Avoid ordinary unweighted chi-square tests; use a documented Rao-Scott or Wald-style survey approximation when an exact SUDAAN test is unavailable.",
  "For PROC CROSSTAB output columns such as wsum, weighted count, total, or weighted_frequency, compute the value from the current table cell, current row domain, or current subgroup mask as specified by the SAS output, not from the full analytic dataset.",
  "Never assign the grand total sum of weights to every crosstab row. In nested loops, build a mask such as domain_mask & row_level_mask & column_level_mask before summing weights for a cell-level wsum, and use domain_mask & row_level_mask for a row-total wsum.",
  "If a crosstab has variables such as agecat_b by glp_med12m, the wsum values should vary by age/GLP cell or by requested row/column total; their sum may equal the overall weighted total, but each row should not equal the overall weighted total.",
  "When using pandas, prefer groupby over the exact crosstab variables with observed=False/dropna=False as appropriate and aggregate the weight column with sum, rather than computing total_weight once and copying it into every output row.",
  "For SUDAAN CHISQ, LLCHISQ, WALDCHISQ, CMH, ACMH, or association tests, produce nonblank test-result rows whenever the source requests the test and the input table has estimable dimensions; include statistic, degrees of freedom or parameter text, p-value, and a note describing the approximation.",
  "Do not output an ACMH row with blank statistic and p-value merely because exact SUDAAN ACMH is unavailable; compute the closest documented design-adjusted CMH/Rao-Scott/Wald-style association approximation, or explicitly reuse the available design-adjusted association test row with a clear note when ACMH cannot be separated.",
  "Never compute or report a raw Pearson chi-square statistic directly from survey-weighted population totals as a Rao-Scott or SUDAAN-like statistic; those values can be inflated by the sum of weights and produce million-scale statistics that are not comparable to SUDAAN or R survey output.",
  "For Rao-Scott-like tests in Python, base the test on weighted proportions plus design-based covariance, an effective sample size, or a documented design-effect adjustment, and report t/F/chi-square statistics on a scale comparable to standard survey software rather than on the weighted population-total scale.",
  "When generating both adjusted F and adjusted chi-square rows, align their df, p-value calculation, and notes with the same design-adjusted association approximation, similar in structure to R survey::svychisq adjusted F and Rao-Scott chi-square outputs.",
  "For PROC RLOGIST or logistic SUDAAN models, use survey weights and cluster-aware covariance at minimum, preserving class/reference levels and predicted margins. Prefer statsmodels GLM/logit with weights plus cluster-robust covariance by PSU when no exact survey logistic implementation is available, and document the approximation.",
  "For SUBPOPN or domain/subpopulation analysis, keep the full survey design in variance calculations and use a domain indicator/mask; do not simply filter the dataframe before computing variance unless the SAS/SUDAAN source explicitly does that.",
  "Use survey-design degrees of freedom derived from non-empty PSUs minus non-empty strata when the SAS/SUDAAN procedure uses design df, and use t/F critical values based on those df rather than default normal or chi-square approximations.",
  "Preserve SUDAAN percent scaling, confidence-level defaults, missing-data handling, category ordering, reference levels, and printed output column semantics as closely as possible.",
  "If Python cannot exactly reproduce a SUDAAN variance or test statistic with available packages, generate a clear SUDAAN-like helper with the closest documented approximation rather than silently replacing the method with a simpler non-survey analysis.",
].join("\n");

const PYTHON_SUBGROUP_LABEL_PROMPT = [
  "For Python translations of SAS/SUDAAN SUBGROUP, LEVELS, CLASS, TABLES, FORMAT, and label-driven output, keep variable-name columns and displayed level/value columns semantically separate.",
  "A column that represents the displayed subgroup/category should contain the current subgroup level's formatted label or raw value, not the subgroup variable name repeated for every row.",
  "If the SAS/SUDAAN output includes an overall/total row, generate that row explicitly and place it consistently with the SAS/R output, commonly before subgroup rows with a display label such as 'Overall'.",
  "For example, rows for agecat_b should display values such as '18-39y', '40-64y', and '65y+' when those labels are defined, not 'agecat_b' repeated three times.",
  "Rows for sex, race, poverty, metro, and region variables should likewise display the SAS format labels or observed coded values for each level, such as 'Male', 'Female', 'White', 'Below 100% FPL', 'Metropolitan', or '-6', rather than the source variable name.",
  "When SAS defines PROC FORMAT VALUE mappings and FORMAT statements that attach those mappings to variables, create both a format_maps dictionary and a variable_formats dictionary in Python, then use them for all displayed categorical output.",
  "For variables such as p_sex, p_poverty4_r, nchs_metro, dem_region, race, and agecat_b, do not export raw numeric codes such as 1, 2, 3, or 4 in the main display column when SAS labels exist; export labels such as 'Male', 'Female', 'Below 100% FPL', 'Metropolitan', 'Northeast', 'Midwest', 'South', or 'West'.",
  "If a raw code has no SAS format label, preserve the raw code as a display string, for example '-6'; do not invent labels.",
  "Do not treat special numeric category codes such as -9, -8, -7, -6, -1, 7, 8, or 9 as missing unless the SAS code explicitly recodes them to missing or excludes them with WHERE/IF logic.",
  "When SAS LEVELS or PROC FORMAT defines an ordered category list, iterate over that intended level list rather than only over pandas groupby-observed values, so special or formatted categories such as '-6' are not silently omitted.",
  "Use pandas groupby/crosstab settings and category dtypes that preserve intended categories, for example dropna=False and explicit CategoricalDtype levels when appropriate; do not rely on defaults that drop missing-like or unobserved categories if SAS would print them.",
  "If the output also needs numeric category codes for validation, keep them in a separate column such as p_sex_code or subgroup_code; the human-readable subgroup/category column should still contain the formatted label.",
  "Use a helper such as format_label(variable_name, value) that looks up variable_formats[variable_name] and format_maps[format_name], normalizes numeric/string key differences, and falls back to the raw value only when no label exists.",
  "Apply display labeling before writing CSV files, Excel sheets, printed tables, or combined workbooks so Python and R outputs use the same category text.",
  "If the output needs both pieces of information, use two columns such as subgroup_variable for the source variable name and subgroup for the formatted level label/value.",
  "When translating PROC FORMAT or user-defined SAS formats, build explicit Python mapping dictionaries and use them for output display labels while preserving raw coded values for calculations.",
  "Respect the order implied by SUDAAN LEVELS, SAS formats, or the SAS code; do not sort labels alphabetically when SAS specifies a level order.",
  "Inside loops over subgroup variables, assign the display label from the loop's current level value, for example label = format_label(group_var, level_value), and never assign label = group_var unless the requested output column is specifically the variable-name column.",
].join("\n");

const CROSS_LANGUAGE_NUMERIC_CONSISTENCY_PROMPT = [
  "Choose formulas and defaults that can be implemented consistently in Python and R translations of the same SAS source.",
  "Do not let target-language library defaults decide core statistical behavior when SAS specifies or implies the method.",
  "Explicitly set choices that affect numeric comparability, including missing-value filtering, weights, subpopulation/domain handling, category/reference levels, sort order, variance method, degrees of freedom, critical-value family, percent scaling, and rounding.",
  "When an exact SAS method is unavailable, use the same documented approximation across Python and R whenever feasible so their outputs are comparable.",
].join("\n");

const PYTHON_OUTPUT_FORMAT_PROMPT = [
  "Preserve SAS output file formats and workbook structure whenever practical.",
  "If SAS uses PROC EXPORT with DBMS=XLS, DBMS=XLSX, DBMS=EXCEL, an OUTFILE ending in .xls or .xlsx, or multiple SHEET= outputs to the same workbook, generate one Excel workbook with matching sheet names rather than replacing it with separate CSV files.",
  "For Python, prefer pandas.ExcelWriter with openpyxl or xlsxwriter for multi-sheet Excel output.",
  "When Python code creates multiple tabular CSV outputs from translated SAS tables, also create one companion .xlsx workbook that combines those CSV/table outputs as separate sheets with clear sheet names.",
  "Keep the individual CSV files when they are useful, but do not leave Python output as CSV-only if multiple related tables are produced and Excel output is available.",
  "Generate user-readable downloadable outputs such as .xlsx, .csv, .txt, .html, .pdf, or .png.",
  "Do not generate .pkl or .pickle files as user-facing outputs unless the SAS source explicitly creates a serialized binary analysis object.",
  "Use relative output paths in the current working directory so the app can collect generated files as artifacts.",
  "Only generate CSV files when the SAS source explicitly writes CSV output or when Excel output is impossible; if using a CSV fallback, explain the fallback in a code comment and do not claim that an Excel workbook was created unless it was actually saved.",
].join("\n");

const R_OUTPUT_FORMAT_PROMPT = [
  "Preserve SAS output file formats and workbook structure whenever practical.",
  "If SAS uses PROC EXPORT with DBMS=XLS, DBMS=XLSX, DBMS=EXCEL, an OUTFILE ending in .xls or .xlsx, or multiple SHEET= outputs to the same workbook, generate one Excel workbook with matching sheet names rather than replacing it with separate CSV files.",
  "For R, prefer openxlsx::createWorkbook(), openxlsx::addWorksheet(), openxlsx::writeData(), and openxlsx::saveWorkbook() for multi-sheet Excel output.",
  "Generate user-readable downloadable outputs such as .xlsx, .csv, .txt, .html, .pdf, or .png.",
  "Do not generate .rds or .RData files as user-facing outputs unless the SAS source explicitly creates a serialized binary analysis object; these formats are not suitable as primary demo/download outputs.",
  "Use relative output paths in the current working directory so the app can collect generated files as artifacts.",
  "Only generate CSV files when the SAS source explicitly writes CSV output or when Excel output is impossible; if using a CSV fallback, explain the fallback in a code comment and do not claim that an Excel workbook was created unless it was actually saved.",
].join("\n");

const R_TABLE_SCHEMA_PROMPT = [
  "When building R output tables with tibble(), transmute(), bind_rows(), map_dfr(), or map2_dfr(), every row/table must have unique column names before binding.",
  "Do not create a dynamic column with `:=`(!!g, value) and also create explicit columns that may have the same name, such as sex, age, or racegr, in the same tibble() call.",
  "For crosstab or BY-group output that needs fixed columns such as sex, age, and racegr, create the fixed schema once with those columns initialized to NA, then assign the active group column value after creation, for example row[[g]] <- gl.",
  "Avoid relying on `.name_repair` to hide duplicated names; generate unique names by construction instead.",
  "Before finalizing table-building code, check loops where the grouping variable name can equal one of the output column names, and ensure no duplicate names are produced.",
  "When converting R table(), xtabs(), svytable(), or as.data.frame.table() results, do not assume generic column names such as Var1, Var2, or Freq.",
  "Explicitly set names(dimnames(table_object)) before as.data.frame(), or rename columns by position after checking ncol(), for example names(df)[seq_along(expected_names)] <- expected_names.",
  "Prefer as_tibble(as.data.frame(table_object), .name_repair = 'unique') followed by deterministic column-name assignment based on the variables used to build the table.",
  "Never call rename(agecat_b = Var1, glp_med12m = Var2, weighted_frequency = Freq) unless those source columns are confirmed to exist in names(df).",
].join("\n");

const PYTHON_TABLE_SCHEMA_PROMPT = [
  "When building Python output tables with pandas DataFrame constructors, assign(), concat(), groupby(), crosstab(), pivot_table(), or reset_index(), every table must have unique column names before combining or exporting.",
  "Do not create a dynamic grouping column and also create explicit columns that may have the same name, such as sex, age, or racegr, in the same row/table construction.",
  "For crosstab or BY-group output that needs fixed columns such as sex, age, and racegr, create the fixed schema once with those columns initialized to numpy.nan or pandas.NA, then assign the active group column value after creation, for example row[g] = gl.",
  "Avoid relying on pandas' automatic duplicate-name repair or suffixes to hide duplicated names; generate unique names by construction instead.",
  "Before finalizing table-building code, check loops where the grouping variable name can equal one of the output column names, and ensure no duplicate names are produced.",
  "When converting pandas crosstab, groupby.size(), pivot_table(), weighted crosstabs, or reset_index() results, do not assume generic column names such as Var1, Var2, level_0, index, or 0.",
  "Explicitly set index names, Series names, and DataFrame column names before reset_index(), or rename columns by position after checking the number of columns.",
  "Prefer deterministic column-name assignment based on the variables used to build the table, and never call rename(columns={'Var1': 'agecat_b', 'Var2': 'glp_med12m', 'Freq': 'weighted_frequency'}) unless those source columns are confirmed to exist.",
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
    PYTHON_TABLE_SCHEMA_PROMPT,
    CROSS_LANGUAGE_NUMERIC_CONSISTENCY_PROMPT,
    "Preserve the same statistical logic used in SAS, including weighting, subpopulation or domain analysis, variance estimation method, degrees of freedom, and distribution assumptions such as t versus normal.",
    "Match SAS procedures as closely as possible, including PROC SURVEYMEANS, PROC DESCRIPT, and PROC UNIVARIATE behavior when applicable.",
    "If exact Python equivalents do not exist, document any approximation clearly in code comments near the relevant step.",
    "Use appropriate Python packages such as pandas, numpy, scipy, and statsmodels when the SAS code uses statistical procedures, and use suitable survey-analysis tooling or clearly documented approximations for complex survey design.",
    "Ensure subpopulation or domain analysis is handled correctly and not by naive pre-filtering unless that matches the SAS logic.",
    "Match percentile or quantile definitions as closely as possible when used.",
    "If results may differ from SAS, add brief comments explaining the most likely causes of discrepancies.",
    PYTHON_CONFIDENCE_INTERVAL_PROMPT,
    PYTHON_SUDAAN_PROMPT,
    PYTHON_SUBGROUP_LABEL_PROMPT,
    PYTHON_FACTOR_LEVEL_PROMPT,
    PYTHON_PREDICTIVE_MARGIN_PROMPT,
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
    PYTHON_SAS_READER_PROMPT,
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
    CROSS_LANGUAGE_NUMERIC_CONSISTENCY_PROMPT,
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
    language === "R" ? R_TABLE_SCHEMA_PROMPT : PYTHON_TABLE_SCHEMA_PROMPT,
    CROSS_LANGUAGE_NUMERIC_CONSISTENCY_PROMPT,
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
    language === "R" ? R_CONFIDENCE_INTERVAL_PROMPT : PYTHON_CONFIDENCE_INTERVAL_PROMPT,
    language === "R" ? "" : PYTHON_SUDAAN_PROMPT,
    language === "R" ? "" : PYTHON_SUBGROUP_LABEL_PROMPT,
    language === "R" ? R_FACTOR_LEVEL_PROMPT : PYTHON_FACTOR_LEVEL_PROMPT,
    language === "R" ? R_PREDICTIVE_MARGIN_PROMPT : PYTHON_PREDICTIVE_MARGIN_PROMPT,
    "Do not introduce absolute local file paths.",
    language === "R" ? R_FILE_PATH_PROMPT : PYTHON_FILE_PATH_PROMPT,
    language === "R" ? "" : PYTHON_SAS_READER_PROMPT,
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
    params.language === "R" ? R_TABLE_SCHEMA_PROMPT : PYTHON_TABLE_SCHEMA_PROMPT,
    CROSS_LANGUAGE_NUMERIC_CONSISTENCY_PROMPT,
    "When suggesting code changes, keep dataset column names target-safe and canonicalized after input loading.",
    params.language === "R" ? R_COLUMN_NAME_PROMPT : PYTHON_COLUMN_NAME_PROMPT,
    params.language === "R" ? R_CONFIDENCE_INTERVAL_PROMPT : PYTHON_CONFIDENCE_INTERVAL_PROMPT,
    params.language === "R" ? "" : PYTHON_SUDAAN_PROMPT,
    params.language === "R" ? "" : PYTHON_SUBGROUP_LABEL_PROMPT,
    params.language === "R" ? R_FACTOR_LEVEL_PROMPT : PYTHON_FACTOR_LEVEL_PROMPT,
    params.language === "R" ? R_PREDICTIVE_MARGIN_PROMPT : PYTHON_PREDICTIVE_MARGIN_PROMPT,
    params.language === "R" ? R_FILE_PATH_PROMPT : PYTHON_FILE_PATH_PROMPT,
    params.language === "R" ? "" : PYTHON_SAS_READER_PROMPT,
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
