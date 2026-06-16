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

const R_TIDY_EVAL_PROMPT = [
  "Use .data only directly inside dplyr data-mask verbs such as filter(), mutate(), transmute(), arrange(), summarize(), select(), group_by(), and case_when() inside those verbs. Never use .data[[...]] inside survey::update()/update(), svydesign(), subset(), survey::subset(), model.frame(), predict(), base eval(), or parent.frame().",
  "For dynamic columns in helpers or purrr loops, pass column names or precomputed logical masks and use df[[name]] or design$variables[[name]].",
  "When adding variables to a survey design with update(), use ordinary vectors from the design object, for example update(design, indicator_tmp = as.numeric(design$variables[[var]] == level)); never use update(design, indicator_tmp = as.numeric(.data[[var]] == level)).",
  "Do not create helpers that accept unevaluated domain_expr or indicator_expr arguments and then call eval(substitute(...)); accept logical masks such as domain_mask and indicator_mask instead.",
  "Bad R pattern: survey_mean_binary(design, domain_expr = .data[[g]] == gl, indicator_expr = copd == cl). Good R pattern: domain_mask <- design$variables[[g]] == gl; indicator_mask <- design$variables[['copd']] == cl.",
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
  "Never use pred$fit or pred$se.fit unless is.list(pred) is true. Normalize prediction output with pred_fit <- if (is.list(pred) && !is.null(pred$fit)) as.numeric(pred$fit) else as.numeric(pred); pred_se <- if (is.list(pred) && !is.null(pred$se.fit)) as.numeric(pred$se.fit) else rep(NA_real_, length(pred_fit)).",
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

const R_SUDAAN_PROMPT = [
  "For SAS/SUDAAN procedures, use survey-design R code, not ordinary dplyr/table/chisq.test summaries. Preserve NEST strata/PSU, WEIGHT, DESIGN=WR/MISSUNIT, SUBPOPN/domain logic, CLASS/TABLES/VAR/CATLEVEL order, labels, percent scale, and output rows.",
  "For survey design modifications, call the S3 generic update(design, ...) after loading survey; do not call survey::update(...) because update is not exported from the survey namespace.",
  "PROC CROSSTAB: compute NSUM as the unweighted cell/domain count; compute row/column/total percentages, SEs, CIs, and tests from the survey design. For TEST CHISQ with OUTPUT STESTVAL SDF SPVAL, use a SUDAAN-like adjusted Wald F statistic, for example survey::svychisq(..., statistic = 'adjWald'), not survey::svychisq(..., statistic = 'Chisq'). SDF is nominal table df (row levels - 1) * (column levels - 1); do not export survey::svychisq(statistic='F') denominator/parameter df as SDF.",
  "For svychisq, create explicit factor columns in the design, for example group_tmp and outcome_tmp, and test ~group_tmp + outcome_tmp; do not rely on factor(variable) inside the formula if it causes blank output.",
  "PROC DESCRIPT: include SUDAAN overall rows such as _ONE_=0/ROW_NAME='0' when downstream SAS cleanup expects them.",
  "When extracting survey estimates or SEs in R, do not directly call SE(est)[['indicator_tmp']], coef(est)[['indicator_tmp']], or vcov(est)[name, name] unless the name exists. survey::SE(), coef(), and vcov() may return unnamed vectors/matrices or names such as factor-level columns. Store coef(est), SE(est), and vcov(est), check names()/rownames()/colnames(), and fall back to as.numeric(...)[1] or a documented positional extraction for one-variable estimates.",
  "All domain masks and count guards must be NA-safe: replace missing values in logical masks with FALSE before subsetting, compute NSUM/denominators with sum(..., na.rm = TRUE), and guard empty domains with if (is.na(nsum) || nsum == 0), not if (nsum == 0).",
  "PAIRWISE/POLY/CONTRAST: operate on the requested SUDAAN estimate scale, usually CATLEVEL PERCENT, with the design covariance matrix. Prefer survey::svyby(..., covmat=TRUE) plus survey::svycontrast(); names(coefs) must come from names(coef(est)), not invented strings such as indicator:1.",
  "PROC DESCRIPT POLY var = 2: output separate linear and quadratic rows using the ordered CLASS level scores on SUDAAN's displayed estimate scale. Do not use arbitrary integer-rescaled coefficients because PERCENT and SEPERCENT must match SAS, not only the p-value. For six ordered AGE levels scored 1:6, linear coefficients are c(-2.5, -1.5, -0.5, 0.5, 1.5, 2.5), and quadratic coefficients are c(10/3, -2/3, -8/3, -8/3, -2/3, 10/3).",
  "For crossed CONTRAST statements such as SEX=(...) * RACE=(...), compute PERCENT, SEPERCENT, and P_PCT for the full interaction grid. Collapse the crossed domains into one factor in SAS order, compute survey::svyby(..., covmat=TRUE), align coefficients to names(coef(est)), and use the full covariance matrix. Do not restrict contrast SEs to one-way domain_vars, and do not leave crossed contrast SEPERCENT or P_PCT blank.",
  "Translate crossed CONTRAST coefficients literally and do not leave estimable rows blank. Do not use tryCatch(..., error=function(e) NULL) to silently export blank test or contrast values; compute a documented fallback or stop with an informative helper error.",
  "For RLOGIST/model Wald tests, do not pass a numeric contrast matrix to survey::regTermTest(); regTermTest expects model terms/formulas. For explicit coefficient sets, coerce term names with as.character(), subset coef(model) and vcov(model), and compute the Wald chi-square manually as t(beta) %*% solve(vcov_subset) %*% beta with a chi-square p-value.",
  "For confidence intervals, use design-based SEs and t critical values with survey df when SUDAAN Taylor WR variance is implied. If exact SUDAAN parity is unavailable, use the closest documented survey approximation with a short code comment.",
].join("\n");

const R_SUBGROUP_LABEL_PROMPT = [
  "For R translations of SAS/SUDAAN SUBGROUP, LEVELS, CLASS, TABLES, FORMAT, and label-driven output, preserve display labels separately from raw numeric codes.",
  "Build an explicit format lookup for every variable that appears in SUBGROUP/TABLES output, including variables whose labels may come from PROC FORMAT, FORMAT statements, included setup files, or dataset labels. Do not only label derived variables.",
  "When using haven::zap_labels(), first preserve value labels or recreate equivalent label maps; do not zap labels and then output raw codes as subgroup_label/row_label.",
  "For output columns named subgroup_label, row_label, row_name, var_name, or column_label, use the format lookup for the current variable and level. Raw codes may remain in separate subgroup_level/code columns, but display-label columns should be human-readable.",
  "For CDC/NCHS-style RSS variables, keep common labels when present or inferable: p_poverty4_r = Below 100% FPL, 100%-199% FPL, 200%-399% FPL, 400%+ FPL; nchs_metro = Metropolitan, Nonmetropolitan; dem_region = Northeast, Midwest, South, West.",
].join("\n");

const PYTHON_SUDAAN_PROMPT = [
  "When SAS code uses SUDAAN procedures or SUDAAN-style syntax, translate them as complex survey analyses, not as ordinary pandas summaries.",
  "Treat PROC DESCRIPT, PROC CROSSTAB, PROC RLOGIST, PROC REGRESS, NEST, WEIGHT, SUBPOPN, SUBGROUP, LEVELS, CLASS, TABLES, MODEL, PREDMARG, PRINT, SETENV, DESIGN=, and related SUDAAN statements as statistical method specifications that must drive the Python implementation.",
  "Always carry the survey design variables from NEST or equivalent statements: strata variables, PSU/cluster variables, and weight variables. Do not drop them after reading or recoding data.",
  "Do not use plain pandas value_counts(), crosstab(), groupby().mean(), scipy chi-square, or unweighted statsmodels models as the final SUDAAN replacement when weights, NEST, SUBPOPN, or survey design statements are present.",
  "For PROC DESCRIPT, compute weighted means/proportions/totals and design-based standard errors using Taylor-linearization-style PSU-by-stratum aggregation where possible; if a limited approximation is required, state the approximation in a code comment next to the helper.",
  "For PROC CROSSTAB, compute weighted counts, row/column percentages, standard errors, confidence limits, and tests using the survey design. Avoid ordinary unweighted chi-square tests; use a documented Rao-Scott or Wald-style survey approximation when an exact SUDAAN test is unavailable.",
  "For PROC CROSSTAB TEST CHISQ with OUTPUT STESTVAL SDF SPVAL in Python, export a SUDAAN-like adjusted Wald F-style statistic from the crosstab/table estimate covariance, not a raw Wald chi-square and not a logistic-regression coefficient test. If a helper computes table Wald chi-square W with test rank q, set stestval = W / q, set SDF to the nominal table df (row levels - 1) * (column levels - 1), and compute p from F(q, design_df) when design df is available.",
  "For PROC CROSSTAB output columns such as wsum, weighted count, total, or weighted_frequency, compute the value from the current table cell, current row domain, or current subgroup mask as specified by the SAS output, not from the full analytic dataset.",
  "Never assign the grand total sum of weights to every crosstab row. In nested loops, build a mask such as domain_mask & row_level_mask & column_level_mask before summing weights for a cell-level wsum, and use domain_mask & row_level_mask for a row-total wsum.",
  "If a crosstab has variables such as agecat_b by glp_med12m, the wsum values should vary by age/GLP cell or by requested row/column total; their sum may equal the overall weighted total, but each row should not equal the overall weighted total.",
  "When using pandas, prefer groupby over the exact crosstab variables with observed=False/dropna=False as appropriate and aggregate the weight column with sum, rather than computing total_weight once and copying it into every output row.",
  "For SUDAAN CHISQ, LLCHISQ, WALDCHISQ, CMH, ACMH, or association tests, produce nonblank test-result rows whenever the source requests the test and the input table has estimable dimensions; include statistic, degrees of freedom or parameter text, p-value, and a note describing the approximation.",
  "Do not output an ACMH row with blank statistic and p-value merely because exact SUDAAN ACMH is unavailable. For ordered row/column variables, compute a distinct design-based linear-by-linear score/trend Wald test with 1 numerator df using ordered row and column scores plus PSU-by-stratum Taylor covariance. Do not reuse CHISQ/STESTVAL/Wald chi-square values for ACMH.",
  "Never compute or report a raw Pearson chi-square statistic directly from survey-weighted population totals as a Rao-Scott or SUDAAN-like statistic; those values can be inflated by the sum of weights and produce million-scale statistics that are not comparable to SUDAAN or R survey output.",
  "For Rao-Scott-like tests in Python, base the test on weighted proportions plus design-based covariance, an effective sample size, or a documented design-effect adjustment, and report t/F/chi-square statistics on a scale comparable to standard survey software rather than on the weighted population-total scale.",
  "When generating both adjusted F and adjusted chi-square rows, align their df, p-value calculation, and notes with the same design-adjusted association approximation, similar in structure to R survey::svychisq adjusted F and Rao-Scott chi-square outputs.",
  "PROC DESCRIPT PAIRWISE/POLY/CONTRAST output must preserve exact row order and row count. For the NHIS-style block with PAIRWISE SEX, AGE, _RACEGR; POLY AGE=2; and four CONTRAST statements, generate 28 rows: sex pairwise, 15 age pairwise, 6 race pairwise, AGE-LINEAR, AGE-QUAD, WHITE-HISP-CONTRAST, DIF-IN-DIF-WH-HISP-SEX-DIFFERENCES, WH-HIS-MALE, WH-HISP-FEMALE. Do not merge labels onto fewer computed rows.",
  "PROC DESCRIPT POLY AGE=2 in Python: output both linear and quadratic rows. On SUDAAN's displayed PERCENT scale for six AGE levels scored 1:6, use linear coefficients [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5] and quadratic coefficients [10/3, -2/3, -8/3, -8/3, -2/3, 10/3]. Do not use the integer-rescaled [-5, -3, -1, 1, 3, 5] or [5, -1, -4, -4, -1, 5] when exporting PERCENT/SEPERCENT.",
  "For crossed PROC DESCRIPT CONTRAST statements such as SEX=(...) * _RACEGR=(...), compute PERCENT, SEPERCENT, and P_PCT over the full sex-by-race domain grid. If a full covariance matrix is not implemented in Python, use the same documented design-based or independent-domain covariance approximation consistently, but never export NaN/blank crossed contrast rows when the component domains are estimable.",
  "For PROC RLOGIST or logistic SUDAAN models, use survey weights and stratified PSU Taylor sandwich covariance, preserving class/reference levels and predicted margins. A statsmodels GLM/logit fit may supply coefficients, but do not use fit.cov_params() from cov_type='cluster' as the final SUDAAN covariance for WALDCHI/SE/CI. Compute weighted score residuals by row, sum them by PSU within strata, center PSU scores within each stratum, accumulate nh/(nh-1) * centered_score'centered_score, and sandwich with the inverse weighted logistic bread X'W*mu*(1-mu)X. Use that covariance for beta SEs, odds-ratio CIs, predicted-margin delta-method SEs, and TEST WALDCHI rows.",
  "For PROC RLOGIST TEST WALDCHI output, preserve SUDAAN's coefficient sets: OVERALL MODEL includes the intercept and all model coefficients; MODEL MINUS INTERCEPT excludes only the intercept; variable rows test the coefficient columns belonging to that CLASS/model term. Compute Wald chi-square as beta' inv(cov_beta) beta from the survey Taylor covariance, not from a non-stratified cluster covariance.",
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

function normalizeGeneratedRCode(code: string) {
  return code
    .replace(/\bsurvey\s*::\s*update\s*\(/g, "update(")
    .replace(
      /svychisq\s*\(([^)]*?),\s*statistic\s*=\s*["']Chisq["']\s*\)/g,
      'svychisq($1, statistic = "adjWald")',
    )
    .replace(
      /\b(nsum|nden|n_valid|n_total|denom|denominator)\s*<-\s*sum\s*\(\s*valid\s*\)/g,
      "$1 <- sum(valid, na.rm = TRUE)",
    )
    .replace(
      /\bif\s*\(\s*!\s*any\s*\(\s*valid\s*\)\s*\)/g,
      "if (!any(valid, na.rm = TRUE))",
    )
    .replace(
      /\bif\s*\(\s*(nsum|nden|n_valid|n_total|denom|denominator)\s*==\s*0\s*\)/g,
      "if (is.na($1) || $1 == 0)",
    );
}

function getGeneratedRValidationIssues(code: string) {
  const issues: string[] = [];
  const dataPronounPattern = /(?:dplyr::|rlang::)?\.data\s*\[\[/;

  if (
    /\b(?:domain_expr|indicator_expr)\s*=\s*(?:dplyr::|rlang::)?\.data\s*\[\[/.test(
      code,
    )
  ) {
    issues.push(
      "unsafe .data[[...]] was passed as a domain_expr/indicator_expr helper argument",
    );
  }

  if (
    /survey_mean_binary\s*\([\s\S]{0,600}(?:dplyr::|rlang::)?\.data\s*\[\[/.test(
      code,
    )
  ) {
    issues.push(
      "survey_mean_binary() call uses .data[[...]] outside a dplyr data mask",
    );
  }

  if (
    dataPronounPattern.test(code) &&
    /eval\s*\(\s*substitute\s*\(\s*(?:domain_expr|indicator_expr)\s*\)/.test(
      code,
    )
  ) {
    issues.push(
      "base eval(substitute(...)) helper is combined with .data[[...]], which causes the R data-mask error",
    );
  }

  if (/update\s*\([\s\S]{0,900}(?:dplyr::|rlang::)?\.data\s*\[\[/.test(code)) {
    issues.push(
      "survey update()/update() uses .data[[...]] outside a dplyr data mask; use design$variables[[name]] or d2$variables[[name]] instead",
    );
  }

  if (
    /(?:svydesign|model\.frame|predict|survey::subset|subset)\s*\([\s\S]{0,700}(?:dplyr::|rlang::)?\.data\s*\[\[/.test(
      code,
    )
  ) {
    issues.push(
      ".data[[...]] is used inside a non-dplyr helper call where no data mask exists",
    );
  }

  if (
    /\bpred\s*\$\s*(?:fit|se\.fit)\b/.test(code) &&
    !/is\.list\s*\(\s*pred\s*\)\s*&&\s*!\s*is\.null\s*\(\s*pred\s*\$\s*fit\s*\)/.test(
      code,
    )
  ) {
    issues.push(
      "predict() output uses pred$fit or pred$se.fit without the required is.list(pred) and null guard",
    );
  }

  if (
    /age_linear\s*<-\s*contrast_from_domains\s*\([\s\S]{0,200}c\s*\(\s*-5\s*,\s*-3\s*,\s*-1\s*,\s*1\s*,\s*3\s*,\s*5\s*\)/i.test(
      code,
    )
  ) {
    issues.push(
      "PROC DESCRIPT POLY AGE linear contrast uses integer-scaled coefficients; SUDAAN AGE 1:6 linear coefficients should be c(-2.5, -1.5, -0.5, 0.5, 1.5, 2.5)",
    );
  }

  if (
    /age_quad\s*<-\s*contrast_from_domains\s*\([\s\S]{0,200}c\s*\(\s*5\s*,\s*-1\s*,\s*-4\s*,\s*-4\s*,\s*-1\s*,\s*5\s*\)/i.test(
      code,
    )
  ) {
    issues.push(
      "PROC DESCRIPT POLY AGE quadratic contrast uses integer-scaled coefficients; SUDAAN AGE 1:6 quadratic coefficients should be c(10/3, -2/3, -8/3, -8/3, -2/3, 10/3)",
    );
  }

  if (
    /contrast_from_domains\s*<-\s*function[\s\S]*?if\s*\(\s*length\s*\(\s*domain_vars\s*\)\s*==\s*1\s*\)[\s\S]*?svyby\s*\(/.test(
      code,
    ) &&
    /contrast_from_domains\s*\([\s\S]{0,120}c\s*\(\s*["']sex["']\s*,\s*["']racegr["']\s*\)/.test(
      code,
    )
  ) {
    issues.push(
      "crossed SUDAAN CONTRAST rows are present, but contrast_from_domains only computes covariance for one-way domains; crossed contrast SEPERCENT and P_PCT would be blank",
    );
  }

  if (/\bsurvey\s*::\s*update\s*\(/.test(code)) {
    issues.push(
      "survey::update() is invalid because update is not exported from the survey namespace; use update(design, ...) instead",
    );
  }

  if (
    /\bif\s*\(\s*(nsum|nden|n_valid|n_total|denom|denominator)\s*==\s*0\s*\)/.test(
      code,
    )
  ) {
    issues.push(
      "empty-domain count guard is not NA-safe; use if (is.na(nsum) || nsum == 0) and compute counts with sum(..., na.rm = TRUE)",
    );
  }

  if (/\b(nsum|nden|n_valid|n_total|denom|denominator)\s*<-\s*sum\s*\(\s*valid\s*\)/.test(code)) {
    issues.push(
      "valid-row count uses sum(valid) without na.rm = TRUE; missing logical values can make nsum NA",
    );
  }

  if (/\bif\s*\(\s*!\s*any\s*\(\s*valid\s*\)\s*\)/.test(code)) {
    issues.push(
      "valid-row guard uses any(valid) without na.rm = TRUE; missing logical values can make the if condition NA",
    );
  }

  if (
    /regTermTest\s*\(\s*[^,\n]+,\s*(?:L|contrast_matrix|contrast|matrix)\s*\)/.test(
      code,
    )
  ) {
    issues.push(
      "survey::regTermTest() is called with a numeric contrast matrix; compute explicit Wald tests from coef(model) and vcov(model) instead",
    );
  }

  if (
    /stestval[\s\S]{0,800}svychisq\s*\([\s\S]{0,300}statistic\s*=\s*["']Chisq["']/.test(
      code,
    ) ||
    /svychisq\s*\([\s\S]{0,300}statistic\s*=\s*["']Chisq["'][\s\S]{0,800}stestval/.test(
      code,
    )
  ) {
    issues.push(
      "SUDAAN CROSSTAB STESTVAL should use a SUDAAN-like adjusted Wald F statistic, not svychisq(..., statistic = 'Chisq')",
    );
  }

  if (
    /\b(?:SE|coef)\s*\(\s*[^)\n]+\s*\)\s*\[\[\s*["'][^"']+["']\s*\]\]/.test(
      code,
    )
  ) {
    issues.push(
      "survey estimate extraction directly indexes SE(est) or coef(est) by name; store the vector, check names(), and fall back to positional extraction for one-variable estimates",
    );
  }

  if (
    /\bvcov\s*\(\s*[^)\n]+\s*\)\s*\[\s*["'][^"']+["']\s*,\s*["'][^"']+["']\s*\]/.test(
      code,
    )
  ) {
    issues.push(
      "survey covariance extraction directly indexes vcov(est) by name; store the matrix, check rownames()/colnames(), and fall back safely when names differ",
    );
  }

  if (
    /\b(?:est_se|se_est|se_vec|se_values)\s*\[\[\s*["'][^"']+["']\s*\]\]/.test(
      code,
    ) &&
    !/names\s*\(\s*(?:est_se|se_est|se_vec|se_values)\s*\)/.test(code)
  ) {
    issues.push(
      "survey SE vector is indexed by name without a names() guard; this can cause subscript out of bounds when survey::SE() returns unnamed output",
    );
  }

  if (
    /subgroup_levels\s*<-\s*list[\s\S]*\bp_poverty4_r\b/.test(code) &&
    !/p_poverty4_r\s*=\s*c\s*\([\s\S]{0,250}Below\s+100%\s+FPL/i.test(
      code,
    )
  ) {
    issues.push(
      "R SUBGROUP output includes p_poverty4_r but no poverty value-label lookup; subgroup_label would show raw codes instead of FPL labels",
    );
  }

  if (
    /subgroup_levels\s*<-\s*list[\s\S]*\bnchs_metro\b/.test(code) &&
    !/nchs_metro\s*=\s*c\s*\([\s\S]{0,180}Metropolitan[\s\S]{0,120}Nonmetropolitan/i.test(
      code,
    )
  ) {
    issues.push(
      "R SUBGROUP output includes nchs_metro but no metro value-label lookup; subgroup_label would show raw codes instead of metro labels",
    );
  }

  if (
    /subgroup_levels\s*<-\s*list[\s\S]*\bdem_region\b/.test(code) &&
    !/dem_region\s*=\s*c\s*\([\s\S]{0,220}Northeast[\s\S]{0,80}Midwest[\s\S]{0,80}South[\s\S]{0,80}West/i.test(
      code,
    )
  ) {
    issues.push(
      "R SUBGROUP output includes dem_region but no region value-label lookup; subgroup_label would show raw codes instead of region labels",
    );
  }

  return Array.from(new Set(issues));
}

function getGeneratedPythonValidationIssues(code: string) {
  const issues: string[] = [];
  const associationTestMatch = code.match(
    /def\s+design_adjusted_association_test[\s\S]*?(?=\n(?:def|class|#|\w+\s*=)|$)/,
  );
  const associationTestCode = associationTestMatch?.[0] || "";

  if (
    /age_(?:linear_)?scores\s*=\s*np\.array\s*\(\s*\[\s*-5\s*,\s*-3\s*,\s*-1\s*,\s*1\s*,\s*3\s*,\s*5\s*\]/i.test(
      code,
    )
  ) {
    issues.push(
      "PROC DESCRIPT POLY AGE linear contrast uses integer-scaled coefficients; use [-2.5, -1.5, -0.5, 0.5, 1.5, 2.5] for exported PERCENT/SEPERCENT",
    );
  }

  if (
    /age_(?:quad|quadratic)(?:_coefs|_scores)?\s*=\s*np\.array\s*\(\s*\[\s*5\s*,\s*-1\s*,\s*-4\s*,\s*-4\s*,\s*-1\s*,\s*5\s*\]/i.test(
      code,
    )
  ) {
    issues.push(
      "PROC DESCRIPT POLY AGE quadratic contrast uses integer-scaled coefficients; use [10/3, -2/3, -8/3, -8/3, -2/3, 10/3] for exported PERCENT/SEPERCENT",
    );
  }

  if (
    /AGE-QUAD/.test(code) &&
    !/(AGE-QUAD|age_quad|quadratic)[\s\S]{0,600}(10\s*\/\s*3|-8\s*\/\s*3)/i.test(
      code,
    )
  ) {
    issues.push(
      "T1_TESTS labels include AGE-QUAD, but the Python conversion does not compute a separate SUDAAN-scale quadratic POLY row",
    );
  }

  if (
    /WH(?:ITE)?-?HISP|WHITE-HISP|DIF-IN-DIF-WH-HISP/i.test(code) &&
    /COMPARE WHITE-HISPANIC[\s\S]{0,900}["']sepercent["']\s*:\s*np\.nan[\s\S]{0,200}["']p_pct["']\s*:\s*np\.nan/i.test(
      code,
    )
  ) {
    issues.push(
      "crossed WHITE-HISPANIC contrast rows are explicitly exported with NaN SEPERCENT/P_PCT instead of computed estimates",
    );
  }

  if (
    /contrast_names\s*=\s*\[[\s\S]{0,1400}AGE-QUAD[\s\S]{0,600}labels\.merge\s*\(\s*ex7_2c/i.test(
      code,
    ) &&
    /ex7_2c\s*=\s*approximate_descript_tests/i.test(code) &&
    !/AGE-QUAD[\s\S]{0,900}rows\.append/i.test(code)
  ) {
    issues.push(
      "T1_TESTS label merge may hide missing computed rows; compute all 28 EX7_2C rows before merging labels",
    );
  }

  if (
    /wald_test_from_beta[\s\S]*?["']stestval["']\s*:\s*stat\b/.test(
      associationTestCode,
    )
  ) {
    issues.push(
      "Python CROSSTAB TEST CHISQ exports raw Wald chi-square as STESTVAL; use adjusted F-style stestval = stat / test_df and nominal SDF",
    );
  }

  if (
    /sm\.(?:GLM|Logit)[\s\S]*?(?:families\.Binomial|Binomial|cov_type\s*=\s*["']cluster["'])/i.test(
      associationTestCode,
    )
  ) {
    issues.push(
      "Python CROSSTAB TEST CHISQ uses a logistic-regression Wald helper; compute the SUDAAN-like adjusted Wald statistic from the crosstab/table covariance instead",
    );
  }

  if (
    /["']stestval["']\s*:\s*test\s*\[\s*["']stat(?:istic)?["']\s*\]/i.test(code)
  ) {
    issues.push(
      "Python CROSSTAB STESTVAL appears to export an unadjusted test statistic directly; use a SUDAAN-like adjusted Wald F-style statistic",
    );
  }

  if (
    /(OVERALL MODEL|MODEL MINUS INTERCEPT|OVERALL_MODEL_TESTS|waldchi)/i.test(
      code,
    ) &&
    /\bcov\s*=\s*fit\.cov_params\s*\(\s*\)/.test(code) &&
    !/model_result\.get\s*\(\s*["']cov["']/.test(code) &&
    !/(stratified PSU Taylor|psu_scores|score_work|weighted score)/i.test(code)
  ) {
    issues.push(
      "Python RLOGIST WALDCHI uses statsmodels fit.cov_params() directly; compute a stratified PSU Taylor sandwich covariance and use it for OVERALL MODEL and MODEL MINUS INTERCEPT tests",
    );
  }

  if (
    /["']test["']\s*:\s*["']ACMH["'][\s\S]{0,700}["'](?:statistic|stestval)["']\s*:\s*(?:wald_chisq|adjusted_f|chisq_stat|chi_stat|stestval|p_value)\b/i.test(
      code,
    ) ||
    /ACMH[\s\S]{0,500}(?:reuses?|same)\s+(?:design-adjusted\s+)?association/i.test(
      code,
    )
  ) {
    issues.push(
      "Python ACMH output reuses the CHISQ/association statistic; compute a distinct 1-df design-based linear-by-linear score/trend Wald test",
    );
  }

  return Array.from(new Set(issues));
}

function assertValidGeneratedRCode(code: string) {
  const issues = getGeneratedRValidationIssues(code);
  if (issues.length === 0) {
    return;
  }

  throw new Error(
    [
      "Generated R failed validation before saving.",
      "The model emitted code that is known to fail at runtime:",
      ...issues.map((issue) => `- ${issue}`),
      "Regenerate the R conversion or refine it so dynamic columns use design$variables[[name]] or precomputed logical masks instead of .data[[...]] outside dplyr verbs.",
    ].join("\n"),
  );
}

function assertValidGeneratedPythonCode(code: string) {
  const issues = getGeneratedPythonValidationIssues(code);
  if (issues.length === 0) {
    return;
  }

  throw new Error(
    [
      "Generated Python failed validation before saving.",
      "The model emitted code that is known to produce incorrect SUDAAN-like output:",
      ...issues.map((issue) => `- ${issue}`),
      "Regenerate the Python conversion or refine it so SUDAAN tests, POLY/CONTRAST rows, SEs, and p-values are computed with distinct design-based approximations instead of placeholders.",
    ].join("\n"),
  );
}

function getErrorCauseCode(error: unknown) {
  if (
    error &&
    typeof error === "object" &&
    "cause" in error &&
    error.cause &&
    typeof error.cause === "object" &&
    "code" in error.cause
  ) {
    return String(error.cause.code);
  }
  return "";
}

function getErrorStatus(error: unknown) {
  if (error && typeof error === "object" && "status" in error) {
    const status = Number(error.status);
    if (Number.isFinite(status)) {
      return status;
    }
  }

  if (error instanceof Error) {
    const match = error.message.match(/\b([1-5][0-9]{2}) status code\b/i);
    if (match) {
      return Number(match[1]);
    }
  }

  return undefined;
}

function isTimeoutError(error: unknown) {
  return (
    error instanceof Error &&
    (error.name === "AbortError" || error.message.toLowerCase().includes("timeout"))
  );
}

function isTransientOpenAIError(error: unknown) {
  const status = getErrorStatus(error);
  return Boolean(status && status >= 500 && status < 600);
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
    const code = getErrorCauseCode(error);

    if (isTimeoutError(error)) {
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
    const code = getErrorCauseCode(error);

    if (isTimeoutError(error)) {
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
      (error.message === "OpenAI request timed out." ||
        isTransientOpenAIError(error)) &&
      fallbackModel !== primaryConfig.model
    ) {
      console.warn(
        `Retrying ${task} with fallback Azure OpenAI model ${fallbackModel} after primary model ${primaryConfig.model} failed:`,
        error,
      );
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
  const code = await generateWithOpenAIFallback(prompt, "conversion");
  assertValidGeneratedPythonCode(code);
  return code;
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
    R_SUDAAN_PROMPT,
    R_SUBGROUP_LABEL_PROMPT,
    R_FACTOR_LEVEL_PROMPT,
    R_PREDICTIVE_MARGIN_PROMPT,
    "SAS identifiers are often case-insensitive, but R references may be case-sensitive depending on the data frame and tooling.",
    "At the start of the generated code, normalize input dataframe or dataset column names to lowercase.",
    "After normalizing input columns, use lowercase variable, column, and field references consistently throughout the generated R code.",
    "When reading files into data frames, include a step such as names(df) <- tolower(names(df)) or the equivalent lowercase normalization for every relevant input dataframe.",
    R_COLUMN_NAME_PROMPT,
    R_TIDY_EVAL_PROMPT,
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
  const code = normalizeGeneratedRCode(
    await generateWithOpenAIFallback(prompt, "conversion"),
  );
  assertValidGeneratedRCode(code);
  return code;
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
    language === "R" ? R_TIDY_EVAL_PROMPT : "",
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
    language === "R" ? R_SUDAAN_PROMPT : PYTHON_SUDAAN_PROMPT,
    language === "R" ? R_SUBGROUP_LABEL_PROMPT : "",
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
  const generatedCode = await generateWithOpenAIFallback(prompt, "conversion");
  const updatedCode =
    language === "R" ? normalizeGeneratedRCode(generatedCode) : generatedCode;
  if (language === "R") {
    assertValidGeneratedRCode(updatedCode);
  } else {
    assertValidGeneratedPythonCode(updatedCode);
  }
  return updatedCode;
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
    params.language === "R" ? R_TIDY_EVAL_PROMPT : "",
    params.language === "R" ? R_CONFIDENCE_INTERVAL_PROMPT : PYTHON_CONFIDENCE_INTERVAL_PROMPT,
    params.language === "R" ? R_SUDAAN_PROMPT : PYTHON_SUDAAN_PROMPT,
    params.language === "R" ? R_SUBGROUP_LABEL_PROMPT : "",
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
