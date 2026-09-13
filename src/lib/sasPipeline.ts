export type SasDialectRoute = "base_sas" | "sas_survey" | "sudaan" | "mixed";
export type SourceRouteSelection = "auto" | "sas" | "sudaan" | "mixed";

export type SasProcedureBlock = {
  proc: string;
  route: Exclude<SasDialectRoute, "mixed">;
  startLine: number;
  endLine: number;
};

export type SasSourceAnalysis = {
  route: SasDialectRoute;
  confidence: number;
  signals: string[];
  selectedSourceType?: SourceRouteSelection;
  blocks: SasProcedureBlock[];
  structure: {
    libnames: string[];
    datasets: string[];
    formats: string[];
    sudaan: {
      procedures: string[];
      nest: string[];
      weight: string[];
      subpopn: string[];
      subgroup: string[];
      levels: string[];
      tables: string[];
      model: string[];
      reflevel: string[];
    };
    sasSurvey: {
      procedures: string[];
      strata: string[];
      cluster: string[];
      weight: string[];
      domain: string[];
      tables: string[];
    };
  };
};

export type ConversionReviewFinding = {
  severity: "error" | "warning" | "info";
  code: string;
  message: string;
};

export type ConversionPipelineReport = {
  sourceAnalysis: SasSourceAnalysis;
  review: {
    passed: boolean;
    findings: ConversionReviewFinding[];
  };
};

const SUDAAN_PROCS = new Set([
  "CROSSTAB",
  "DESCRIPT",
  "RLOGIST",
  "MULTILOG",
  "REGRESS",
  "SURVIVAL",
]);

const SAS_SURVEY_PROCS = new Set([
  "SURVEYFREQ",
  "SURVEYLOGISTIC",
  "SURVEYMEANS",
  "SURVEYPHREG",
  "SURVEYREG",
  "SURVEYSELECT",
]);

function stripBlockComments(source: string) {
  return source.replace(/\/\*[\s\S]*?\*\//g, (match) =>
    match.replace(/[^\r\n]/g, " "),
  );
}

function stripInlineCommentStatements(line: string) {
  return line.replace(/(^|\s)\*[^;]*;/g, " ");
}

function stripGeneratedComments(code: string, language: "PYTHON" | "R") {
  const withoutBlockComments = code.replace(/\/\*[\s\S]*?\*\//g, (match) =>
    match.replace(/[^\r\n]/g, " "),
  );

  return withoutBlockComments
    .split(/\r?\n/)
    .map((line) => {
      if (/^\s*#/.test(line)) {
        return "";
      }

      if (language === "R") {
        return line.replace(/\s+#.*$/, "");
      }

      return line.replace(/\s+#.*$/, "");
    })
    .join("\n");
}

function normalizeStatement(statement: string) {
  return statement.replace(/\s+/g, " ").trim();
}

function unique(values: string[]) {
  return Array.from(new Set(values.map((value) => value.trim()).filter(Boolean)));
}

function extractStatements(source: string) {
  const withoutBlockComments = stripBlockComments(source);
  const lines = withoutBlockComments.split(/\r?\n/);
  const statements: { text: string; startLine: number; endLine: number }[] = [];
  let current: string[] = [];
  let startLine = 1;

  lines.forEach((rawLine, index) => {
    const line = stripInlineCommentStatements(rawLine);
    if (!current.length && line.trim()) {
      startLine = index + 1;
    }
    current.push(line);

    if (line.includes(";")) {
      const joined = current.join("\n");
      const parts = joined.split(";");
      for (let i = 0; i < parts.length - 1; i += 1) {
        const text = normalizeStatement(parts[i]);
        if (text) {
          statements.push({ text, startLine, endLine: index + 1 });
        }
      }
      const remainder = parts[parts.length - 1];
      current = remainder.trim() ? [remainder] : [];
      startLine = index + 1;
    }
  });

  const trailing = normalizeStatement(current.join("\n"));
  if (trailing) {
    statements.push({ text: trailing, startLine, endLine: lines.length });
  }

  return statements;
}

function routeForProc(proc: string): Exclude<SasDialectRoute, "mixed"> {
  if (SUDAAN_PROCS.has(proc)) return "sudaan";
  if (SAS_SURVEY_PROCS.has(proc)) return "sas_survey";
  return "base_sas";
}

function collectStatementValues(statements: { text: string }[], keyword: string) {
  const pattern = new RegExp(`^${keyword}\\b\\s*(.*)$`, "i");
  return unique(
    statements
      .map((statement) => statement.text.match(pattern)?.[1] || "")
      .filter(Boolean),
  );
}

export function analyzeSasSource(sasCode: string): SasSourceAnalysis {
  const statements = extractStatements(sasCode);
  const blocks: SasProcedureBlock[] = [];
  const signals: string[] = [];

  statements.forEach((statement, index) => {
    const procMatch = statement.text.match(/^proc\s+([a-z_][\w]*)\b/i);
    if (!procMatch) return;

    const proc = procMatch[1].toUpperCase();
    const nextProc = statements
      .slice(index + 1)
      .find((candidate) => /^proc\s+([a-z_][\w]*)\b/i.test(candidate.text));
    const route = routeForProc(proc);
    blocks.push({
      proc,
      route,
      startLine: statement.startLine,
      endLine: nextProc ? Math.max(statement.endLine, nextProc.startLine - 1) : statement.endLine,
    });
    signals.push(`PROC ${proc}`);
  });

  const sudaanKeywords = [
    "NEST",
    "SUBPOPN",
    "SUBGROUP",
    "LEVELS",
    "REFLEVEL",
    "PREDMARG",
    "SETENV",
  ];
  for (const keyword of sudaanKeywords) {
    if (statements.some((statement) => new RegExp(`^${keyword}\\b`, "i").test(statement.text))) {
      signals.push(keyword);
    }
  }

  if (statements.some((statement) => /^weight\b/i.test(statement.text))) {
    signals.push("WEIGHT");
  }
  if (statements.some((statement) => /^strata\b/i.test(statement.text))) {
    signals.push("STRATA");
  }
  if (statements.some((statement) => /^cluster\b/i.test(statement.text))) {
    signals.push("CLUSTER");
  }
  if (statements.some((statement) => /^domain\b/i.test(statement.text))) {
    signals.push("DOMAIN");
  }

  const hasSudaanProc = blocks.some((block) => block.route === "sudaan");
  const hasSudaanStatement = signals.some((signal) =>
    ["NEST", "SUBPOPN", "SUBGROUP", "LEVELS", "REFLEVEL", "PREDMARG", "SETENV"].includes(signal),
  );
  const hasSurveyProc = blocks.some((block) => block.route === "sas_survey");
  const hasBaseProc = blocks.some((block) => block.route === "base_sas");

  let route: SasDialectRoute = "base_sas";
  let confidence = 0.72;

  if ((hasSudaanProc || hasSudaanStatement) && (hasBaseProc || hasSurveyProc)) {
    route = "mixed";
    confidence = hasSudaanProc ? 0.9 : 0.76;
  } else if (hasSudaanProc || hasSudaanStatement) {
    route = "sudaan";
    confidence = hasSudaanProc ? 0.92 : 0.74;
  } else if (hasSurveyProc) {
    route = "sas_survey";
    confidence = 0.88;
  }

  return {
    route,
    confidence,
    signals: unique(signals),
    blocks,
    structure: {
      libnames: collectStatementValues(statements, "libname"),
      datasets: collectStatementValues(statements, "data"),
      formats: collectStatementValues(statements, "value"),
      sudaan: {
        procedures: unique(blocks.filter((block) => block.route === "sudaan").map((block) => block.proc)),
        nest: collectStatementValues(statements, "nest"),
        weight: collectStatementValues(statements, "weight"),
        subpopn: collectStatementValues(statements, "subpopn"),
        subgroup: collectStatementValues(statements, "subgroup"),
        levels: collectStatementValues(statements, "levels"),
        tables: collectStatementValues(statements, "tables"),
        model: collectStatementValues(statements, "model"),
        reflevel: collectStatementValues(statements, "reflevel"),
      },
      sasSurvey: {
        procedures: unique(blocks.filter((block) => block.route === "sas_survey").map((block) => block.proc)),
        strata: collectStatementValues(statements, "strata"),
        cluster: collectStatementValues(statements, "cluster"),
        weight: collectStatementValues(statements, "weight"),
        domain: collectStatementValues(statements, "domain"),
        tables: collectStatementValues(statements, "tables"),
      },
    },
  };
}

export function applySourceRouteSelection(
  analysis: SasSourceAnalysis,
  selection: SourceRouteSelection = "auto",
): SasSourceAnalysis {
  if (selection === "auto") {
    return { ...analysis, selectedSourceType: selection };
  }

  const route =
    selection === "sas"
      ? analysis.route === "sas_survey"
        ? "sas_survey"
        : "base_sas"
      : selection;

  return {
    ...analysis,
    route,
    confidence: 1,
    selectedSourceType: selection,
    signals: unique([...analysis.signals, `USER_SELECTED_${selection.toUpperCase()}`]),
  };
}

export function buildRoutePromptFragment(
  analysis: SasSourceAnalysis,
  language: "PYTHON" | "R",
) {
  const target = language === "R" ? "R" : "Python";
  const lines = [
    "Conversion routing and extracted source structure:",
    `User selected source type: ${analysis.selectedSourceType || "auto"}`,
    `Used route: ${analysis.route}`,
    `Route confidence: ${analysis.confidence.toFixed(2)}`,
    `Signals: ${analysis.signals.length ? analysis.signals.join(", ") : "none"}`,
    "Use this routing decision to choose translation strategy. If the route is mixed, translate Base SAS data-prep blocks as ordinary SAS and SUDAAN/SAS survey analysis blocks with survey-design-aware logic.",
  ];

  if (
    analysis.selectedSourceType !== "sas" &&
    analysis.structure.sudaan.procedures.length
  ) {
    lines.push(
      "SUDAAN extraction:",
      `Procedures: ${analysis.structure.sudaan.procedures.join(", ")}`,
      `NEST: ${analysis.structure.sudaan.nest.join(" | ") || "none detected"}`,
      `WEIGHT: ${analysis.structure.sudaan.weight.join(" | ") || "none detected"}`,
      `SUBPOPN: ${analysis.structure.sudaan.subpopn.join(" | ") || "none detected"}`,
      `SUBGROUP: ${analysis.structure.sudaan.subgroup.join(" | ") || "none detected"}`,
      `LEVELS: ${analysis.structure.sudaan.levels.join(" | ") || "none detected"}`,
      `TABLES: ${analysis.structure.sudaan.tables.join(" | ") || "none detected"}`,
      `MODEL: ${analysis.structure.sudaan.model.join(" | ") || "none detected"}`,
      `REFLEVEL: ${analysis.structure.sudaan.reflevel.join(" | ") || "none detected"}`,
      `For ${target}, preserve SUDAAN statement semantics explicitly rather than treating them as ordinary SAS comments or generic table code.`,
    );
  }

  if (analysis.structure.sasSurvey.procedures.length) {
    lines.push(
      "SAS survey extraction:",
      `Procedures: ${analysis.structure.sasSurvey.procedures.join(", ")}`,
      `STRATA: ${analysis.structure.sasSurvey.strata.join(" | ") || "none detected"}`,
      `CLUSTER: ${analysis.structure.sasSurvey.cluster.join(" | ") || "none detected"}`,
      `WEIGHT: ${analysis.structure.sasSurvey.weight.join(" | ") || "none detected"}`,
      `DOMAIN: ${analysis.structure.sasSurvey.domain.join(" | ") || "none detected"}`,
      `TABLES: ${analysis.structure.sasSurvey.tables.join(" | ") || "none detected"}`,
    );
  }

  if (analysis.blocks.length) {
    lines.push(
      "Procedure blocks:",
      ...analysis.blocks.map(
        (block) =>
          `- lines ${block.startLine}-${block.endLine}: PROC ${block.proc} -> ${block.route}`,
      ),
    );
  }

  return lines.join("\n");
}

function hasAny(text: string, patterns: RegExp[]) {
  return patterns.some((pattern) => pattern.test(text));
}

export function reviewGeneratedConversion(params: {
  sasCode: string;
  generatedCode: string;
  language: "PYTHON" | "R";
  analysis: SasSourceAnalysis;
}): ConversionPipelineReport {
  const findings: ConversionReviewFinding[] = [];
  const source = stripBlockComments(params.sasCode);
  const generated = stripGeneratedComments(params.generatedCode, params.language);
  const hasSudaanPercentileCiWorkflow =
    /\bproc\s+univariate\b/i.test(source) &&
    /\bproc\s+descript\b/i.test(source) &&
    /\bdeffmean\b/i.test(source) &&
    /\b(?:tinv|n_act\s*=\s*nsum)\b/i.test(source);
  const sourceRequestsExcel = hasAny(source, [
    /\bdbms\s*=\s*(?:excel|xlsx|xls)\b/i,
    /\boutfile\s*=\s*["'][^"']+\.xlsx?["']/i,
    /\bods\s+excel\b/i,
  ]);
  const sourceRequestsCsv = hasAny(source, [
    /\bdbms\s*=\s*csv\b/i,
    /\boutfile\s*=\s*["'][^"']+\.csv["']/i,
  ]);
  const generatedWritesExcel =
    params.language === "PYTHON"
      ? hasAny(generated, [/\bExcelWriter\s*\(/i, /\.to_excel\s*\(/i])
      : hasAny(generated, [
          /\b(?:saveWorkbook|write\.xlsx|write_xlsx)\s*\(/i,
          /\bopenxlsx\s*::\s*(?:saveWorkbook|write\.xlsx)\s*\(/i,
          /\bwritexl\s*::\s*write_xlsx\s*\(/i,
        ]);
  const generatedWritesCsv =
    params.language === "PYTHON"
      ? /\.to_csv\s*\(/i.test(generated)
      : /\b(?:write\.csv|write_csv|fwrite)\s*\(/i.test(generated);

  if (params.analysis.route === "sudaan" || params.analysis.route === "mixed") {
    if (params.analysis.structure.sudaan.nest.length && !hasAny(generated, [/strata/i, /cluster/i, /ids\s*=/i, /svydesign/i, /psu/i])) {
      findings.push({
        severity: "error",
        code: "missing_sudaan_design",
        message: "SUDAAN NEST was detected, but the generated code does not appear to preserve strata/PSU survey design.",
      });
    }

    if (params.analysis.structure.sudaan.weight.length && !hasAny(generated, [/weight/i, /weights\s*=/i, /wt[a-z0-9_]*\b/i])) {
      findings.push({
        severity: "error",
        code: "missing_sudaan_weight",
        message: "SUDAAN WEIGHT was detected, but the generated code does not appear to use survey weights.",
      });
    }

    if (params.analysis.structure.sudaan.subpopn.length && !hasAny(generated, [/subpop/i, /domain/i, /subset\s*\(/i])) {
      findings.push({
        severity: "warning",
        code: "missing_subpopulation_logic",
        message: "SUDAAN SUBPOPN was detected; generated code should preserve domain/subpopulation analysis explicitly.",
      });
    }

    if (/proc\s+crosstab\b/i.test(source) && !hasAny(generated, [/crosstab/i, /svychisq/i, /adjWald/i, /wald/i])) {
      findings.push({
        severity: "warning",
        code: "weak_crosstab_translation",
        message: "PROC CROSSTAB was detected, but generated code has few signs of design-adjusted crosstab or test logic.",
      });
    }

    if (hasSudaanPercentileCiWorkflow) {
      if (!/\bn_act\b/i.test(generated) || !/\bnsum\b/i.test(generated)) {
        findings.push({
          severity: "error",
          code: "missing_percentile_actual_sample_size",
          message:
            "The percentile-CI source assigns N_ACT from SUDAAN NSUM, but the generated code does not preserve both quantities explicitly.",
        });
      }

      const hasInvalidActualSampleSize =
        /\bn_act\s*(?:<-|=)\s*[^\n;]*(?:(?:weight|weights|wt_orig|wt_mean)[^\n;]*\b(?:sum|total)\b|\b(?:sum|total)\s*\([^\n;]*(?:weight|weights|wt_orig|wt_mean))/i.test(
          generated,
        ) ||
        /\bn_act\s*(?:<-|=)\s*[^\n;]*\b(?:effective[_ ]?n|n_eff|neff)\b/i.test(
          generated,
        ) ||
        /\bn_act\s*(?:<-|=)\s*[^\n;]*\b(?:ind2|tail|below|above)[^\n;]*(?:sum|count|len|nrow)\b/i.test(
          generated,
        );

      if (hasInvalidActualSampleSize) {
        findings.push({
          severity: "error",
          code: "invalid_percentile_actual_sample_size",
          message:
            "N_ACT appears to be derived from weights, an effective sample size, or a percentile-tail count. It must be copied from the unweighted eligible-domain NSUM.",
        });
      }

      if (
        !hasInvalidActualSampleSize &&
        /\bn_act\b/i.test(generated) &&
        !hasAny(generated, [
          /\bn_act\s*(?:<-|=)\s*(?:as\.(?:integer|numeric)\s*\(\s*)?nsum\b/i,
          /\bn_act\s*(?:<-|=)\s*(?:int\s*\(\s*)?(?:eligible|domain)[a-z0-9_]*(?:\.sum\s*\(|\[|\)|$)/i,
          /\bn_act\s*(?:<-|=)\s*(?:nrow|len)\s*\(\s*(?:eligible|domain)/i,
        ])
      ) {
        findings.push({
          severity: "warning",
          code: "unclear_percentile_actual_sample_size_lineage",
          message:
            "N_ACT is present, but its lineage from unweighted eligible-domain NSUM is not explicit enough for static verification.",
        });
      }
    }
  }

  if (
    params.analysis.selectedSourceType === "sas" &&
    params.analysis.structure.sudaan.procedures.length
  ) {
    findings.push({
      severity: "warning",
      code: "user_selected_sas_but_sudaan_detected",
      message:
        "User selected SAS, but SUDAAN procedure signals were detected in the source.",
    });
  }

  if (params.analysis.route === "sas_survey" || params.analysis.route === "mixed") {
    if (params.analysis.structure.sasSurvey.procedures.length && !hasAny(generated, [/survey/i, /svydesign/i, /statsmodels/i, /strata/i, /cluster/i])) {
      findings.push({
        severity: "warning",
        code: "weak_survey_translation",
        message: "SAS SURVEY procedure was detected, but generated code has few signs of survey-design-aware implementation.",
      });
    }
  }

  if (sourceRequestsExcel && !generatedWritesExcel) {
    findings.push({
      severity: "error",
      code: "missing_excel_output",
      message:
        "The SAS source requests an Excel workbook, but the generated code does not appear to create one.",
    });
  }

  if (sourceRequestsExcel && !sourceRequestsCsv && generatedWritesCsv) {
    findings.push({
      severity: "error",
      code: "unexpected_csv_outputs",
      message:
        "The SAS source requests Excel output and no CSV output, but the generated code writes CSV files. Write the tables directly to workbook sheets so Python and R expose the same artifacts.",
    });
  }

  if (
    hasAny(generated, [
      new RegExp("[A-Z]:\\\\Users\\\\", "i"),
      /\/home\/[^'"\s]+/i,
    ])
  ) {
    findings.push({
      severity: "error",
      code: "hardcoded_local_path",
      message:
        "Generated executable code appears to contain a hardcoded local machine path.",
    });
  }

  if (params.language === "R" && /survey\s*::\s*update\s*\(/.test(generated)) {
    findings.push({
      severity: "error",
      code: "invalid_survey_update",
      message: "Generated R uses survey::update(), but update() should be called as the S3 generic after loading survey.",
    });
  }

  return {
    sourceAnalysis: params.analysis,
    review: {
      passed: !findings.some((finding) => finding.severity === "error"),
      findings,
    },
  };
}
