export type GeneratedCodeLanguage = "PYTHON" | "R";

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function getPythonRowLineageIssues(code: string) {
  const issues: string[] = [];

  if (
    /\b([A-Za-z_]\w*)\.loc\s*\[\s*([A-Za-z_]\w*)\.reindex\s*\(\s*\1\.index\b/.test(
      code,
    )
  ) {
    issues.push(
      "a Boolean mask is reindexed to the dataframe being filtered; recompute the mask from that dataframe or align through a preserved immutable row identifier",
    );
  }

  const reorderedFrames = new Map<string, string>();
  for (const match of code.matchAll(
    /^\s*([A-Za-z_]\w*)\s*=\s*([A-Za-z_]\w*)\.sort_values\s*\([\s\S]{0,500}?\)(?:\s*\.reset_index\s*\([^)]*\))?/gm,
  )) {
    reorderedFrames.set(match[1], match[2]);
  }

  const directMasks = new Map<string, string>();
  for (const match of code.matchAll(
    /^\s*([A-Za-z_]\w*(?:mask|condition|eligible|valid)\w*)\s*=\s*\(?\s*([A-Za-z_]\w*)\s*\[/gim,
  )) {
    directMasks.set(match[1], match[2]);
  }

  for (const [frame, source] of reorderedFrames) {
    for (const [mask, maskSource] of directMasks) {
      if (source !== maskSource) {
        continue;
      }
      const unsafeUse = new RegExp(
        `\\b${escapeRegExp(frame)}\\.loc\\s*\\[\\s*${escapeRegExp(mask)}\\b`,
      );
      if (unsafeUse.test(code)) {
        issues.push(
          `Boolean mask ${mask} is derived from ${source} but applied after rows were reordered into ${frame}; recompute it from ${frame} or join by an immutable row identifier`,
        );
      }
    }
  }

  return issues;
}

function getRRowLineageIssues(code: string) {
  const issues: string[] = [];
  const directMasks = new Map<string, string>();
  const reorderedFrames = new Map<string, string>();

  for (const match of code.matchAll(
    /^\s*([A-Za-z.][\w.]*(?:mask|condition|eligible|valid)[\w.]*)\s*<-\s*\(?\s*([A-Za-z.][\w.]*)\s*(?:\$|\[\[)/gim,
  )) {
    directMasks.set(match[1], match[2]);
  }

  for (const match of code.matchAll(
    /^\s*([A-Za-z.][\w.]*)\s*<-\s*([A-Za-z.][\w.]*)\s*(?:\|>|%>%)\s*(?:dplyr::)?arrange\s*\(/gim,
  )) {
    reorderedFrames.set(match[1], match[2]);
  }
  for (const match of code.matchAll(
    /^\s*([A-Za-z.][\w.]*)\s*<-\s*(?:dplyr::)?arrange\s*\(\s*([A-Za-z.][\w.]*)\s*,/gim,
  )) {
    reorderedFrames.set(match[1], match[2]);
  }

  for (const [frame, source] of reorderedFrames) {
    for (const [mask, maskSource] of directMasks) {
      if (source !== maskSource) {
        continue;
      }
      const unsafeUse = new RegExp(
        `\\b${escapeRegExp(frame)}\\s*\\[\\s*${escapeRegExp(mask)}\\s*,`,
      );
      if (unsafeUse.test(code)) {
        issues.push(
          `logical mask ${mask} is derived from ${source} but applied after rows were reordered into ${frame}; recompute it from ${frame} or join by an immutable row identifier`,
        );
      }
    }
  }

  const aggregateResults = Array.from(
    code.matchAll(
      /^\s*([A-Za-z.][\w.]*)\s*<-\s*(?:stats::)?aggregate\s*\(/gim,
    ),
    (match) => match[1],
  );
  for (const aggregateResult of aggregateResults) {
    const extractedColumns = new Map<string, Set<string>>();
    const extractionPattern = new RegExp(
      `^\\s*([A-Za-z.][\\w.]*)\\s*<-\\s*${escapeRegExp(
        aggregateResult,
      )}\\s*\\[\\[\\s*["']([^"']+)["']\\s*\\]\\]`,
      "gim",
    );
    for (const match of code.matchAll(extractionPattern)) {
      const targetName = match[1];
      const sourceColumn = match[2];
      const targets = extractedColumns.get(sourceColumn) || new Set<string>();
      targets.add(targetName);
      extractedColumns.set(sourceColumn, targets);
    }
    for (const [sourceColumn, targets] of extractedColumns) {
      if (targets.size > 1) {
        issues.push(
          `R aggregate result ${aggregateResult} column ${sourceColumn} is assigned to multiple variables (${Array.from(
            targets,
          ).join(
            ", ",
          )}); use distinct names for grouping values and aggregated statistics so values cannot be mistaken for weights or counts`,
        );
      }
    }
  }

  return issues;
}

export function getGeneratedRowLineageValidationIssues(
  code: string,
  language: GeneratedCodeLanguage,
) {
  const issues =
    language === "R"
      ? getRRowLineageIssues(code)
      : getPythonRowLineageIssues(code);
  return Array.from(new Set(issues));
}
