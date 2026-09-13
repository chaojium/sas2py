const DATA_FILE_EXTENSIONS =
  "xpt|sas7bdat|csv|tsv|txt|json|xlsx|xls|parquet|feather";

export type InputFileValidationResult = {
  expectedFiles: string[];
  uploadedFiles: string[];
  missingFiles: string[];
  suggestions: Array<{ expected: string; uploaded: string }>;
};

export function sanitizeInputFileName(name: string) {
  const basename = name.replace(/^.*[\\/]/, "");
  const cleaned = basename.replace(/[^A-Za-z0-9._-]+/g, "_");
  return cleaned || "input.dat";
}

function addConcreteFileName(target: Set<string>, rawName: string) {
  if (/[&%]/.test(rawName)) {
    return;
  }

  const name = sanitizeInputFileName(rawName.split(/[?#]/, 1)[0]);
  if (new RegExp(`\\.(?:${DATA_FILE_EXTENSIONS})$`, "i").test(name)) {
    target.add(name);
  }
}

export function extractExpectedInputFileNames(sasCode: string) {
  const expected = new Set<string>();
  const explicitInputPatterns = [
    new RegExp(
      `\\blibname\\s+[a-z_][\\w]*\\s+xport\\s+(["'])([^"'\\r\\n]+?\\.(?:${DATA_FILE_EXTENSIONS}))\\1`,
      "gi",
    ),
    new RegExp(
      `\\bdatafile\\s*=\\s*(["'])([^"'\\r\\n]+?\\.(?:${DATA_FILE_EXTENSIONS}))\\1`,
      "gi",
    ),
    new RegExp(
      `\\binfile\\s+(?:[a-z_][\\w]*\\s+)?(["'])([^"'\\r\\n]+?\\.(?:${DATA_FILE_EXTENSIONS}))\\1`,
      "gi",
    ),
  ];

  for (const pattern of explicitInputPatterns) {
    for (const match of sasCode.matchAll(pattern)) {
      addConcreteFileName(expected, match[2]);
    }
  }

  return [...expected];
}

function comparableStem(name: string) {
  return name
    .replace(/\.[^.]+$/, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function findSuggestion(expected: string, uploadedFiles: string[]) {
  const expectedStem = comparableStem(expected);
  const expectedExtension = expected.split(".").pop()?.toLowerCase();
  return uploadedFiles.find((uploaded) => {
    const uploadedStem = comparableStem(uploaded);
    const uploadedExtension = uploaded.split(".").pop()?.toLowerCase();
    return (
      expectedExtension === uploadedExtension &&
      (expectedStem === uploadedStem ||
        expectedStem.includes(uploadedStem) ||
        uploadedStem.includes(expectedStem))
    );
  });
}

export function validateInputFileNames(
  sasCode: string,
  uploadedNames: string[],
): InputFileValidationResult {
  const expectedFiles = extractExpectedInputFileNames(sasCode);
  const uploadedFiles = uploadedNames.map(sanitizeInputFileName);
  const uploadedSet = new Set(uploadedFiles);
  const missingFiles = expectedFiles.filter((name) => !uploadedSet.has(name));
  const suggestions = missingFiles.flatMap((expected) => {
    const uploaded = findSuggestion(expected, uploadedFiles);
    return uploaded ? [{ expected, uploaded }] : [];
  });

  return { expectedFiles, uploadedFiles, missingFiles, suggestions };
}

export function formatInputFileValidationError(
  result: InputFileValidationResult,
) {
  const lines = [
    "Input file check failed. Upload the files named by the SAS source before continuing.",
    `Expected input files: ${result.expectedFiles.join(", ") || "none detected"}`,
    `Uploaded input files: ${result.uploadedFiles.join(", ") || "none"}`,
    `Missing input files: ${result.missingFiles.join(", ")}`,
  ];

  if (result.suggestions.length) {
    lines.push(
      `Possible matches: ${result.suggestions
        .map(({ expected, uploaded }) => `${expected} -> ${uploaded}`)
        .join(", ")}`,
    );
  }

  lines.push(
    "Filenames must match exactly, including capitalization. Re-upload the correct input files and try again.",
  );
  return lines.join("\n");
}
