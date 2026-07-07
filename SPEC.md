# SAS2Py Studio Specification

## Goal

Build a secure web application that converts SAS programs into Python or R, preserves the analytical intent of the original SAS code, stores conversion history, supports review and refinement, and can execute generated code with captured outputs and artifacts.

## Target Users

- Analysts who maintain SAS programs and need Python or R equivalents.
- Reviewers who validate translated statistical logic and generated outputs.
- Developers or administrators who maintain prompts, execution backends, storage, and deployment configuration.

## Core User Workflows

### Authentication

1. User creates an account or signs in.
2. Application protects conversion, review, execution, history, project, and conversation routes.
3. User sees only records associated with their account.

### SAS Conversion

1. User enters a conversion name.
2. User pastes SAS code or uploads a `.sas` file.
3. User selects target language: Python or R.
4. User optionally provides additional guidance or a reference URL.
5. User chooses whether to reuse an existing matching conversion or force a fresh generation.
6. Backend sends the SAS source to the AI conversion layer.
7. Backend validates generated code before saving.
8. UI displays generated code and saves the conversion record.

### Refinement

1. User opens a saved conversion.
2. User provides an enhancement instruction.
3. Backend refines the existing generated code while preserving SAS logic.
4. Application stores enhancement history with previous code, updated code, language, instruction, and timestamp.
5. UI highlights changed lines where possible.

### Review

1. User opens a conversion.
2. User adds review comments, optional summary, and optional rating.
3. Application stores reviews and displays them with the conversion.

### Code Execution

1. User runs generated Python or R code.
2. User can upload execution input files.
3. Backend selects an execution backend:
   - Python defaults to Databricks Jobs.
   - R can use Docker or Databricks depending on configuration.
4. Runner sets `SAS2PY_INPUT_DIR` so converted code can read uploaded files without hardcoded local paths.
5. Application captures stdout, stderr, exit code, timeout status, duration, detected packages, policy mode, images, and artifacts.
6. Execution results are stored and shown in history.

### Conversation About Code

1. User asks a question about a saved conversion, error message, or desired change.
2. Backend sends the SAS source, current generated code, prior messages, and context to the AI assistant.
3. Application stores user and assistant messages in a conversation thread.
4. User can apply assistant suggestions through the refinement workflow.

### Projects and History

1. User can view recent conversions and full history.
2. User can search prior conversions.
3. User can organize conversions into projects.
4. User can delete conversions they own.

## Required Pages

- Sign-in page.
- Main converter workspace.
- Dashboard or landing workspace page.
- Conversion history page.
- Settings/profile page.
- Documentation page.

## Required API Capabilities

- `GET /api/conversions`: list user conversions and related reviews, runs, and enhancements.
- `POST /api/conversions`: generate a new Python or R conversion.
- `PATCH /api/conversions`: update generated code, assign project, or apply enhancement instruction.
- `DELETE /api/conversions`: delete a conversion owned by the user.
- `POST /api/execute`: start or run code execution.
- `GET /api/execute`: poll Databricks execution status.
- `POST /api/reviews`: save conversion review.
- `GET /api/conversations`: load conversation threads for a conversion.
- `POST /api/conversations`: ask about a conversion and persist the answer.
- `POST /api/sas-analysis`: generate validation-oriented SAS analysis.
- `GET /api/projects`, `POST /api/projects`, `PATCH /api/projects/[projectId]`, `DELETE /api/projects/[projectId]`: manage projects.
- Auth routes for sign-in, sign-up, and profile updates.

## AI Conversion Requirements

- Preserve SAS comments, headers, banners, and file metadata.
- Preserve analytical logic, not only syntax.
- Support Python and R target languages.
- Avoid hardcoded local machine paths in generated code.
- Normalize dataset column names consistently for target-language safety.
- Preserve confidence interval methods, percent/proportion scale, degrees of freedom, and weighting assumptions where possible.
- Handle SAS/SUDAAN survey procedures with survey-design-aware logic.
- Make generated code deterministic unless the SAS source explicitly uses randomness.
- Return code only for conversion/refinement calls, not Markdown explanations.
- Validate generated Python or R before saving.

## Execution Requirements

- Support Python and R execution.
- Support uploaded input files.
- Support configurable timeout and output truncation.
- Support package blocklist or allowlist policy modes.
- Capture generated artifacts such as CSV, text, JSON, Excel, PDF, PNG, HTML, Parquet, Feather, and serialized files.
- Use Azure Blob Storage for large code or input payload handoff when configured.
- Persist execution outputs with conversion history.

## Data Requirements

Store at minimum:

- Users.
- Projects.
- Code entries with SAS source, target language, generated code, guidance, reference URL, owner, timestamps.
- Reviews with reviewer, rating, summary, comments, timestamp.
- Execution runs with stdout, stderr, exit code, timeout flag, duration, packages, policy mode, artifacts, timestamp.
- Enhancement history.
- Conversation threads and messages.

## Acceptance Criteria

- Unauthenticated users cannot access protected app data.
- A signed-in user can convert SAS to Python.
- A signed-in user can convert SAS to R.
- A generated conversion is saved and appears in history.
- Reusing an identical prior conversion works unless the user forces regeneration.
- User can refine generated code and see enhancement history.
- User can save reviews.
- User can run generated code and inspect stdout/stderr.
- Execution input files are available to code through `SAS2PY_INPUT_DIR`.
- Application does not expose secrets to the browser.
- The app can be configured through `.env` / `.env.example`.

## Non-Goals

- Guarantee perfect semantic equivalence for every SAS program.
- Replace expert statistical review.
- Execute arbitrary untrusted code without policy controls.
- Store production secrets in repository files.
