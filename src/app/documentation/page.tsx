const pdfHref = "/docs/sas2py-documentation.pdf";

const quickStart = [
  "Sign in with your internal email/password account.",
  "Open Studio and enter a required conversion name.",
  "Choose Python or R as the target language.",
  "Paste SAS code or upload a `.sas` file.",
  "Run the conversion and review the generated output.",
  "Optionally edit, enhance, execute, review, and organize the saved entry.",
];

const manualSections = [
  {
    title: "What SAS2Py does",
    body: [
      "SAS2Py converts SAS programs into Python or R, stores the original and generated code together, and keeps review and execution history with each saved conversion.",
      "The current application is organized into five user-facing areas: Dashboard, Studio, History, Documentation, and Settings.",
    ],
  },
  {
    title: "Access and sign-in",
    body: [
      "Authentication is required for the main workspace, dashboard, history, project management, execution history, and profile settings.",
      "The current sign-in flow supports internal email/password accounts stored in the application database.",
    ],
  },
  {
    title: "Studio workflow",
    body: [
      "The Studio page is the main workspace. Each conversion requires a name before it can be submitted.",
      "Users can select either Python or R as the output language. The app sends SAS code to the Azure OpenAI-backed conversion service and saves the result as a new conversion entry.",
      "If a `.sas` file is uploaded, the file contents populate the SAS editor and, if the conversion name is still blank, the file name becomes the default entry name.",
      "The generated code is shown in the output panel and is also stored in history immediately after a successful conversion.",
    ],
  },
  {
    title: "SAS analysis",
    body: [
      "The Studio can generate a SAS analysis summary for the current source code.",
      "The analysis returns three items: a concise interpretation of the SAS program, the expected business or data output, and three to six validation checks a reviewer can use.",
      "This is intended to help users verify that the converted Python or R output still matches the SAS program's intent.",
    ],
  },
  {
    title: "Editing and enhancement",
    body: [
      "After conversion, users can switch into edit mode and directly modify the generated code. Saving writes the edited output back to the stored conversion entry.",
      "Users can also submit an enhancement prompt, such as performance improvements or style changes. The app refines the current conversion while preserving the SAS logic and returns an updated version of the code.",
      "The converted file can be downloaded locally as `.py` for Python output or `.R` for R output.",
    ],
  },
  {
    title: "Executing converted code",
    body: [
      "Converted code can be executed from the Studio. Execution results include stdout, stderr, exit code, runtime duration, timeout status, detected packages, and any generated plot images.",
      "Input files can be attached before execution. The runtime exposes them through the `SAS2PY_INPUT_DIR` folder. In Docker runs this path is `/workspace/input`. In Databricks runs this path is `/tmp/sas2py-input` or a temporary directory created by the runner payload.",
      "In the current implementation, Python execution defaults to Databricks when no backend is specified. R execution defaults to Docker unless the deployment is configured differently.",
      "Large uploaded execution files may require Azure Blob Storage support when Databricks is used. If the Databricks notebook parameter size limit is exceeded and Blob handoff is not configured, execution is rejected.",
      "Package policy is enforced before execution. Depending on deployment settings, the runner may block code that imports disallowed packages or packages outside an allowlist.",
    ],
  },
  {
    title: "Reviews and quality notes",
    body: [
      "Each saved conversion can receive review notes. A review requires comments and can optionally include a short summary and a rating from 1 to 5.",
      "Reviews are stored with the conversion and displayed in the Studio entry panel.",
    ],
  },
  {
    title: "History and projects",
    body: [
      "The History page shows saved conversions, groups them by project, and supports search by name, project name, SAS code, or generated code.",
      "Users can create projects with a name and optional description, move conversions into or out of projects, and delete projects.",
      "Deleting a project does not delete its conversions. The current behavior is to unassign those conversions from the project.",
      "Users can also delete individual conversions from History. That removes the conversion entry and its associated review and execution records.",
    ],
  },
  {
    title: "Dashboard and settings",
    body: [
      "The Dashboard shows summary counts for stored conversions and reviews for the signed-in user.",
      "The Settings page currently supports profile maintenance only. Users can update the display name associated with their account.",
    ],
  },
  {
    title: "User-facing validation and error cases",
    body: [
      "A conversion cannot start without both a name and SAS code.",
      "A review cannot be saved without comments, and ratings must be between 1 and 5.",
      "Execution cannot start without generated code.",
      "Only `.sas` uploads are accepted for source import in the Studio upload flow.",
      "Most workspace actions require ownership of the underlying conversion or project; unauthorized requests are rejected.",
    ],
  },
];

export default function DocumentationPage() {
  return (
    <main className="grain min-h-screen px-6 py-10 md:px-12">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-8">
        <section className="fade-up flex flex-col gap-6">
          <div className="max-w-4xl">
            <p className="text-sm font-semibold uppercase tracking-[0.3em] text-[var(--muted)]">
              Documentation
            </p>
            <h1 className="mt-4 text-4xl font-semibold leading-tight md:text-6xl">
              SAS2Py user manual
            </h1>
            <p className="mt-4 text-lg text-[var(--muted)]">
              This guide is based on the current application behavior in the
              codebase and describes the features that are available to users
              today.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <a
              href={pdfHref}
              target="_blank"
              rel="noreferrer"
              className="inline-flex w-fit items-center justify-center rounded-full bg-[var(--foreground)] px-5 py-2.5 text-sm font-semibold text-[var(--background)] transition hover:scale-[1.02]"
            >
              Open PDF documentation
            </a>
            <a
              href="/docs/sas2py-user-manual.md"
              target="_blank"
              rel="noreferrer"
              className="inline-flex w-fit items-center justify-center rounded-full border border-[var(--border)] px-5 py-2.5 text-sm font-semibold text-[var(--foreground)] transition hover:bg-white/70"
            >
              Open Markdown manual
            </a>
          </div>
        </section>

        <section className="glass-card rounded-3xl p-6 md:p-8">
          <h2 className="text-xl font-semibold">Quick start</h2>
          <ol className="mt-4 grid gap-3 text-sm leading-6 text-[var(--muted)]">
            {quickStart.map((step, index) => (
              <li key={step} className="rounded-2xl border border-[var(--border)] bg-white/70 px-4 py-3">
                <span className="font-semibold text-[var(--foreground)]">
                  {index + 1}.
                </span>{" "}
                {step}
              </li>
            ))}
          </ol>
        </section>

        <section className="grid gap-4">
          {manualSections.map((section) => (
            <article
              key={section.title}
              className="glass-card rounded-3xl p-6 md:p-8"
            >
              <h2 className="text-xl font-semibold">{section.title}</h2>
              <div className="mt-4 space-y-3 text-sm leading-6 text-[var(--muted)]">
                {section.body.map((paragraph) => (
                  <p key={paragraph}>{paragraph}</p>
                ))}
              </div>
            </article>
          ))}
        </section>
      </div>
    </main>
  );
}
