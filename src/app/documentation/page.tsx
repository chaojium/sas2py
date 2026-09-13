const pdfHref = "/docs/sas2py-documentation.pdf";

const quickStart = [
  "Sign in with your internal email/password account.",
  "Open Studio and enter a required conversion name.",
  "Choose R (the default) or Python and confirm the source type.",
  "Paste SAS code or upload a `.sas` file, then attach any required data files.",
  "Choose saved-result reuse or Force regenerate, and optionally enable auto-repair.",
  "Run the conversion and review the generated code and validation output.",
  "Optionally edit, enhance, execute, review, and organize the saved entry.",
];

const manualSections = [
  {
    title: "What SAS2Py does",
    body: [
      "SAS2Py converts SAS and SAS-callable SUDAAN programs into R or Python, stores the original and generated code together, and keeps enhancement, review, and execution history with each saved conversion.",
      "It supports modernization and review, but generated statistical code still requires subject-matter review when exact SAS or SUDAAN parity is important.",
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
      "R is the default output language, and Python is also available. Source routes include Auto-detect / Not sure, SAS, SAS-callable SUDAAN, and Mixed SAS + SUDAAN.",
      "SAS-callable SUDAAN means SUDAAN procedures invoked from SAS. Mixed SAS + SUDAAN is intended for files that combine substantial SAS preparation or reporting with SUDAAN analysis.",
      "If a `.sas` file is uploaded, the file contents populate the SAS editor and, if the conversion name is still blank, the file name becomes the default entry name.",
      "The generated code is shown in the output panel and is also stored in history immediately after a successful conversion.",
    ],
  },
  {
    title: "Guidance, URLs, and saved-result reuse",
    body: [
      "Additional guidance can define methods, constraints, or expected output. For an HTTP or HTTPS reference URL, SAS2Py attempts to fetch readable page text and supplies up to 8,000 characters to the model as untrusted methodological context. The fetch has a 15-second limit; private-network URLs and unsupported non-text content are not read.",
      "Unless Force regenerate or auto-repair is selected, SAS2Py reuses the latest saved translation when the signed-in user, SAS source, output language, guidance, and reference URL all match.",
      "Force regenerate requests a fresh model response. When auto-repair is selected without Force regenerate, SAS2Py reuses the latest statically valid matching translation as its starting point, executes it, and calls the model only if repair is needed. If no valid saved translation exists, it generates a new one first.",
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
      "Apply enhancement sends the current source, generated code, and instruction to the refinement service. It can be used on an entry opened with View latest; a new conversion is not required.",
      "The app requires the enhancement to make a real code change and retries once when the first response is unchanged. An unchanged response is reported instead of being saved as a successful enhancement.",
      "The converted file can be downloaded locally as `.py` for Python output or `.R` for R output.",
    ],
  },
  {
    title: "Input files and automatic repair",
    body: [
      "Attach input data in the source section when auto-repair or execution needs it. Before auto-repair, SAS2Py checks the filenames referenced by the SAS source and asks for the correct files when an exact name is missing.",
      "Input files remain attached when switching R and Python and when View latest opens the same SAS source in the current browser session. A different source clears them. File contents are not stored in conversion history and cannot be restored after a refresh.",
      "When Run generated code and auto-repair runtime errors before saving is selected, SAS2Py applies static checks, executes the target-language code in Databricks, and makes a limited repair attempt after a failure. The latest code and execution output are saved even when further review is needed.",
      "Auto-repair does not run SAS/SUDAAN or compare against an unknown expected SAS result. The four repository examples are internal regression tests and are not included in user conversions.",
    ],
  },
  {
    title: "Executing converted code",
    body: [
      "Converted code can be executed from the Studio. Execution results include Run Output, Errors / Warnings, exit code, runtime duration, timeout status, detected packages, generated plot images, and downloadable files.",
      "Input files can be attached before execution. The Databricks runtime exposes them through the `SAS2PY_INPUT_DIR` folder, usually as a temporary directory created by the runner payload.",
      "Python and R execution both run through Databricks Jobs.",
      "When the SAS source requests one Excel workbook and no CSV files, generated R and Python code should expose one workbook with the requested sheets rather than intermediate CSV files.",
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
      "The Settings page supports profile maintenance and a persistent dark-mode preference. Users can update their display name and switch the color theme for the full application.",
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
