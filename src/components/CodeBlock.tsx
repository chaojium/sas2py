"use client";

import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

const Highlighter = SyntaxHighlighter as typeof SyntaxHighlighter & ((props: {
  language: string;
  style: unknown;
  children: string;
  customStyle?: React.CSSProperties;
  codeTagProps?: { style?: React.CSSProperties };
  showLineNumbers?: boolean;
  lineNumberStyle?: React.CSSProperties;
  lineProps?: (lineNumber: number) => React.HTMLAttributes<HTMLElement>;
}) => React.ReactElement);

type CodeBlockProps = {
  code: string;
  language: "python" | "sas" | "r" | "text";
  maxHeight?: number;
  wrapLongLines?: boolean;
  showLineNumbers?: boolean;
  highlightedLines?: number[];
};

export default function CodeBlock({
  code,
  language,
  maxHeight = 320,
  wrapLongLines = false,
  showLineNumbers = false,
  highlightedLines = [],
}: CodeBlockProps) {
  const highlightedLineSet = new Set(highlightedLines);

  return (
    <div
      className={`w-full min-w-0 rounded-xl border border-[var(--border)] bg-[#141312] ${
        wrapLongLines ? "overflow-x-hidden overflow-y-auto" : "overflow-auto"
      }`}
      style={{ maxHeight }}
    >
      <Highlighter
        language={language}
        style={oneDark}
        showLineNumbers={showLineNumbers}
        lineNumberStyle={{
          minWidth: "2.5em",
          paddingRight: "1em",
          color: "rgba(255,255,255,0.38)",
          userSelect: "none",
        }}
        lineProps={(lineNumber) =>
          highlightedLineSet.has(lineNumber)
            ? {
                style: {
                  display: "block",
                  background: "rgba(250, 204, 21, 0.14)",
                  boxShadow: "inset 3px 0 0 rgba(250, 204, 21, 0.75)",
                },
              }
            : { style: { display: "block" } }
        }
        customStyle={{
          margin: 0,
          background: "transparent",
          padding: "12px 14px",
          fontSize: "12px",
          whiteSpace: wrapLongLines ? "pre-wrap" : "pre",
          overflowWrap: wrapLongLines ? "anywhere" : "normal",
          wordBreak: wrapLongLines ? "break-word" : "normal",
        }}
        codeTagProps={{
          style: {
            fontFamily: "var(--font-mono), ui-monospace, SFMono-Regular, monospace",
            whiteSpace: wrapLongLines ? "pre-wrap" : "pre",
            overflowWrap: wrapLongLines ? "anywhere" : "normal",
            wordBreak: wrapLongLines ? "break-word" : "normal",
          },
        }}
      >
        {code}
      </Highlighter>
    </div>
  );
}
