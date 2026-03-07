"use client";

import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark } from "react-syntax-highlighter/dist/esm/styles/prism";

type CodeBlockProps = {
  code: string;
  language: "python" | "sas" | "r" | "text";
  maxHeight?: number;
};

export default function CodeBlock({
  code,
  language,
  maxHeight = 320,
}: CodeBlockProps) {
  return (
    <div
      className="overflow-auto rounded-xl border border-[var(--border)] bg-[#141312]"
      style={{ maxHeight }}
    >
      <SyntaxHighlighter
        language={language}
        style={oneDark}
        customStyle={{
          margin: 0,
          background: "transparent",
          padding: "12px 14px",
          fontSize: "12px",
        }}
        codeTagProps={{
          style: {
            fontFamily: "var(--font-mono), ui-monospace, SFMono-Regular, monospace",
          },
        }}
      >
        {code}
      </SyntaxHighlighter>
    </div>
  );
}
