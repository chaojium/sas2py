declare module "react-syntax-highlighter" {
  import type { ComponentType, CSSProperties, ReactNode } from "react";

  export const Prism: ComponentType<{
    language?: string;
    style?: unknown;
    customStyle?: CSSProperties;
    codeTagProps?: { style?: CSSProperties };
    children?: ReactNode;
  }>;
}

declare module "react-syntax-highlighter/dist/esm/styles/prism" {
  export const oneDark: unknown;
}
