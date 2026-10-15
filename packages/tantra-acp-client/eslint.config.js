import js from "@eslint/js";
import tseslint from "@typescript-eslint/eslint-plugin";
import tsParser from "@typescript-eslint/parser";

export default [
  { ignores: ["dist/**", "node_modules/**"] },
  js.configs.recommended,
  {
    files: ["src/**/*.ts", "test/**/*.ts"],
    languageOptions: {
      parser: tsParser,
      globals: {
        AbortController: "readonly",
        AbortSignal: "readonly",
        clearTimeout: "readonly",
        crypto: "readonly",
        setTimeout: "readonly",
        TextEncoder: "readonly",
        WebSocket: "readonly",
      },
    },
    plugins: { "@typescript-eslint": tseslint },
    rules: {
      ...tseslint.configs.recommended.rules,
      "no-empty": ["error", { allowEmptyCatch: true }],
      "no-redeclare": "off",
    },
  },
  {
    files: ["test/**/*.mjs"],
    languageOptions: {
      globals: {
        AbortController: "readonly",
        crypto: "readonly",
        Event: "readonly",
        EventTarget: "readonly",
        fetch: "readonly",
        MessageEvent: "readonly",
        process: "readonly",
        setImmediate: "readonly",
        setTimeout: "readonly",
        URL: "readonly",
      },
    },
  },
];
