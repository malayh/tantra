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
        clearTimeout: "readonly",
        CloseEvent: "readonly",
        crypto: "readonly",
        Event: "readonly",
        MessageEvent: "readonly",
        performance: "readonly",
        setTimeout: "readonly",
        TextEncoder: "readonly",
        WebSocket: "readonly",
      },
    },
    plugins: { "@typescript-eslint": tseslint },
    rules: {
      ...tseslint.configs.recommended.rules,
      "@typescript-eslint/no-unused-vars": [
        "error",
        { varsIgnorePattern: "^I$" },
      ],
      "no-empty": ["error", { allowEmptyCatch: true }],
      "no-redeclare": "off",
    },
  },
  {
    files: ["test/**/*.mjs"],
    languageOptions: {
      globals: {
        Buffer: "readonly",
        crypto: "readonly",
        Event: "readonly",
        EventTarget: "readonly",
        fetch: "readonly",
        MessageEvent: "readonly",
        process: "readonly",
        queueMicrotask: "readonly",
        setTimeout: "readonly",
        structuredClone: "readonly",
        URL: "readonly",
      },
    },
  },
];
