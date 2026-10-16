import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";
import starlightLinksValidator from "starlight-links-validator";

export default defineConfig({
  site: "https://malayh.github.io",
  base: "/tantra",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "tantra",
      description: "The agent turn loop as a library.",
      social: [{ icon: "github", label: "GitHub", href: "https://github.com/malayh/tantra" }],
      customCss: [
        "@fontsource-variable/geist",
        "@fontsource-variable/geist-mono",
        "./src/styles/tokens.css",
        "./src/styles/starlight.css",
      ],
      components: {
        Header: "./src/components/starlight/Header.astro",
        ThemeSelect: "./src/components/starlight/ThemeSelect.astro",
        Footer: "./src/components/starlight/Footer.astro",
      },
      plugins: [starlightLinksValidator({ exclude: ["/tantra/"], sameSitePolicy: "validate" })],
      sidebar: [
        {
          label: "Start",
          items: [
            { label: "Home", slug: "docs" },
            { label: "Install", slug: "docs/getting-started/install" },
            { label: "Quickstart", slug: "docs/getting-started/quickstart" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "The turn loop", slug: "docs/concepts/turn-loop" },
            { label: "Runtime, Connection, Agent", slug: "docs/concepts/architecture" },
            { label: "Durability & crashes", slug: "docs/concepts/durability" },
          ],
        },
        {
          label: "Guides",
          items: [
            { label: "Defining tools", slug: "docs/guides/tools" },
            { label: "Shell & ShellGuard", slug: "docs/guides/shell" },
            { label: "Web search", slug: "docs/guides/web-search" },
            { label: "Web fetch", slug: "docs/guides/web-fetch" },
            { label: "Reading documents", slug: "docs/guides/read-doc" },
            { label: "Permissions & hooks", slug: "docs/guides/permissions-hooks" },
            { label: "Skills", slug: "docs/guides/skills" },
            { label: "Memory", slug: "docs/guides/memory" },
            { label: "Actor subagents", slug: "docs/guides/subagents" },
            { label: "Storage backends", slug: "docs/guides/storage" },
            { label: "Providers", slug: "docs/guides/providers" },
            { label: "Compaction", slug: "docs/guides/compaction" },
            { label: "Telemetry", slug: "docs/guides/telemetry" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "Agent", slug: "docs/reference/agent" },
            { label: "Runtime & Connection", slug: "docs/reference/runtime" },
            { label: "ACP server", slug: "docs/reference/acp" },
            { label: "ACP profile proposal", slug: "docs/acp/tantra-profile" },
            { label: "Loop & events flow", slug: "docs/reference/loop" },
            { label: "Tools", slug: "docs/reference/tools" },
            { label: "Context", slug: "docs/reference/context" },
            { label: "Events", slug: "docs/reference/events" },
            { label: "Ask", slug: "docs/reference/ask" },
            { label: "Hooks", slug: "docs/reference/hooks" },
            { label: "Permissions", slug: "docs/reference/permissions" },
            { label: "Compaction", slug: "docs/reference/compaction" },
            { label: "Memory", slug: "docs/reference/memory" },
            { label: "Skills", slug: "docs/reference/skills" },
            { label: "Errors", slug: "docs/reference/errors" },
            { label: "Providers", slug: "docs/reference/providers" },
            { label: "Stores", slug: "docs/reference/stores" },
            { label: "Extra tools", slug: "docs/reference/extratools" },
            { label: "Telemetry", slug: "docs/reference/telemetry" },
          ],
        },
        {
          label: "Upgrading",
          items: [
            { label: "Migrate to 1.4", slug: "docs/guides/migration-1.4" },
            { label: "Migrate to 1.3", slug: "docs/guides/migration-1.3" },
            { label: "Migrate to 1.2", slug: "docs/guides/migration-1.2" },
            { label: "Migrate to 1.1", slug: "docs/guides/migration-1.1" },
            { label: "Migrate to 1.0", slug: "docs/guides/migration-1.0" },
          ],
        },
        { label: "Sharp edges", slug: "docs/sharp-edges" },
      ],
    }),
  ],
});
