# Extra tools

| Module | Tools | Extra |
|---|---|---|
| `tantra.extratools.shell` | `bash`, `ShellGuard` | core |
| `tantra.extratools.web` | `web_search`, `web_fetch` | `tantra-harness[web]` |
| `tantra.extratools.doc` | `read_doc` | `tantra-harness[doc]` |

Tool factories receive application configuration explicitly. Pass guards through `Runtime(hooks=[...])`.
