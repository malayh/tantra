# Object tool arguments arrive as JSON strings on some providers (`$ref` in tool schemas)

**Environment:** Osuite local, Tantra 2.0.0, `OpenAICompatible` → OpenRouter `z-ai/glm-5.3-flash`, no provider pinning.

## Symptom
- Osuite's `freeze_telemetry(title, source)` failed every call in chats #22 and #29 (2026-10-21). The second chat burned its 600 s turn on 13 identical retries.
- Tool result: `7 validation errors for freeze_telemetry_args … source.PromqlSource Input should be a valid dictionary`.
- Stored args: `{"title": "…", "source": "{\"type\": \"promql\", \"query\": …}"}`. `source` is a JSON string, not an object.
- Once a call is stringified, history replays it as written (`context.py:128`), so the model repeats the pattern.

## Evidence
Probes sent one `SampleRequest` through `OpenAICompatible` with a capturing `httpx` transport. Each probe carried the real tool schema and the prompt "call freeze_telemetry once with a promql source", and was pinned with `provider: {order: [slug], allow_fallbacks: false}`.

- **Tantra passes arguments through.** In every captured response (about 250), `StreamEnd.tool_calls[].args` equals the concatenated raw SSE `function.arguments`. History rebuilds args with `json.dumps(event.args)`, which is correct.
- **It depends on the host.** 24 of 35 endpoints answered:
  - `source` as a **string** on 9: OpenInference, StreamLake, Near AI, Novita, Phala, AtlasCloud, SiliconFlow, Together, Z.AI.
  - `source` as an **object** on 15: Wafer, GMICloud, Relace, Sail Research ×2, Reka, InferenceNet ×2, DigitalOcean, PrimeIntellect, Friendli, Venice, DeepInfra, Fireworks, Morph.
  - Unpinned traffic was load-balanced, so the same code worked on 2026-10-20 and failed on 2026-10-21.
- **The trigger is the schema shape.** On the failing hosts, 2 runs per variant:

  | `source` schema sent | OpenInference | StreamLake | Near AI | Novita |
  |---|---|---|---|---|
  | As Tantra sends it: `{anyOf: [{$ref: "#/$defs/…"} ×7], title}` | str 2/2 | str 2/2 | str 2/2 | str 2/2 |
  | + `"type": "object"` | object 2/2 | object 2/2 | object 2/2 | object 2/2 |
  | `$defs` inlined (no `$ref`) | object 2/2 | object 2/2 | object 2/2 | object 2/2 |
  | Both | object 2/2 | object 2/2 | object 2/2 | object 2/2 |

- **The model writes an object; the host's parser makes it a string.**
  - GLM-5.3-Flash's chat template (`zai-org/GLM-5.3-Flash`) writes calls as `<tool_call>name<arg_key>k</arg_key><arg_value>v</arg_value>…</tool_call>`. A string value is written raw and any other value as `tojson`, so a string and an object look identical in the model's text.
  - That template was rendered locally and sent to OpenRouter `/completions` (raw text, no tool parsing), pinned to Z.AI and Together. Both wrote `<arg_value>{"type": "promql", "query": "…", "start": "now-1h", "end": "now"}</arg_value>`, a correct object.
  - The same hosts in chat mode, with the same prompt and tool, returned `source` as a string: 2/2 on each.
  - Novita doesn't serve `/completions` for this model.

## Cause
- `Tool.__init__` sends `self.args_model.model_json_schema()` verbatim (`tools.py:122`); `loop.py:87` does the same for `output_schema`.
- Pydantic puts every nested model in `$defs`. A model-typed or model-union parameter then becomes a bare `$ref`, or an `anyOf` of `$ref`s, with no `type`.
- GLM writes tool arguments as key/value text, and the host's parser re-types each value from the parameter's schema. These parsers don't resolve `$ref`, so they can't tell the value is an object and keep it as a string.
- The schema is valid JSON Schema, but it isn't portable across OpenAI-compatible hosts.

## Exposure in Osuite (2.0.0)
- Object params behind `$ref`: `window` on `get_label_values`, `validate_promql` and `run_promql` (both the dashboard editor and the investigator); `source` on `freeze_telemetry`.
- Array params with `$ref` items (`filters`, `causes`, `next_steps`) declare `type: array` and decode fine.
- This has bitten Osuite before. `ToolWindow` (`backend/app/agents/tools.py:29-38`) already JSON-decodes a stringified `window`, a per-parameter workaround for this bug. `source` had none.

## Fix verified by an in-process patch
The patch wraps `Tool.__init__` and post-processes `self.schema.parameters`. It does two things:
1. Inlines every `$defs` ref, keeping `$ref` only for self-referencing models.
2. Where every non-null `anyOf` branch has the same `type`, sets that `type` on the parent.

Inlining alone fixed 8 of the 9 hosts. AtlasCloud still returned a string on 3 of 4 runs with only inlining, but returned an object on 4 of 4 once the parent `type` was set.

Results with both parts:

| Test | Original schema | Patched |
|---|---|---|
| Direct calls, the 9 stringifying hosts, 3 per host | 25 str, 1 object, 1 rate-limited | 27/27 object, all valid |
| Direct calls, the 15 working hosts, 2 per host | 29 object, 1 rate-limited | 27 object, 3 rate-limited |
| Osuite chats pinned to Z.AI, 4 per arm, same question | 32 freezes, 32 str, 0 accepted; 1 of 4 wrote no summary | 10 freezes, 0 str, 10 accepted; 4/4 summaries |

The other tool errors in the patched chats were wrong argument content, such as a missing `window.step`; every object arrived as an object.

The patch, applied before any tool is constructed:

```python
import copy


def inline(schema):
    defs = schema.get("$defs", {})
    kept = {}

    def walk(node, stack):
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                name = ref.split("/")[-1]
                if name in stack:
                    kept[name] = defs[name]
                    return node
                resolved = walk(copy.deepcopy(defs[name]), stack | {name})
                extra = {k: v for k, v in node.items() if k != "$ref"}
                return {**resolved, **walk(extra, stack)}
            return {k: walk(v, stack) for k, v in node.items() if k != "$defs"}
        if isinstance(node, list):
            return [walk(v, stack) for v in node]
        return node

    result = walk(schema, frozenset())
    if kept:
        result["$defs"] = {name: walk(body, frozenset({name})) for name, body in kept.items()}
    return hoist(result)


def hoist(node):
    if isinstance(node, list):
        return [hoist(v) for v in node]
    if not isinstance(node, dict):
        return node
    node = {k: hoist(v) for k, v in node.items()}
    branches = node.get("anyOf")
    if isinstance(branches, list) and "type" not in node:
        types = {b.get("type") for b in branches if isinstance(b, dict) and b.get("type") != "null"}
        if len(types) == 1 and None not in types:
            node["type"] = types.pop()
    return node
```

Hoisting onto `anyOf: [object, null]` makes the declared type `object`. The param is still optional because it has a default, so the model leaves it out instead of sending `null`.

## Limits of the evidence
- One model: `z-ai/glm-5.3-flash`.
- Chats were pinned to Z.AI only. The large direct-call A/B covered `freeze_telemetry`; `run_promql` was checked on 4 hosts with 8 calls per arm.
- OpenRouter sits between the host and us and translates some hosts' native APIs. Whether the conversion happens in the host's server or in OpenRouter's adapter for that host isn't separated: no host was called directly. Either way it's host-specific, and the fix covers it.
- The raw-text test used a locally rendered template, which may differ slightly from the host's version.

## Suggested fix
- Build tool and output parameter schemas self-contained: inline `$defs` refs, keeping `$ref` only for self-referencing models, and drop `$defs` when nothing references it.
- Hoist a shared `type` onto `anyOf` parents. Some hosts (AtlasCloud) read only the top-level `type`.
- Tests:
  - no `$ref` in the schema for nested, union and optional model params
  - recursive models still produce a valid schema
  - `args_model` validation is unchanged
- Not recommended as the fix: coercing JSON-string values in validation. It hides the cause and costs a turn per retry on schemas the host can't read.

**Status:** fixed in 2.0.1
