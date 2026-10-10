# Compaction fails when reasoning consumes the summary output budget

**Environment:** Osuite `osuite-backend`, `prod-us`, Tantra 1.4.0, OpenRouter `z-ai/glm-5.3-flash`.

## Evidence

- Trace `6c2ef7a6daf669349d379b1b3ad85afe`, conversation `f0cbd971b7784f3db7b9d8ba55b9ed8a`.
- Turn started at 2026-10-04 14:33:28.585610 UTC and failed after 167.277 seconds; this failure precedes the turn deadline.
- `compact` started at 14:35:23.041770 UTC, ran for 52.781 seconds and raised `ProviderError`: “Could not parse response content as the length limit was reached”.
- Reported usage: 50,594 prompt tokens, 4,096 completion tokens, 4,098 reasoning tokens. The slightly inconsistent reasoning/completion counts are provider-reported. Reasoning consumed approximately the entire output allowance.
- Evidence extracted with `osuite traces search-span` and `osuite traces getbyid`; only the diagnostic fields above are recorded in this report.

## Cause

`CompactionConfig.summary_max_output` defaults to 4,096. `PruneThenSummarize._brief()` independently sends this as `max_tokens`, using the current reasoning model. Osuite's `ModelLimits(max_output=8192)` does not change this summary setting.

`OpenAICompatible.stream()` uses `ChatCompletionStreamState.get_final_completion()`. The OpenAI SDK rejects `finish_reason="length"` even for this free-text summary. The provider wraps that exception as `ProviderError`, which propagates through compaction and ends the turn. This message does not establish that dashboard JSON was malformed or that the context window was exceeded.

## Follow-up

- Osuite can configure a larger bounded summary output budget through `CompactionConfig`; this improves headroom but does not guarantee completion for a reasoning model.
- Tantra should distinguish output exhaustion from context overflow and expose an actionable compaction-specific failure. Consider a bounded retry with more output headroom or supported reasoning controls, preserving the original prefix until a complete usable summary succeeds.
- Do not silently accept a truncated or empty summary or mutate journals on a failed summary.
- Add a deterministic provider regression covering summary output exhaustion, unchanged history on failure, bounded recovery and successful subsequent compaction. Real-model evidence above is observational, not a local reproduction.
