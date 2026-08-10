from collections.abc import AsyncIterator

import openai
from openai.lib.streaming.chat import ChatCompletionStreamState

from tantra import ProviderError, SampleRequest
from tantra.events import Usage
from tantra.providers.base import (
    ProviderEvent,
    ReasoningBlock,
    ReasoningDelta,
    StreamEnd,
    TextDelta,
    ToolCall,
    ToolCallDelta,
)
from tantra.providers.openai_compat import OpenAICompatible, _usage_payload


class ReasoningCompat(OpenAICompatible):
    async def stream(self, req: SampleRequest) -> AsyncIterator[ProviderEvent]:
        reasoning = ""
        saw_chunk = False

        state = ChatCompletionStreamState()
        try:
            chunks = await self._client.chat.completions.create(stream=True, **self.build_payload(req))
            async for chunk in chunks:
                saw_chunk = True
                state.handle_chunk(chunk)
                if not chunk.choices:
                    continue

                delta = chunk.choices[0].delta
                if delta.content:
                    yield TextDelta(text=delta.content)

                fragment = getattr(delta, "reasoning", None) or getattr(delta, "reasoning_content", None)
                if fragment:
                    reasoning += fragment
                    yield ReasoningDelta(text=fragment)

                for raw_call in delta.tool_calls or []:
                    function = raw_call.function
                    yield ToolCallDelta(
                        index=raw_call.index,
                        id=raw_call.id,
                        name=function.name if function else None,
                        args_fragment=(function.arguments if function else None) or "",
                    )

            if not saw_chunk:
                raise ProviderError(f"no SSE data frames from {self.base_url}")
            final = state.get_final_completion()
        except openai.OpenAIError as exc:
            raise ProviderError(
                str(exc),
                status_code=getattr(exc, "status_code", None),
                retryable=True if isinstance(exc, openai.APIConnectionError) else None,
            ) from exc
        except (TypeError, ValueError, AttributeError, KeyError, AssertionError) as exc:
            raise ProviderError(f"malformed stream from {self.base_url}: {exc!r}") from exc

        choice = final.choices[0] if final.choices else None
        message = choice.message if choice else None
        calls = [
            ToolCall(
                id=raw_call.id or f"call_{index}",
                name=raw_call.function.name or "",
                args=raw_call.function.arguments or "",
            )
            for index, raw_call in enumerate(message.tool_calls or [] if message else [])
        ]
        for call in calls:
            yield call

        yield StreamEnd(
            text=(message.content if message else None) or "",
            reasoning=[ReasoningBlock(text=reasoning)] if reasoning else [],
            tool_calls=calls,
            usage=_usage_payload(final.usage.model_dump()) if final.usage else Usage(),
            finish_reason=choice.finish_reason if choice else None,
        )
