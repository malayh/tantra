import re
from contextlib import aclosing

from tantra.providers.base import Provider, SampleRequest, StreamEnd, SystemBlock, UserMessage

TITLE_WIDTH = 50
THINK = re.compile(r"<think>.*?</think>", re.DOTALL)

TITLE_PROMPT = """You are a title generator. You output only a thread title and nothing else.

Write a title that will help the user find this conversation again later.

Rules:
- One line, at most 50 characters.
- Same language as the user's message.
- Grammatical and natural to read, not a pile of keywords.
- Keep technical terms, numbers, filenames and status codes exactly as the user wrote them.
- Drop articles and possessives: no "the", "a", "an", "this", "my".
- Never name a tool. Never assume a language or framework the user did not mention.
- Never answer the message, question it, refuse it or comment on it. Title it.
- Always produce something, however thin the input: a greeting becomes "Greeting", a bare ping becomes
  "Quick check-in".

Examples:
- "debug 500 errors in production" -> Debugging production 500 errors
- "why is app.js failing" -> app.js failure investigation
- "how do I connect postgres to my API" -> Postgres API connection"""


def _headline(text: str) -> str:
    for line in THINK.sub("", text).splitlines():
        stripped = line.strip().strip("\"'")
        if stripped:
            return stripped[:TITLE_WIDTH]
    return ""


async def generate_title(provider: Provider, model: str, text: str) -> str:
    try:
        request = SampleRequest(
            model=model,
            system=[SystemBlock(text=TITLE_PROMPT)],
            messages=[UserMessage(content=f"Generate a title for this conversation:\n{text}")],
        )
        answer = ""
        async with aclosing(provider.stream(request)) as events:
            async for event in events:
                if isinstance(event, StreamEnd):
                    answer = event.text
        return _headline(answer)
    except Exception:
        return ""
