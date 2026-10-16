# Reading documents

```python
from tantra.extratools.doc import extract_docx, extract_pdf, read_doc
```

Needs `pip install "tantra-harness[doc]"` — `pypdf` and `python-docx`. Importing the module without them raises `ImportError` naming the extra.

## `read_doc()`

A factory returning a `Tool` named `read_doc` whose only model-facing parameter is `path: str`.

```python
class Researcher(Agent):
    tools = [read_doc()]
    permissions = {"read_doc": "allow"}
```

- **Local files only.** No downloads, no URLs — that is [`web_fetch`](web-fetch.md)'s job.
- **Dispatch is by extension**: `.pdf` → pypdf, `.docx` → python-docx. Anything else is an error naming the supported types.
- **Text only.** Tables lose their layout; images, charts, headers and footers are dropped. A scan or photographed page has no text layer and raises a message that says exactly that, so the model reports it instead of guessing.
- Output is capped at 64 000 characters with a `[truncated at N chars]` marker. Re-reading the same path returns the same truncated text.
- The stat, the read and the extraction all happen in one worker thread — reading a large file on the event loop stalls it measurably.

Every failure — missing path, unsupported suffix, unreadable file, corrupt document, no text layer — raises a message naming the cause and the next move. That string is all the model gets.

## The extractors are public

```python
text = extract_pdf(pdf_bytes)
text = extract_docx(docx_bytes)
```

Both take bytes and return text, raising `RuntimeError` with a self-describing message on corrupt input or an empty extraction. They are public because [`web_fetch`](web-fetch.md) calls them on fetched bytes, and they are useful directly when the document arrives from somewhere other than the filesystem — an upload, an object store, an email attachment.

## Next

- [Web fetch](web-fetch.md) — documents that live on the web.
- [Extra tools reference](../reference/extratools.md).
