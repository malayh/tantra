from conftest import SharedProvider

from sarathi.titles import _headline, generate_title


def test_headline_strips_think_blocks() -> None:
    assert _headline("<think>\nweighing options\n</think>\nPostgres API connection") == "Postgres API connection"


def test_headline_takes_the_first_non_blank_line() -> None:
    assert _headline("\n\n  Debugging production 500 errors  \nand more") == "Debugging production 500 errors"


def test_headline_strips_surrounding_quotes() -> None:
    assert _headline('"app.js failure investigation"') == "app.js failure investigation"


def test_headline_caps_at_fifty_characters() -> None:
    assert _headline("x" * 80) == "x" * 50


def test_headline_of_nothing_is_empty() -> None:
    assert _headline("<think>only thinking</think>\n\n   \n") == ""


async def test_generate_title_is_empty_when_the_provider_raises() -> None:
    provider = SharedProvider([])

    assert await generate_title(provider, "test-model", "hi") == ""
    assert provider.requests == []
