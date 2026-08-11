"""The engine is built on first use, not at import.

Before this, `create_async_engine` ran at import time, so the database URL was
frozen by whatever environment existed when the first module happened to import
`app.db.connection` — nothing could redirect it afterwards. These tests pin the
lazy behaviour so it can't silently regress.
"""

import app.db.connection as connection
from app.config import get_settings


def _reset_settings_and_engine():
    get_settings.cache_clear()
    connection._engine = None
    connection._session_factory = None


def test_engine_url_follows_configuration_set_after_import(monkeypatch):
    _reset_settings_and_engine()
    try:
        monkeypatch.setenv("POSTGRES_HOST", "somewhere-else")
        monkeypatch.setenv("POSTGRES_PORT", "6543")

        engine = connection.get_engine()

        assert engine.url.host == "somewhere-else"
        assert engine.url.port == 6543
    finally:
        monkeypatch.undo()
        _reset_settings_and_engine()


def test_engine_is_cached_between_calls():
    _reset_settings_and_engine()
    try:
        assert connection.get_engine() is connection.get_engine()
    finally:
        _reset_settings_and_engine()


async def test_dispose_clears_the_cached_engine():
    """A disposed engine must not be handed out again on the next call."""
    _reset_settings_and_engine()
    try:
        first = connection.get_engine()
        await connection.dispose_engine()

        assert connection._engine is None
        assert connection.get_engine() is not first
    finally:
        _reset_settings_and_engine()
