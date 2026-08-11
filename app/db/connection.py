from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings

# Built on first use rather than at import. Creating the engine at import time
# froze it to whatever DATABASE_URL happened to be set then, so nothing could
# point the application at a different database afterwards — importing anything
# from this package was enough to lock it in.
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(get_settings().database_url, pool_pre_ping=True)
    return _engine


def async_session_factory() -> AsyncSession:
    """Return a new session. Callable exactly like the sessionmaker it replaces."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(get_engine(), expire_on_commit=False)
    return _session_factory()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        yield session


async def dispose_engine() -> None:
    """Close all pooled connections. Call on app shutdown to avoid leaking
    connections when the process exits (e.g. during a reload or redeploy).

    Also clears the cached engine, so a later call rebuilds it from current
    settings instead of handing back one that has already been disposed.
    """
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
