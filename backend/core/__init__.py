"""
Core infrastructure package for the FastAPI backend.

Provides:
- Configuration management via pydantic-settings
- Async PostgreSQL database connectivity via asyncpg
- FastAPI dependency injection utilities

This module re-exports key components from submodules for convenient importing
by other modules throughout the backend. This allows simplified imports like:

    from backend.core import get_settings, get_db_pool, DBSessionDep

Instead of:

    from backend.core.config import get_settings
    from backend.core.database import get_db_pool
    from backend.core.dependencies import DBSessionDep

Design Pattern References (Section 0.3.2):
- Repository Pattern for Data Access via database module
- Dependency Injection for Loose Coupling via dependencies module
- Singleton Pattern for Configuration via config module

Components Re-exported:
    Settings: Pydantic settings class with all configuration parameters
    get_settings: Function returning the cached Settings singleton
    init_db: Async function to initialize the database connection pool
    close_db: Async function to close the database connection pool
    get_db_pool: Async function to get the database connection pool
    get_db_session: FastAPI dependency yielding database connections
    get_settings_dependency: FastAPI dependency returning Settings
    SettingsDep: Type alias for Settings dependency injection
    DBSessionDep: Type alias for database session dependency injection

Usage Examples:
    # Configuration access
    from backend.core import get_settings
    settings = get_settings()
    print(settings.database_url)
    print(settings.min_calls_window)

    # Database pool lifecycle (in FastAPI lifespan)
    from backend.core import init_db, close_db
    
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await init_db()
        yield
        await close_db()

    # FastAPI endpoint with dependencies
    from backend.core import DBSessionDep, SettingsDep
    
    @router.get("/runs")
    async def list_runs(
        db: DBSessionDep,
        settings: SettingsDep
    ) -> List[RunResponse]:
        rows = await db.fetch("SELECT * FROM analysis_run LIMIT 10")
        return [RunResponse.from_record(row) for row in rows]

Reference Implementation:
    - Source pattern: lib/db.ts (Prisma singleton for Node.js)
    - Target: Python async equivalent with proper package structure
"""

# =============================================================================
# Re-exports from backend.core.config
# =============================================================================
from backend.core.config import Settings, get_settings

# =============================================================================
# Re-exports from backend.core.database
# =============================================================================
from backend.core.database import init_db, close_db, get_db_pool

# =============================================================================
# Re-exports from backend.core.dependencies
# =============================================================================
from backend.core.dependencies import (
    get_db_session,
    get_settings_dependency,
    SettingsDep,
    DBSessionDep,
)

# =============================================================================
# Public API Definition
# =============================================================================
# The __all__ list defines the public interface of this package.
# Only these symbols are exported when using "from backend.core import *"
# This matches the exports schema requirements for this file.

__all__ = [
    # Configuration management (from config.py)
    'Settings',
    'get_settings',
    # Database pool lifecycle (from database.py)
    'init_db',
    'close_db',
    'get_db_pool',
    # FastAPI dependency injection (from dependencies.py)
    'get_db_session',
    'get_settings_dependency',
    'SettingsDep',
    'DBSessionDep',
]
