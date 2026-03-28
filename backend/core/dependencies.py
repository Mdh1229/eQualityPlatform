"""
FastAPI dependency injection module for the Quality Compass backend.

This module provides reusable FastAPI dependencies for database sessions,
configuration access, and authentication. It implements the Dependency Injection
pattern as specified in Section 0.3.2 of the Agent Action Plan, enabling loose
coupling between endpoint handlers and infrastructure components.

Key Dependencies Provided:
- get_db_session: Async generator yielding database connections from the pool
- get_settings_dependency: Returns the cached Settings singleton
- SettingsDep: Type alias for injecting Settings into endpoints
- DBSessionDep: Type alias for injecting database connections into endpoints
- get_current_user_optional: Placeholder for optional authentication

Design Pattern:
This module follows the Dependency Injection pattern for loose coupling, which:
- Enables easy testing with mocks (Section 0.3.2)
- Provides consistent database session management across endpoints
- Centralizes configuration access for all endpoint handlers
- Allows future authentication integration without endpoint changes

Reference Implementation:
- Pattern source: lib/db.ts (Prisma singleton for Node.js)
- Target: Python async dependencies for FastAPI

Usage Examples:
    # Using type alias annotations (recommended)
    @router.get("/runs")
    async def list_runs(
        db: DBSessionDep,
        settings: SettingsDep
    ) -> List[RunResponse]:
        # db is an asyncpg.Connection from the pool
        # settings is the cached Settings instance
        rows = await db.fetch("SELECT * FROM analysis_run LIMIT 10")
        return [RunResponse.from_record(row) for row in rows]
    
    # Using Depends directly
    @router.post("/actions")
    async def create_action(
        db: Annotated[asyncpg.Connection, Depends(get_db_session)]
    ):
        pass

Dependencies:
    - asyncpg==0.30.0 (Section 0.5.1 Backend Dependencies)
    - fastapi==0.115.6 (Section 0.5.1 Backend Dependencies)
    - backend.core.config: Settings class and get_settings function
    - backend.core.database: get_db_pool for connection pool access

See Also:
    - backend/core/config.py: Settings management and environment variables
    - backend/core/database.py: Connection pool lifecycle management
    - backend/api/*.py: Endpoint handlers using these dependencies
"""

from typing import AsyncGenerator, Annotated, Optional

from fastapi import Depends
from asyncpg import Connection

from backend.core.config import Settings, get_settings
from backend.core.database import get_db_pool


# =============================================================================
# Database Session Dependency
# =============================================================================

async def get_db_session() -> AsyncGenerator[Connection, None]:
    """
    Yield an async database connection from the pool.
    
    This dependency acquires a connection from the asyncpg connection pool
    and yields it for use in the endpoint handler. The connection is
    automatically released back to the pool when the endpoint completes,
    regardless of whether the operation succeeded or raised an exception.
    
    The connection supports all standard asyncpg operations:
    - fetch(): Execute query and return all rows
    - fetchrow(): Execute query and return single row
    - fetchval(): Execute query and return single value
    - execute(): Execute command (INSERT/UPDATE/DELETE)
    - executemany(): Batch command execution
    - transaction(): Transaction context manager
    
    Yields:
        asyncpg.Connection: An active database connection from the pool.
    
    Raises:
        asyncpg.PostgresError: If connection acquisition fails.
        asyncpg.PoolAcquireTimeoutError: If no connection available within timeout.
    
    Example:
        @router.get("/items/{item_id}")
        async def get_item(
            item_id: str,
            db: DBSessionDep
        ) -> ItemResponse:
            row = await db.fetchrow(
                "SELECT * FROM items WHERE id = $1",
                item_id
            )
            if not row:
                raise HTTPException(status_code=404, detail="Item not found")
            return ItemResponse.from_record(row)
    
    Note:
        The connection is acquired using `async with pool.acquire()` which
        ensures proper cleanup even if an exception occurs. Do not manually
        close or release the connection - it's handled automatically.
    """
    pool = await get_db_pool()
    async with pool.acquire() as connection:
        yield connection


# =============================================================================
# Settings Dependency
# =============================================================================

def get_settings_dependency() -> Settings:
    """
    Return the Settings singleton instance.
    
    This dependency provides access to the application's configuration,
    implementing the same singleton pattern as lib/db.ts but adapted for
    Python using the @lru_cache decorator in the underlying get_settings()
    function.
    
    The Settings instance contains all configuration values loaded from
    environment variables, including:
    - Database connection (database_url)
    - External service credentials (BigQuery, Google Drive, Slack, Abacus AI)
    - Platform configuration defaults (min_calls_window, metric_presence_threshold, etc.)
    
    Returns:
        Settings: The cached Settings instance with all configuration values.
    
    Raises:
        pydantic.ValidationError: If called before environment is configured
            properly (missing required variables like DATABASE_URL).
    
    Example:
        @router.get("/config/thresholds")
        async def get_thresholds(
            settings: SettingsDep
        ) -> ThresholdResponse:
            return ThresholdResponse(
                min_calls=settings.min_calls_window,
                min_leads=settings.min_leads_window,
                metric_presence=settings.metric_presence_threshold,
                warning_days=settings.warning_window_days
            )
    
    Note:
        This is a thin wrapper around get_settings() to enable FastAPI's
        dependency override mechanism for testing. In tests, you can do:
        
        app.dependency_overrides[get_settings_dependency] = lambda: mock_settings
    """
    return get_settings()


# =============================================================================
# Type Aliases for Dependency Injection
# =============================================================================

# Type alias for injecting Settings into endpoint handlers
# Usage: async def endpoint(settings: SettingsDep)
SettingsDep = Annotated[Settings, Depends(get_settings_dependency)]

# Type alias for injecting database connections into endpoint handlers
# Usage: async def endpoint(db: DBSessionDep)
DBSessionDep = Annotated[Connection, Depends(get_db_session)]


# =============================================================================
# Authentication Dependencies (Placeholder)
# =============================================================================

async def get_current_user_optional() -> Optional[dict]:
    """
    Placeholder for optional authentication - returns None.
    
    Per Section 0.2.1 and 0.6.2 of the Agent Action Plan, authentication
    changes are explicitly out of scope for this refactor. This placeholder
    dependency is provided for future expansion without breaking existing
    endpoint signatures.
    
    Current behavior: Always returns None, indicating no authenticated user.
    This is intentional as the Quality Compass is an internal analytics tool
    that currently does not require user authentication.
    
    Future expansion: When authentication is implemented, this function can
    be updated to validate tokens, check sessions, or integrate with an
    identity provider without changing endpoint signatures that use it.
    
    Returns:
        None: Always returns None (no authentication required per specification).
    
    Example (future use):
        @router.post("/actions")
        async def log_action(
            action: ActionRequest,
            current_user: Annotated[Optional[dict], Depends(get_current_user_optional)]
        ) -> ActionResponse:
            # current_user is None in current implementation
            # In future, could contain user info like {'id': 'user123', 'name': 'John'}
            taken_by = current_user['name'] if current_user else 'system'
            ...
    
    Note:
        The 'system only recommends; humans confirm via Log Action' requirement
        (Section 0.8.1) does not require authentication - it requires human
        confirmation in the UI workflow, which is handled by the Log Action
        modal component.
    """
    return None


# =============================================================================
# Optional: Current User Type Alias (for future use)
# =============================================================================

# Type alias for optional user injection - returns None in current implementation
# This is provided for forward compatibility when auth is implemented
CurrentUserOptionalDep = Annotated[Optional[dict], Depends(get_current_user_optional)]
