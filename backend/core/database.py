"""
Async PostgreSQL connection pool module for Supabase database connectivity.

This module provides an async PostgreSQL connection pool using asyncpg, implementing
a singleton pattern similar to lib/db.ts but adapted for Python's async context.
It serves as the core data access layer for the Quality Compass FastAPI backend.

Key Components:
- Global connection pool singleton (_pool)
- init_db(): Initialize the connection pool at application startup
- get_db_pool(): Get the pool instance (initializes if needed)
- close_db(): Gracefully close the pool at application shutdown
- execute_query(): Convenience helper for executing raw SQL queries

Design Pattern:
This module implements the Repository Pattern for Data Access as specified in
Section 0.3.2 of the Agent Action Plan. All PostgreSQL connections flow through
this module, providing a single point of configuration and management.

Connection Pool Configuration:
- min_size: 2 (minimum idle connections kept in pool)
- max_size: 10 (maximum connections in pool)
- command_timeout: 60 seconds (query timeout)

Reference Implementation:
- Source pattern: lib/db.ts (Prisma singleton for Node.js)
- Target: Python async equivalent using asyncpg

Usage:
    # At application startup (in FastAPI lifespan)
    await init_db()
    
    # In services or endpoints
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        result = await conn.fetch("SELECT * FROM users")
    
    # Or use the convenience helper
    results = await execute_query("SELECT * FROM users WHERE id = $1", user_id)
    
    # At application shutdown
    await close_db()

Environment Variables:
    DATABASE_URL: Supabase PostgreSQL connection string (Required)
        Format: postgresql://user:password@host:port/database
        Reference: Section 0.9.7 Environment Variables Required

Dependencies:
    - asyncpg==0.30.0 (Section 0.5.1 Backend Dependencies)
    - backend.core.config.get_settings (for DATABASE_URL access)

See Also:
    - backend/core/config.py: Settings management with DATABASE_URL
    - backend/core/dependencies.py: FastAPI dependency injection wrappers
"""

import asyncpg
from asyncpg import Pool
from typing import Optional, List, Any

from backend.core.config import get_settings


# =============================================================================
# Global Pool Singleton
# =============================================================================

# Global connection pool instance - None until init_db() is called
# This implements the singleton pattern similar to lib/db.ts globalForPrisma
# The pool is shared across all async tasks for efficient connection reuse
_pool: Optional[Pool] = None


# =============================================================================
# Pool Lifecycle Functions
# =============================================================================

async def init_db() -> Pool:
    """
    Initialize the database connection pool.
    
    This function creates an asyncpg connection pool configured for the Supabase
    PostgreSQL database. It should be called once at application startup, typically
    in the FastAPI lifespan context manager.
    
    The pool is configured with:
    - min_size=2: Keep at least 2 idle connections ready
    - max_size=10: Allow up to 10 concurrent connections
    - command_timeout=60: Queries timeout after 60 seconds
    
    If the pool is already initialized, this function returns the existing pool
    without creating a new one (idempotent behavior).
    
    Returns:
        Pool: The asyncpg connection pool instance.
    
    Raises:
        asyncpg.PostgresError: If connection to the database fails.
        asyncpg.InvalidCatalogNameError: If the database doesn't exist.
        asyncpg.InvalidPasswordError: If authentication fails.
        OSError: If the database host is unreachable.
    
    Example:
        # In FastAPI lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await init_db()
            yield
            await close_db()
    
    Note:
        The DATABASE_URL is loaded from environment variables via get_settings().
        Ensure the environment is properly configured before calling init_db().
    """
    global _pool
    
    if _pool is None:
        # Load settings to get the database connection string
        settings = get_settings()
        
        # Create the asyncpg connection pool with production-ready settings
        # - min_size: Keeps connections warm for quick acquisition
        # - max_size: Limits concurrent connections to prevent DB overload
        # - command_timeout: Prevents runaway queries from blocking
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=2,
            max_size=10,
            command_timeout=60,
        )
    
    return _pool


async def get_db_pool() -> Pool:
    """
    Get the database connection pool, initializing if needed.
    
    This is the primary accessor for obtaining the connection pool. It implements
    lazy initialization - if the pool hasn't been created yet, it will be
    initialized automatically. This allows services to be written without
    worrying about initialization order.
    
    For optimal performance, prefer calling init_db() explicitly at application
    startup rather than relying on lazy initialization, as the first lazy
    init will add latency to that request.
    
    Returns:
        Pool: The asyncpg connection pool instance.
    
    Raises:
        asyncpg.PostgresError: If connection to the database fails during lazy init.
    
    Example:
        # In a service or endpoint
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM analysis_run LIMIT 10")
            for row in rows:
                print(row['id'], row['name'])
    
    Note:
        The returned pool should not be closed manually. Use close_db() at
        application shutdown instead.
    """
    global _pool
    
    if _pool is None:
        await init_db()
    
    # At this point _pool is guaranteed to be non-None due to init_db()
    # We use an assertion to help type checkers understand this
    assert _pool is not None, "Pool should be initialized after init_db()"
    
    return _pool


async def close_db() -> None:
    """
    Close the database connection pool gracefully.
    
    This function should be called at application shutdown to properly close
    all database connections. It waits for active queries to complete before
    closing connections, ensuring no data corruption or connection leaks.
    
    After calling close_db(), the pool will be reset to None. Subsequent calls
    to get_db_pool() will create a new pool (useful for testing scenarios).
    
    This function is idempotent - calling it multiple times or when the pool
    is not initialized has no effect.
    
    Example:
        # In FastAPI lifespan
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await init_db()
            yield
            await close_db()
    
    Note:
        This function gracefully closes all connections. If you need to force
        close immediately (not recommended), consider using pool.terminate().
    """
    global _pool
    
    if _pool is not None:
        # Close the pool gracefully - this waits for active connections
        # to be released before closing
        await _pool.close()
        _pool = None


# =============================================================================
# Query Execution Helper
# =============================================================================

async def execute_query(query: str, *args: Any) -> List[asyncpg.Record]:
    """
    Execute a single query and return results.
    
    This is a convenience helper for services that need to execute simple
    queries without managing connection acquisition/release explicitly.
    The function handles pool acquisition, query execution, and connection
    release automatically.
    
    For complex operations requiring multiple queries in a transaction,
    prefer acquiring a connection from the pool directly:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(...)
                await conn.execute(...)
    
    Args:
        query: SQL query string with optional $1, $2, etc. parameter placeholders.
        *args: Query parameters corresponding to placeholders in the query.
            Parameters are passed positionally and must match the order of
            placeholders in the query.
    
    Returns:
        List[asyncpg.Record]: List of records returned by the query. Each Record
            acts like a dict and supports both key access (row['column']) and
            index access (row[0]).
    
    Raises:
        asyncpg.PostgresError: If the query execution fails.
        asyncpg.PostgresSyntaxError: If the SQL syntax is invalid.
        asyncpg.UndefinedTableError: If a referenced table doesn't exist.
    
    Examples:
        # Simple select
        users = await execute_query("SELECT * FROM users WHERE active = $1", True)
        
        # With multiple parameters
        results = await execute_query(
            "SELECT * FROM classification_result WHERE run_id = $1 AND vertical = $2",
            run_id,
            "Medicare"
        )
        
        # Accessing results
        for row in results:
            print(f"SubID: {row['subid']}, Tier: {row['recommended_tier']}")
    
    Note:
        For INSERT/UPDATE/DELETE operations that don't return rows, you may
        want to use execute() instead of fetch() on the connection directly.
        This helper always uses fetch() which expects rows to be returned.
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def execute_query_one(query: str, *args: Any) -> Optional[asyncpg.Record]:
    """
    Execute a query and return a single row or None.
    
    This is a convenience helper for queries expected to return at most one row,
    such as lookups by primary key. It wraps asyncpg's fetchrow() method which
    returns None if no row matches.
    
    Args:
        query: SQL query string with optional $1, $2, etc. parameter placeholders.
        *args: Query parameters corresponding to placeholders in the query.
    
    Returns:
        Optional[asyncpg.Record]: The first row matching the query, or None if
            no rows match.
    
    Raises:
        asyncpg.PostgresError: If the query execution fails.
    
    Example:
        # Lookup by ID
        run = await execute_query_one(
            "SELECT * FROM analysis_run WHERE id = $1",
            run_id
        )
        if run:
            print(f"Found run: {run['name']}")
        else:
            print("Run not found")
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def execute_command(query: str, *args: Any) -> str:
    """
    Execute a command (INSERT/UPDATE/DELETE) and return the status string.
    
    This is a convenience helper for data modification operations that don't
    return rows. It wraps asyncpg's execute() method which returns a status
    string like 'INSERT 0 1' or 'UPDATE 5'.
    
    Args:
        query: SQL command string with optional $1, $2, etc. parameter placeholders.
        *args: Command parameters corresponding to placeholders.
    
    Returns:
        str: The command status string (e.g., 'INSERT 0 1', 'UPDATE 5', 'DELETE 3').
            The format is: COMMAND OID ROWS for INSERT, or COMMAND ROWS for others.
    
    Raises:
        asyncpg.PostgresError: If the command execution fails.
        asyncpg.UniqueViolationError: If the command violates a unique constraint.
        asyncpg.ForeignKeyViolationError: If the command violates a foreign key.
    
    Example:
        # Insert a new record
        status = await execute_command(
            "INSERT INTO action_history (id, subid, action_taken) VALUES ($1, $2, $3)",
            "abc123",
            "sub001",
            "pause"
        )
        print(status)  # 'INSERT 0 1'
        
        # Update records
        status = await execute_command(
            "UPDATE analysis_run SET status = $1 WHERE id = $2",
            "completed",
            run_id
        )
        rows_affected = int(status.split()[-1])
        print(f"Updated {rows_affected} rows")
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)


async def execute_many(query: str, args_list: List[tuple]) -> None:
    """
    Execute a command for multiple sets of arguments (batch operation).
    
    This is a convenience helper for bulk INSERT/UPDATE/DELETE operations.
    It wraps asyncpg's executemany() method which efficiently executes the
    same query multiple times with different parameters.
    
    Note: asyncpg's executemany() does not use pipelining by default in all
    cases. For maximum performance with large batches, consider using
    copy_records_to_table() for INSERTs.
    
    Args:
        query: SQL command string with parameter placeholders.
        args_list: List of tuples, where each tuple contains parameters for
            one execution of the query.
    
    Raises:
        asyncpg.PostgresError: If any command execution fails.
    
    Example:
        # Batch insert
        records = [
            ("id1", "subid1", "Medicare", "Full O&O"),
            ("id2", "subid2", "Health", "Partial O&O"),
            ("id3", "subid3", "Life", "Non O&O"),
        ]
        await execute_many(
            "INSERT INTO fact_subid_day (id, subid, vertical, traffic_type) VALUES ($1, $2, $3, $4)",
            records
        )
    """
    pool = await get_db_pool()
    
    async with pool.acquire() as conn:
        await conn.executemany(query, args_list)
