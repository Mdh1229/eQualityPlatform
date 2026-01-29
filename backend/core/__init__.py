"""
Core infrastructure package for the FastAPI backend.

Provides:
- Configuration management via pydantic-settings
- Async PostgreSQL database connectivity via asyncpg
- FastAPI dependency injection utilities

Per Section 0.3.2 Design Pattern Applications:
- Dependency Injection for Loose Coupling
- Database sessions, config, and auth injected into endpoints
- Enables easy testing with mocks
"""

from typing import List

# Track available exports for dynamic __all__ construction
_exports: List[str] = []

# Import config module (implemented)
try:
    from backend.core.config import Settings, get_settings
    _exports.extend(['Settings', 'get_settings'])
except ImportError as e:
    import warnings
    warnings.warn(f"Could not import config: {e}")

# Import database module (pending implementation by other agents)
try:
    from backend.core.database import init_db, close_db, get_db_pool
    _exports.extend(['init_db', 'close_db', 'get_db_pool'])
except ImportError:
    # database.py is a pending file to be created by another agent
    # per Section 0.4.1 Backend Files table
    pass

# Import dependencies module (pending implementation by other agents)
try:
    from backend.core.dependencies import (
        get_db_session,
        get_settings_dependency,
        SettingsDep,
        DBSessionDep,
    )
    _exports.extend([
        'get_db_session',
        'get_settings_dependency',
        'SettingsDep',
        'DBSessionDep',
    ])
except ImportError:
    # dependencies.py is a pending file to be created by another agent
    # per Section 0.4.1 Backend Files table
    pass

# Construct __all__ from successfully imported exports
__all__ = _exports
