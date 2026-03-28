"""
Quality Compass Backend Package.

FastAPI service layer for the Quality Compass quality tier classification system.
Provides compute pipelines, insights generation, and data management.

Subpackages:
    - api: FastAPI route handlers
    - core: Configuration, database, and dependencies
    - models: Pydantic schemas and enums
    - services: Business logic services
    - jobs: Daily automation jobs
    - sql: Parameterized SQL queries

Per Section 0.3.1 Target Repository Structure, this backend package provides
the Python-based service layer while maintaining compatibility with the
existing Next.js frontend through API proxies.
"""

__version__ = "1.0.0"
