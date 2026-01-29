"""
FastAPI application entry point for Quality Compass API.

This module serves as the central orchestration file for the Python backend service layer.
It configures CORS, registers API routers, and starts the ASGI server.

Per Section 0.3.2 Design Pattern Applications:
- Dependency injection for loose coupling
- Database sessions injected into endpoints
- Enables easy testing with mocks
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.core.database import init_db, close_db
from backend.api.runs import router as runs_router
from backend.api.actions import router as actions_router
from backend.api.insights import router as insights_router
from backend.api.performance_history import router as performance_history_router
from backend.api.macro_insights import router as macro_insights_router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Lifespan context manager for FastAPI application startup and shutdown events.
    
    On startup:
        - Initialize database connection pool
        - Log startup message
    
    On shutdown:
        - Close database connection pool
        - Log shutdown message
    """
    # Startup
    logger.info("Quality Compass API starting")
    try:
        await init_db()
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        # Continue startup even if DB fails - some endpoints may not need DB
    
    yield
    
    # Shutdown
    logger.info("Quality Compass API shutting down")
    try:
        await close_db()
        logger.info("Database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing database pool: {e}")


# Create FastAPI application
app = FastAPI(
    title="Quality Compass API",
    version="1.0.0",
    description=(
        "FastAPI backend for Quality Compass platform. "
        "Provides endpoints for analysis runs, actions, insights, "
        "performance history, and macro-level clustering."
    ),
    lifespan=lifespan,
)

# Configure CORS middleware
# Per Section 0.3.1: Next.js API routes proxy to FastAPI so frontend contracts remain unchanged
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # Next.js dev server
        "http://127.0.0.1:3000",  # Alternative localhost
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routers
# Per Section 0.4.1 File-by-File Transformation Plan
app.include_router(runs_router, prefix="/runs", tags=["runs"])
app.include_router(actions_router, prefix="/actions", tags=["actions"])
app.include_router(insights_router, tags=["insights"])  # Has its own /insights prefix
app.include_router(performance_history_router, prefix="/performance-history", tags=["performance-history"])
app.include_router(macro_insights_router, tags=["macro-insights"])  # Has its own /macro-insights prefix


@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring and load balancer probes.
    
    Returns:
        Dict with status 'healthy'
    """
    return {"status": "healthy"}


@app.get("/")
async def root():
    """
    Root endpoint providing API information.
    
    Returns:
        Dict with API name and version
    """
    return {
        "name": "Quality Compass API",
        "version": "1.0.0",
        "docs": "/docs",
        "openapi": "/openapi.json",
    }


# Run with uvicorn when executed directly
if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "backend.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
