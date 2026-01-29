"""
Backend API package initialization.

This package contains FastAPI router modules for the Quality Compass platform:
- runs: Analysis runs management (create, list, compute, detail bundle)
- actions: Action history and outcome tracking
- insights: Smart + WOW insights (anomalies, change-point, drivers, salvage, explain)
- macro_insights: Macro-level clustering and what-if simulations
- performance_history: Performance trend series endpoints
"""

from fastapi import APIRouter

# Import router modules
from backend.api.runs import router as runs_router
from backend.api.actions import router as actions_router
from backend.api.insights import router as insights_router
from backend.api.macro_insights import router as macro_insights_router
from backend.api.performance_history import router as performance_history_router

# Create main API router
api_router = APIRouter()

# Include all sub-routers
api_router.include_router(runs_router, prefix="/runs", tags=["runs"])
api_router.include_router(actions_router, prefix="/actions", tags=["actions"])
api_router.include_router(insights_router, tags=["insights"])  # insights router has its own prefix
api_router.include_router(macro_insights_router, tags=["macro-insights"])  # macro_insights router has its own prefix
api_router.include_router(performance_history_router, prefix="/performance-history", tags=["performance-history"])

# Export all routers for selective imports
__all__ = [
    "api_router",
    "runs_router",
    "actions_router",
    "insights_router",
    "macro_insights_router",
    "performance_history_router",
]
