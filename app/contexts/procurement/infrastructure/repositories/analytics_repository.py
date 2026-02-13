# Backward-compatibility shim. Analytics repository lives in app.contexts.analytics.
from app.contexts.analytics.infrastructure.repository import AnalyticsRepository

__all__ = ["AnalyticsRepository"]
