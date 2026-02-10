import os
from fastapi import FastAPI
from contextlib import asynccontextmanager

from app.routers import metrics, health


def _create_client():
    """Use Snowflake if configured, otherwise fall back to local CSV data."""
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        from app.services.snowflake_client import SnowflakeClient
        print("Using Snowflake client")
        return SnowflakeClient()
    else:
        from app.services.local_client import LocalClient
        print("No SNOWFLAKE_ACCOUNT set — using local CSV data")
        return LocalClient()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage data client lifecycle."""
    client = _create_client()
    client.connect()
    app.state.sf_client = client
    yield
    client.close()

app = FastAPI(
    title="Data Lakehouse API",
    description="REST API exposing Gold-layer metrics from the Data Lakehouse",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router, tags=["Health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["Metrics"])
