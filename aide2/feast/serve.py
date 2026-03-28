"""
FastAPI service exposing Feast online/historical feature retrieval with Prometheus metrics.

Run from the feature repo directory (or set FEAST_REPO_PATH). Requires `feast apply`
to have materialized the registry and (for online) the online store.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from feast import FeatureStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

REPO_PATH = Path(os.environ.get("FEAST_REPO_PATH", Path(__file__).resolve().parent))

REQUESTS_TOTAL = Counter(
    "feast_api_requests_total",
    "Total HTTP requests to the Feast API",
    ["method", "path", "status"],
)
REQUEST_LATENCY_SECONDS = Histogram(
    "feast_api_request_latency_seconds",
    "Request latency in seconds",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, float("inf")),
)

_store: Optional["FeatureStore"] = None


def get_store() -> "FeatureStore":
    """Lazily construct the FeatureStore so import-time failures are avoided without config."""
    from feast import FeatureStore

    global _store
    if _store is None:
        try:
            _store = FeatureStore(repo_path=str(REPO_PATH))
        except Exception as exc:
            logger.exception("Failed to initialize Feast FeatureStore at %s", REPO_PATH)
            raise HTTPException(
                status_code=503,
                detail=f"Feast store unavailable: {exc!s}. Check registry and FEAST_REPO_PATH.",
            ) from exc
    return _store


app = FastAPI(
    title="NYC Taxi Feast API",
    description="Online and historical features backed by Feast.",
    version="1.0.0",
)


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    elapsed = time.perf_counter() - start
    path = request.url.path
    REQUEST_LATENCY_SECONDS.labels(request.method, path).observe(elapsed)
    REQUESTS_TOTAL.labels(request.method, path, str(response.status_code)).inc()
    return response


class OnlineFeaturesBody(BaseModel):
    """Request body for point-in-time online feature retrieval."""

    feature_refs: List[str] = Field(
        ...,
        description='Feast refs, e.g. ["zone_features:zone_avg_fare", "hourly_trip_features:avg_fare"]',
    )
    entity_rows: List[Dict[str, Any]] = Field(
        ...,
        description=(
            "Rows of entity join keys (e.g. location_id, vendor_id, hour_of_day). "
            "For on-demand views (e.g. fare_prediction_features), include request fields "
            "such as fare_amount alongside entity keys."
        ),
    )


class HistoricalFeaturesBody(BaseModel):
    """Request body for batch historical retrieval (training / backfill)."""

    feature_refs: List[str] = Field(..., description="Feast feature references to join.")
    entity_rows: List[Dict[str, Any]] = Field(
        ...,
        description=(
            "Entity rows; include `event_timestamp` (ISO 8601) per row for point-in-time joins. "
            "If omitted, UTC 'now' is used."
        ),
    )


def _fields_to_json(fields: List[Any]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for f in fields:
        dtype = getattr(f, "dtype", None)
        out.append(
            {
                "name": f.name,
                "dtype": str(dtype) if dtype is not None else "",
            }
        )
    return out


@app.get("/health")
def health() -> Dict[str, str]:
    """Liveness / readiness style health check."""
    return {"status": "ok", "feast_repo": str(REPO_PATH)}


@app.get("/metrics")
def metrics() -> Response:
    """Prometheus scrape endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/features/online")
def features_online(body: OnlineFeaturesBody) -> Dict[str, Any]:
    """
    Return the latest online features for the given entity rows and feature references.
    """
    store = get_store()
    try:
        response = store.get_online_features(
            features=body.feature_refs,
            entity_rows=body.entity_rows,
            full_feature_names=True,
        )
        return {"features": response.to_dict(include_event_timestamps=True)}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("get_online_features failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/features/historical")
def features_historical(body: HistoricalFeaturesBody) -> Dict[str, Any]:
    """
    Run a historical (offline) point-in-time join and return the resulting table as JSON.
    """
    store = get_store()
    try:
        df = pd.DataFrame(body.entity_rows)
        if df.empty:
            raise HTTPException(status_code=400, detail="entity_rows must not be empty")
        if "event_timestamp" not in df.columns:
            df = df.copy()
            df["event_timestamp"] = pd.Timestamp.now(tz="UTC")
        else:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True)

        job = store.get_historical_features(
            entity_df=df,
            features=body.feature_refs,
            full_feature_names=True,
        )
        out_df = job.to_df()
        # Native JSON types
        return {
            "dataframe": out_df.astype(object)
            .where(pd.notnull(out_df), None)
            .to_dict(orient="list")
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("get_historical_features failed")
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/features/list")
def features_list() -> Dict[str, Any]:
    """List registered feature views and on-demand feature views with schemas."""
    from feast.feature_view import FeatureView
    from feast.on_demand_feature_view import OnDemandFeatureView

    store = get_store()
    try:
        batch_views: List[Dict[str, Any]] = []
        for fv in store.list_feature_views():
            if not isinstance(fv, FeatureView):
                continue
            batch_views.append(
                {
                    "name": fv.name,
                    "type": "feature_view",
                    "entities": list(fv.entities),
                    "ttl_seconds": fv.ttl.total_seconds() if fv.ttl else None,
                    "schema": _fields_to_json(list(fv.schema)),
                }
            )

        odfvs: List[Dict[str, Any]] = []
        for od in store.list_on_demand_feature_views():
            if not isinstance(od, OnDemandFeatureView):
                continue
            odfvs.append(
                {
                    "name": od.name,
                    "type": "on_demand_feature_view",
                    "entities": list(od.entities),
                    "schema": _fields_to_json(list(od.schema)),
                }
            )

        return {"feature_views": batch_views, "on_demand_feature_views": odfvs}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("list feature views failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("serve:app", host="0.0.0.0", port=int(os.environ.get("PORT", "6566")), reload=False)
