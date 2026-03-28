"""AIDE 3 FastAPI service with event publishing hook for Knative broker."""

from __future__ import annotations

import os
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

app = FastAPI(
    title="AIDE3 Inference API",
    description="Inference endpoint that emits real-time events for anomaly detection.",
    version="1.0.0",
)

REQUEST_COUNT = Counter(
    "aide3_api_requests_total",
    "Total requests for AIDE 3 API",
    ["endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "aide3_api_request_duration_seconds",
    "Request duration for AIDE 3 API",
    ["endpoint"],
)
EVENT_PUBLISH_COUNT = Counter(
    "aide3_event_publish_total",
    "Total emitted events",
    ["status"],
)

KNATIVE_BROKER_URL = os.getenv("KNATIVE_BROKER_URL", "http://anomaly-consumer:8081/events")
EVENT_ENABLED = os.getenv("AIDE3_EVENT_ENABLED", "true").lower() == "true"
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.85"))


class PredictionRequest(BaseModel):
    features: list[float] = Field(
        ...,
        min_length=4,
        max_length=4,
        description="Four feature values for online scoring.",
    )


class PredictionResponse(BaseModel):
    prediction: int
    prediction_score: float
    event_dispatched: bool
    request_id: str


def _simple_score(features: list[float]) -> tuple[int, float]:
    """Simple deterministic score for demo; replace with model call in production."""
    weighted = features[0] * 0.25 + features[1] * 0.15 + features[2] * 0.35 + features[3] * 0.25
    normalized = min(1.0, max(0.0, weighted / 10.0))
    if normalized < 0.45:
        pred = 0
    elif normalized < 0.75:
        pred = 1
    else:
        pred = 2
    return pred, normalized


def publish_prediction_event(payload: dict[str, Any], timeout_s: float = 2.0) -> bool:
    """Publish event payload to Knative Broker ingress endpoint."""
    if not EVENT_ENABLED:
        EVENT_PUBLISH_COUNT.labels(status="disabled").inc()
        return False
    try:
        headers = {
            "Content-Type": "application/json",
            "Ce-Id": str(uuid.uuid4()),
            "Ce-Specversion": "1.0",
            "Ce-Type": "aide3.prediction.created",
            "Ce-Source": "aide3.serving.api",
        }
        response = requests.post(
            KNATIVE_BROKER_URL,
            json=payload,
            headers=headers,
            timeout=timeout_s,
        )
        response.raise_for_status()
        EVENT_PUBLISH_COUNT.labels(status="success").inc()
        return True
    except Exception:
        EVENT_PUBLISH_COUNT.labels(status="failed").inc()
        return False


@app.get("/health")
async def health() -> dict[str, Any]:
    REQUEST_COUNT.labels(endpoint="/health", status="200").inc()
    return {
        "status": "healthy",
        "knative_broker_url": KNATIVE_BROKER_URL,
        "events_enabled": EVENT_ENABLED,
        "anomaly_threshold": ANOMALY_THRESHOLD,
    }


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest) -> PredictionResponse:
    started = datetime.now(tz=timezone.utc)
    try:
        prediction, score = _simple_score(request.features)
        request_id = str(uuid.uuid4())
        event_payload = {
            "request_id": request_id,
            "timestamp": started.isoformat(),
            "features": request.features,
            "prediction": prediction,
            "prediction_score": score,
            "is_anomaly": score >= ANOMALY_THRESHOLD,
            "trace": {"rand": random.randint(1000, 9999)},
        }
        sent = publish_prediction_event(event_payload)
        REQUEST_COUNT.labels(endpoint="/predict", status="200").inc()
        elapsed = (datetime.now(tz=timezone.utc) - started).total_seconds()
        REQUEST_LATENCY.labels(endpoint="/predict").observe(elapsed)
        return PredictionResponse(
            prediction=prediction,
            prediction_score=score,
            event_dispatched=sent,
            request_id=request_id,
        )
    except Exception as exc:
        REQUEST_COUNT.labels(endpoint="/predict", status="500").inc()
        raise HTTPException(status_code=500, detail=f"Prediction failed: {exc}") from exc
