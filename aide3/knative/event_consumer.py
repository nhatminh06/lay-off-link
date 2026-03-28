"""FastAPI sink service to consume prediction events from Knative Broker."""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import Counter, CONTENT_TYPE_LATEST, generate_latest

app = FastAPI(
    title="AIDE3 Knative Event Consumer",
    description="Consumes prediction events and flags anomalies.",
    version="1.0.0",
)


ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.85"))
EVENT_COUNT = Counter(
    "aide3_consumer_events_total",
    "Total events processed by anomaly consumer",
    ["result"],
)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "aide3-knative-consumer", "status": "running"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "healthy"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/events")
async def events(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON event payload") from exc

    score = float(payload.get("prediction_score", 0.0))
    is_anomaly = score >= ANOMALY_THRESHOLD
    EVENT_COUNT.labels(result="anomaly" if is_anomaly else "normal").inc()
    return {
        "accepted": True,
        "prediction_score": score,
        "anomaly_threshold": ANOMALY_THRESHOLD,
        "is_anomaly": is_anomaly,
    }
