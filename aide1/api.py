"""
FastAPI serving for the AIDE1 Iris classifier.

Two explicit inference modes, selected by the INFERENCE_MODE environment
variable:

- INFERENCE_MODE=model: loads a real model from the MLflow Model Registry
  (MODEL_URI, default "models:/iris-model/latest") at startup and serves
  predictions from it. If loading fails, the process still starts, but
  /ready reports not-ready and /predict returns 503 -- it never silently
  substitutes demo logic for a real model.
- INFERENCE_MODE=demo (default): a deterministic petal-length rule with no
  MLflow dependency. Every response and the /health, /ready, and /info
  endpoints label this explicitly as "demo" so it is never mistaken for
  real model inference.
"""

import logging
import os
import time
from contextlib import asynccontextmanager

import mlflow
import mlflow.sklearn
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

INFERENCE_MODE = os.getenv("INFERENCE_MODE", "demo").strip().lower()
if INFERENCE_MODE not in ("model", "demo"):
    raise RuntimeError(f"Invalid INFERENCE_MODE={INFERENCE_MODE!r}; must be 'model' or 'demo'")

MODEL_URI = os.getenv("MODEL_URI", "models:/iris-model/latest")

_state = {"model": None, "model_load_error": None}


def _load_model() -> None:
    try:
        _state["model"] = mlflow.sklearn.load_model(MODEL_URI)
        _state["model_load_error"] = None
        logger.info("Loaded model from %s", MODEL_URI)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        _state["model"] = None
        _state["model_load_error"] = str(exc)
        logger.exception("Failed to load model from %s", MODEL_URI)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    if INFERENCE_MODE == "model":
        _load_model()
    yield


app = FastAPI(
    title="AIDE1 Iris Classifier API",
    description=(
        "Serves the AIDE1 Iris classifier. INFERENCE_MODE=model loads a real "
        "MLflow-registered model; INFERENCE_MODE=demo (default) uses a "
        "deterministic rule and never claims to be model inference."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

REQUEST_COUNT = Counter(
    "api_requests_total", "Total API requests", ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram("api_request_duration_seconds", "Request latency", ["endpoint"])
PREDICTION_COUNT = Counter("predictions_total", "Total predictions made")


class PredictionRequest(BaseModel):
    features: list[float]

    class Config:
        schema_extra = {"example": {"features": [5.1, 3.5, 1.4, 0.2]}}


class PredictionResponse(BaseModel):
    prediction: int
    probability: list[float]
    model_version: str
    inference_mode: str


def _demo_predict(features: np.ndarray) -> tuple[int, list[float]]:
    """Deterministic petal-length rule. Not a trained model -- demo mode only."""
    petal_length = features[0][2]
    if petal_length < 2.5:
        return 0, [0.9, 0.05, 0.05]
    if petal_length < 5.0:
        return 1, [0.05, 0.85, 0.1]
    return 2, [0.05, 0.1, 0.85]


@app.get("/")
async def root():
    REQUEST_COUNT.labels(method="GET", endpoint="/", status=200).inc()
    return {
        "message": "AIDE1 Iris Classifier API",
        "version": "1.0.0",
        "inference_mode": INFERENCE_MODE,
        "endpoints": {
            "health": "/health",
            "ready": "/ready",
            "predict": "/predict",
            "metrics": "/metrics",
            "docs": "/docs",
        },
    }


@app.get("/health")
async def health_check():
    """Liveness: the process is up. Does not imply a model is loaded."""
    REQUEST_COUNT.labels(method="GET", endpoint="/health", status=200).inc()
    return {
        "status": "healthy",
        "inference_mode": INFERENCE_MODE,
        "mlflow_uri": MLFLOW_TRACKING_URI,
        "model_loaded": _state["model"] is not None,
    }


@app.get("/ready")
async def ready_check():
    """
    Readiness: in demo mode, always ready (there's nothing to load). In
    model mode, ready only once a real model has successfully loaded --
    never reports ready on the strength of demo logic being available.
    """
    if INFERENCE_MODE == "demo":
        return {"ready": True, "inference_mode": "demo"}

    ready = _state["model"] is not None
    body = {"ready": ready, "inference_mode": "model", "model_uri": MODEL_URI}
    if not ready:
        body["error"] = _state["model_load_error"]
        raise HTTPException(status_code=503, detail=body)
    return body


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    start_time = time.time()

    try:
        if len(request.features) != 4:
            REQUEST_COUNT.labels(method="POST", endpoint="/predict", status=400).inc()
            raise HTTPException(
                status_code=400,
                detail="Expected 4 features (sepal length, sepal width, petal length, petal width)",
            )

        features = np.array(request.features).reshape(1, -1)

        if INFERENCE_MODE == "model":
            model = _state["model"]
            if model is None:
                REQUEST_COUNT.labels(method="POST", endpoint="/predict", status=503).inc()
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "INFERENCE_MODE=model but no model is loaded "
                        f"({_state['model_load_error']}); refusing to silently "
                        "fall back to demo predictions."
                    ),
                )
            prediction = int(model.predict(features)[0])
            if hasattr(model, "predict_proba"):
                probabilities = [float(p) for p in model.predict_proba(features)[0]]
            else:
                probabilities = [1.0 if i == prediction else 0.0 for i in range(3)]
            model_version = MODEL_URI
        else:
            prediction, probabilities = _demo_predict(features)
            model_version = "demo-petal-length-rule-v1"

        PREDICTION_COUNT.inc()
        REQUEST_COUNT.labels(method="POST", endpoint="/predict", status=200).inc()

        duration = time.time() - start_time
        REQUEST_LATENCY.labels(endpoint="/predict").observe(duration)

        return PredictionResponse(
            prediction=prediction,
            probability=probabilities,
            model_version=model_version,
            inference_mode=INFERENCE_MODE,
        )

    except HTTPException:
        raise
    except Exception as e:
        REQUEST_COUNT.labels(method="POST", endpoint="/predict", status=500).inc()
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}") from e


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/info")
async def model_info():
    REQUEST_COUNT.labels(method="GET", endpoint="/info", status=200).inc()
    return {
        "model_name": "iris-classifier",
        "model_version": MODEL_URI if INFERENCE_MODE == "model" else "demo-petal-length-rule-v1",
        "inference_mode": INFERENCE_MODE,
        "features": ["sepal_length", "sepal_width", "petal_length", "petal_width"],
        "classes": ["setosa", "versicolor", "virginica"],
        "mlflow_tracking_uri": MLFLOW_TRACKING_URI,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
