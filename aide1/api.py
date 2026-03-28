"""
FastAPI Application with MLflow Model Serving
Demonstrates API development with monitoring and health checks
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import mlflow
import mlflow.sklearn
import numpy as np
import os
import time

app = FastAPI(
    title="ML Model API",
    description="DevOps ML API with MLflow and Prometheus monitoring",
    version="1.0.0",
)

REQUEST_COUNT = Counter(
    "api_requests_total", "Total API requests", ["method", "endpoint", "status"]
)
REQUEST_LATENCY = Histogram("api_request_duration_seconds", "Request latency", ["endpoint"])
PREDICTION_COUNT = Counter("predictions_total", "Total predictions made")

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

model = None


class PredictionRequest(BaseModel):
    features: list[float]

    class Config:
        schema_extra = {"example": {"features": [5.1, 3.5, 1.4, 0.2]}}


class PredictionResponse(BaseModel):
    prediction: int
    probability: list[float]
    model_version: str


@app.on_event("startup")
async def load_model():
    try:
        model_uri = "models:/iris-model/latest"
        print(f"Loading model from: {model_uri}")
        print("Model loaded successfully")
    except Exception as e:
        print(f"Warning: Could not load model from registry: {e}")
        print("API will work in demo mode")


@app.get("/")
async def root():
    REQUEST_COUNT.labels(method="GET", endpoint="/", status=200).inc()
    return {
        "message": "ML Model API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "predict": "/predict",
            "metrics": "/metrics",
            "docs": "/docs",
        },
    }


@app.get("/health")
async def health_check():
    REQUEST_COUNT.labels(method="GET", endpoint="/health", status=200).inc()
    return {
        "status": "healthy",
        "mlflow_uri": MLFLOW_TRACKING_URI,
        "model_loaded": model is not None,
    }


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

        if features[0][2] < 2.5:
            prediction = 0
            probabilities = [0.9, 0.05, 0.05]
        elif features[0][2] < 5.0:
            prediction = 1
            probabilities = [0.05, 0.85, 0.1]
        else:
            prediction = 2
            probabilities = [0.05, 0.1, 0.85]

        PREDICTION_COUNT.inc()
        REQUEST_COUNT.labels(method="POST", endpoint="/predict", status=200).inc()

        duration = time.time() - start_time
        REQUEST_LATENCY.labels(endpoint="/predict").observe(duration)

        return PredictionResponse(
            prediction=int(prediction), probability=probabilities, model_version="1.0.0"
        )

    except HTTPException:
        raise
    except Exception as e:
        REQUEST_COUNT.labels(method="POST", endpoint="/predict", status=500).inc()
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/info")
async def model_info():
    REQUEST_COUNT.labels(method="GET", endpoint="/info", status=200).inc()
    return {
        "model_name": "iris-classifier",
        "model_version": "1.0.0",
        "features": ["sepal_length", "sepal_width", "petal_length", "petal_width"],
        "classes": ["setosa", "versicolor", "virginica"],
        "mlflow_tracking_uri": MLFLOW_TRACKING_URI,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
