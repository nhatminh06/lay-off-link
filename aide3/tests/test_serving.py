"""Unit tests for AIDE 3 serving and event consumer APIs."""

from fastapi.testclient import TestClient

from aide3.knative.event_consumer import app as consumer_app
from aide3.serving.api import app as serving_app


def test_serving_health():
    client = TestClient(serving_app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_predict_success(monkeypatch):
    client = TestClient(serving_app)

    def _mock_publish(_payload, timeout_s=2.0):
        return True

    monkeypatch.setattr("aide3.serving.api.publish_prediction_event", _mock_publish)

    response = client.post("/predict", json={"features": [5.0, 2.8, 4.0, 1.3]})
    assert response.status_code == 200
    data = response.json()
    assert "prediction" in data
    assert "prediction_score" in data
    assert data["event_dispatched"] is True


def test_consumer_event():
    client = TestClient(consumer_app)
    response = client.post("/events", json={"prediction_score": 0.91})
    assert response.status_code == 200
    assert response.json()["is_anomaly"] is True
