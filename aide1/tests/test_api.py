"""Unit tests for the FastAPI application"""

import pytest
from fastapi.testclient import TestClient
from api import app

client = TestClient(app)


class TestAPIEndpoints:
    def test_root_endpoint(self):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data
        assert "version" in data
        assert data["version"] == "1.0.0"

    def test_health_check(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_predict_valid_input(self):
        payload = {"features": [5.1, 3.5, 1.4, 0.2]}
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert "prediction" in data
        assert "probability" in data
        assert isinstance(data["prediction"], int)
        assert len(data["probability"]) == 3

    def test_predict_invalid_input_length(self):
        payload = {"features": [5.1, 3.5]}
        response = client.post("/predict", json=payload)
        assert response.status_code == 400

    def test_predict_setosa(self):
        payload = {"features": [5.1, 3.5, 1.4, 0.2]}
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        assert response.json()["prediction"] == 0

    def test_predict_versicolor(self):
        payload = {"features": [6.4, 3.2, 4.5, 1.5]}
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        assert response.json()["prediction"] == 1

    def test_predict_virginica(self):
        payload = {"features": [6.3, 3.3, 6.0, 2.5]}
        response = client.post("/predict", json=payload)
        assert response.status_code == 200
        assert response.json()["prediction"] == 2

    def test_model_info(self):
        response = client.get("/info")
        assert response.status_code == 200
        data = response.json()
        assert data["model_name"] == "iris-classifier"
        assert len(data["features"]) == 4
        assert len(data["classes"]) == 3

    def test_metrics_endpoint(self):
        response = client.get("/metrics")
        assert response.status_code == 200
        assert b"api_requests_total" in response.content


class TestAPIValidation:
    def test_predict_missing_features(self):
        response = client.post("/predict", json={})
        assert response.status_code == 422

    def test_predict_non_numeric_features(self):
        response = client.post("/predict", json={"features": ["a", "b", "c", "d"]})
        assert response.status_code == 422


@pytest.mark.integration
class TestIntegration:
    def test_multiple_predictions(self):
        test_cases = [
            {"features": [5.1, 3.5, 1.4, 0.2], "expected": 0},
            {"features": [6.4, 3.2, 4.5, 1.5], "expected": 1},
            {"features": [6.3, 3.3, 6.0, 2.5], "expected": 2},
        ]
        for case in test_cases:
            response = client.post("/predict", json=case)
            assert response.status_code == 200
            assert response.json()["prediction"] == case["expected"]
