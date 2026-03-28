"""Tests for monitoring and drift detection"""

import pytest
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier


class TestMonitoringBasics:
    def test_prometheus_metrics_format(self):
        from fastapi.testclient import TestClient
        from api import app

        client = TestClient(app)

        response = client.get("/metrics")
        assert response.status_code == 200
        assert b"api_requests_total" in response.content

    def test_health_endpoint_structure(self):
        from fastapi.testclient import TestClient
        from api import app

        client = TestClient(app)

        response = client.get("/health")
        data = response.json()
        assert "status" in data
        assert "mlflow_uri" in data
        assert "model_loaded" in data


class TestModelQuality:
    def test_model_predictions_valid_range(self):
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split

        iris = load_iris()
        X_train, X_test, y_train, y_test = train_test_split(
            iris.data, iris.target, test_size=0.2, random_state=42
        )
        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(X_train, y_train)

        predictions = model.predict(X_test)
        assert all(p in [0, 1, 2] for p in predictions)

    def test_model_probability_sums(self):
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split

        iris = load_iris()
        X_train, X_test, y_train, y_test = train_test_split(
            iris.data, iris.target, test_size=0.2, random_state=42
        )
        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(X_train, y_train)

        probabilities = model.predict_proba(X_test)
        for row in probabilities:
            assert abs(sum(row) - 1.0) < 1e-6
