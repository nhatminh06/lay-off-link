"""Integration tests for the complete ML pipeline"""

import pytest
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import joblib
import tempfile
import os


class TestMLPipeline:
    @pytest.fixture
    def iris_data(self):
        iris = load_iris()
        X_train, X_test, y_train, y_test = train_test_split(
            iris.data, iris.target, test_size=0.2, random_state=42
        )
        return X_train, X_test, y_train, y_test

    def test_end_to_end_pipeline(self, iris_data):
        X_train, X_test, y_train, y_test = iris_data
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(X_train_scaled, y_train)
        predictions = model.predict(X_test_scaled)
        accuracy = (predictions == y_test).mean()

        assert accuracy > 0.8
        assert len(predictions) == len(y_test)
        assert set(predictions).issubset({0, 1, 2})

    def test_model_persistence(self, iris_data):
        X_train, X_test, y_train, y_test = iris_data
        model = RandomForestClassifier(n_estimators=10, random_state=42)
        model.fit(X_train, y_train)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pkl") as f:
            model_path = f.name
            joblib.dump(model, model_path)

        try:
            loaded_model = joblib.load(model_path)
            original_pred = model.predict(X_test)
            loaded_pred = loaded_model.predict(X_test)
            assert np.array_equal(original_pred, loaded_pred)
        finally:
            os.unlink(model_path)


class TestAPIIntegration:
    def test_prediction_workflow(self):
        from api import app
        from fastapi.testclient import TestClient

        client = TestClient(app)

        assert client.get("/health").status_code == 200
        assert client.get("/info").status_code == 200

        pred = client.post("/predict", json={"features": [5.1, 3.5, 1.4, 0.2]})
        assert pred.status_code == 200
        assert "prediction" in pred.json()

        assert client.get("/metrics").status_code == 200
