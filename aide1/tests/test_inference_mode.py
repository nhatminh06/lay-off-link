"""
Tests for AIDE1's explicit INFERENCE_MODE switch.

api.py reads INFERENCE_MODE/MODEL_URI as module-level constants at import
time, so these tests set env vars via monkeypatch and importlib.reload the
module to pick them up, then restore demo mode afterward so other test
modules (which import `api` expecting default demo behavior) aren't affected.
"""

import importlib
import os
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

import api

_METRIC_ATTRS = ("REQUEST_COUNT", "REQUEST_LATENCY", "PREDICTION_COUNT")


def _reload_api():
    """
    Reload api.py to pick up new env vars. Prometheus's default registry is
    process-global (not per-module), so the Counters/Histogram created at
    api.py's module level must be unregistered first or re-registration
    raises ValueError: Duplicated timeseries -- which would otherwise abort
    the reload partway through and leave api.app with a corrupted route
    table for every test module imported afterward.
    """
    for name in _METRIC_ATTRS:
        collector = getattr(api, name, None)
        if collector is not None:
            try:
                REGISTRY.unregister(collector)
            except KeyError:
                pass
    importlib.reload(api)


def _reload_as_demo():
    """Restore api.py to default demo mode after a test that changed env vars."""
    os.environ.pop("INFERENCE_MODE", None)
    os.environ.pop("MODEL_URI", None)
    _reload_api()


@pytest.fixture(autouse=True)
def _restore_demo_mode_after_test():
    yield
    _reload_as_demo()


class TestInvalidInferenceMode:
    def test_invalid_mode_raises_at_import_time(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "bogus")
        with pytest.raises(RuntimeError, match="Invalid INFERENCE_MODE"):
            _reload_api()


class TestModelModeSuccessfulLoad:
    def _fake_model(self):
        model = MagicMock()
        model.predict.return_value = [1]
        model.predict_proba.return_value = [[0.1, 0.8, 0.1]]
        return model

    def test_ready_true_and_predict_uses_real_model(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "model")
        monkeypatch.setenv("MODEL_URI", "models:/fake-iris/1")
        _reload_api()

        fake_model = self._fake_model()
        monkeypatch.setattr(api.mlflow.sklearn, "load_model", lambda uri: fake_model)

        with TestClient(api.app) as client:
            ready = client.get("/ready")
            assert ready.status_code == 200
            assert ready.json() == {
                "ready": True,
                "inference_mode": "model",
                "model_uri": "models:/fake-iris/1",
            }

            resp = client.post("/predict", json={"features": [6.4, 3.2, 4.5, 1.5]})
            assert resp.status_code == 200
            data = resp.json()
            assert data["prediction"] == 1
            assert data["probability"] == [0.1, 0.8, 0.1]
            assert data["inference_mode"] == "model"
            assert data["model_version"] == "models:/fake-iris/1"
            fake_model.predict.assert_called_once()
            fake_model.predict_proba.assert_called_once()

    def test_health_reports_model_mode_and_loaded_true(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "model")
        _reload_api()
        monkeypatch.setattr(api.mlflow.sklearn, "load_model", lambda uri: self._fake_model())

        with TestClient(api.app) as client:
            data = client.get("/health").json()
            assert data["inference_mode"] == "model"
            assert data["model_loaded"] is True

    def test_falls_back_to_uniform_confidence_without_predict_proba(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "model")
        _reload_api()

        model_without_proba = MagicMock(spec=["predict"])
        model_without_proba.predict.return_value = [2]
        monkeypatch.setattr(api.mlflow.sklearn, "load_model", lambda uri: model_without_proba)

        with TestClient(api.app) as client:
            resp = client.post("/predict", json={"features": [6.3, 3.3, 6.0, 2.5]})
            assert resp.status_code == 200
            assert resp.json()["prediction"] == 2
            assert resp.json()["probability"] == [0.0, 0.0, 1.0]


class TestModelModeLoadFailure:
    def test_ready_returns_503_when_load_fails(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "model")
        _reload_api()

        def _boom(uri):
            raise RuntimeError("registry unreachable")

        monkeypatch.setattr(api.mlflow.sklearn, "load_model", _boom)

        with TestClient(api.app) as client:
            resp = client.get("/ready")
            assert resp.status_code == 503
            assert resp.json()["detail"]["ready"] is False
            assert "registry unreachable" in resp.json()["detail"]["error"]

    def test_predict_returns_503_instead_of_silently_using_demo_logic(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "model")
        _reload_api()
        monkeypatch.setattr(
            api.mlflow.sklearn,
            "load_model",
            lambda uri: (_ for _ in ()).throw(RuntimeError("no such model")),
        )

        with TestClient(api.app) as client:
            resp = client.post("/predict", json={"features": [5.1, 3.5, 1.4, 0.2]})
            assert resp.status_code == 503
            # Must not silently return a demo-mode-style 200 prediction.
            assert "prediction" not in resp.json()

    def test_health_reports_model_loaded_false(self, monkeypatch):
        monkeypatch.setenv("INFERENCE_MODE", "model")
        _reload_api()
        monkeypatch.setattr(
            api.mlflow.sklearn,
            "load_model",
            lambda uri: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        with TestClient(api.app) as client:
            data = client.get("/health").json()
            assert data["model_loaded"] is False


class TestDemoModeExplicitLabeling:
    def test_predict_response_labels_demo_mode(self):
        with TestClient(api.app) as client:
            resp = client.post("/predict", json={"features": [5.1, 3.5, 1.4, 0.2]})
            data = resp.json()
            assert data["inference_mode"] == "demo"
            assert data["model_version"] == "demo-petal-length-rule-v1"

    def test_ready_always_true_in_demo_mode(self):
        with TestClient(api.app) as client:
            resp = client.get("/ready")
            assert resp.status_code == 200
            assert resp.json() == {"ready": True, "inference_mode": "demo"}

    def test_info_labels_demo_mode(self):
        with TestClient(api.app) as client:
            data = client.get("/info").json()
            assert data["inference_mode"] == "demo"
