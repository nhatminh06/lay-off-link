"""Unit tests for model training"""

import pytest
from unittest.mock import patch, MagicMock
import numpy as np
from app import train_model


class TestModelTraining:
    @patch("app.mlflow")
    def test_train_model_basic(self, mock_mlflow):
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_run.info.artifact_uri = "test-artifact-uri"
        mock_mlflow.active_run.return_value = mock_run
        mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        model, accuracy = train_model(n_estimators=10, max_depth=3)

        assert model is not None
        assert 0 <= accuracy <= 1

    @patch("app.mlflow")
    def test_train_model_parameters(self, mock_mlflow):
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_run.info.artifact_uri = "test-artifact-uri"
        mock_mlflow.active_run.return_value = mock_run
        mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        train_model(n_estimators=50, max_depth=7)

        log_param_calls = mock_mlflow.log_param.call_args_list
        param_dict = {call[0][0]: call[0][1] for call in log_param_calls}
        assert param_dict.get("n_estimators") == 50
        assert param_dict.get("max_depth") == 7

    @patch("app.mlflow")
    def test_model_accuracy_threshold(self, mock_mlflow):
        mock_run = MagicMock()
        mock_run.info.run_id = "test-run-id"
        mock_run.info.artifact_uri = "test-artifact-uri"
        mock_mlflow.active_run.return_value = mock_run
        mock_mlflow.start_run.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_mlflow.start_run.return_value.__exit__ = MagicMock(return_value=False)

        model, accuracy = train_model(n_estimators=100, max_depth=10)
        assert accuracy >= 0.85
