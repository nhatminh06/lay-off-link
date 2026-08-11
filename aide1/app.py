"""
MLflow Model Training Application
A sample machine learning application demonstrating DevOps practices
"""

import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.datasets import load_iris
import numpy as np
import argparse
import os


def train_model(n_estimators=100, max_depth=5, experiment_name="iris-classification"):
    mlflow_tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(mlflow_tracking_uri)
    mlflow.set_experiment(experiment_name)

    print(f"Starting MLflow experiment: {experiment_name}")
    print(f"MLflow Tracking URI: {mlflow_tracking_uri}")

    iris = load_iris()
    X, y = iris.data, iris.target
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("test_size", 0.2)
        mlflow.log_param("random_state", 42)

        print(f"Training Random Forest with n_estimators={n_estimators}, max_depth={max_depth}")
        model = RandomForestClassifier(
            n_estimators=n_estimators, max_depth=max_depth, random_state=42
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average="weighted")
        recall = recall_score(y_test, y_pred, average="weighted")
        f1 = f1_score(y_test, y_pred, average="weighted")

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)

        mlflow.sklearn.log_model(model, "model", registered_model_name="iris-model")

        feature_importance = dict(zip(iris.feature_names, model.feature_importances_))
        for feature, importance in feature_importance.items():
            mlflow.log_metric(f"importance_{feature}", importance)

        print(f"\nModel Training Complete!")
        print(f"Accuracy: {accuracy:.4f}")
        print(f"Precision: {precision:.4f}")
        print(f"Recall: {recall:.4f}")
        print(f"F1 Score: {f1:.4f}")

        run = mlflow.active_run()
        print(f"\nMLflow Run ID: {run.info.run_id}")
        print(f"Artifact URI: {run.info.artifact_uri}")

        return model, accuracy


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train ML model with MLflow tracking")
    parser.add_argument("--n-estimators", type=int, default=100, help="Number of trees")
    parser.add_argument("--max-depth", type=int, default=5, help="Maximum tree depth")
    parser.add_argument(
        "--experiment", type=str, default="iris-classification", help="Experiment name"
    )

    args = parser.parse_args()

    train_model(
        n_estimators=args.n_estimators, max_depth=args.max_depth, experiment_name=args.experiment
    )
