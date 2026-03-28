# Lay-Off-Link: End-to-End MLOps and Data Platform

This repository contains a complete end-to-end platform that combines:

- MLOps model training, tracking, and serving
- Data engineering pipelines (batch + streaming)
- Feature store for online/offline features
- Cloud-ready infrastructure and Kubernetes deployment

The project is designed with a modern, observable, and scalable architecture.

## Table of Contents

- [Lay-Off-Link: End-to-End MLOps and Data Platform](#lay-off-link-end-to-end-mlops-and-data-platform)
  - [Table of Contents](#table-of-contents)
  - [Demo Video](#demo-video)
  - [Repository Structure](#repository-structure)
  - [System Architecture](#system-architecture)
  - [Installation and Usage](#installation-and-usage)
    - [Running the Notebooks](#running-the-notebooks)
    - [Local Deployment with Docker Compose](#local-deployment-with-docker-compose)
    - [Local Kubernetes Deployment](#local-kubernetes-deployment)
    - [Cloud Deployment on GKE/GCE with Terraform](#cloud-deployment-on-gkegce-with-terraform)
  - [CI/CD Pipeline](#cicd-pipeline)
    - [Continuous Integration and Testing](#continuous-integration-and-testing)
    - [Continuous Deployment](#continuous-deployment)
  - [Observability](#observability)
  - [Tech Stack](#tech-stack)

## Demo Video

Add your final demo video link here:

- Demo: `TBD`

## Repository Structure

The repository is organized by capabilities.

```text
.
├── aide1/                         # MLOps application (MLflow, FastAPI, tests, notebooks)
├── aide2/                         # Data platform (Spark, Flink, Airflow, Feast, ingestion)
├── aide3/                         # Kubeflow, Knative, real-time serving, tests, notebooks
├── docker-compose.yml             # Local orchestration with profiles (aide1, aide2, aide3)
├── k8s/                           # Kubernetes manifests for platform services
├── helm/                          # Helm charts (ml-app, aide2-platform, aide3-platform)
├── terraform/                     # Infrastructure as code for GKE/GCE/GCS/Composer
├── monitoring/                    # Prometheus, Grafana, Loki, dashboards
├── nginx/                         # API gateway config
├── .github/workflows/ci-cd.yml    # CI/CD automation
├── Makefile                       # Common local and deployment commands
├── checklist.md                   # Requirement checklist
└── checklist_status.md            # Current requirement status snapshot
```

## System Architecture

This project runs in two modes:

- Local development with Docker Compose
- Cloud deployment with Kubernetes + Terraform

High-level architecture:

1. Train and track models with MLflow (`aide1/`).
2. Ingest and process data via Spark/Flink/Airflow (`aide2/`).
3. Train/evaluate via Kubeflow Pipelines and publish model artifacts (`aide3/kubeflow/`).
4. Capture real-time events with Knative Eventing for anomaly workflows (`aide3/knative/`).
5. Serve predictions via FastAPI + KServe behind NGINX.
6. Monitor logs/metrics/traces via Loki, Prometheus, Grafana, Jaeger.

Core services:

- **Serving:** FastAPI, KServe, NGINX, Knative Eventing
- **Tracking/Registry:** MLflow, object storage, SQL metadata
- **Data Platform:** MinIO, Kafka, Spark, Flink, Trino, Airflow, Feast
- **Observability:** Prometheus, Grafana, Loki, Jaeger
- **Platform:** Docker, Kubernetes, Helm, Terraform, GitHub Actions

## Installation and Usage

### Running the Notebooks

Notebooks are provided for both model and data workflows:

- `aide1/notebooks/` for EDA, processing, modeling, deployment prep
- `aide2/notebooks/` for batch processing, stream processing, feature store

### Local Deployment with Docker Compose

Run all services:

```bash
docker compose --profile aide1 --profile aide2 --profile aide3 up -d
```

or:

```bash
make all-up
```

Run only MLOps services:

```bash
docker compose --profile aide1 up -d
```

Run only data platform services:

```bash
docker compose --profile aide2 up -d
```

Run AIDE 3 real-time services:

```bash
docker compose --profile aide3 up -d
```

Main endpoints:

- FastAPI: `http://localhost:8000`
- MLflow: `http://localhost:5000`
- Grafana: `http://localhost:3000`
- Prometheus: `http://localhost:9090`
- Jaeger: `http://localhost:16686`
- MinIO console: `http://localhost:9001`
- Airflow: `http://localhost:8084`
- Feast API: `http://localhost:6566`
- AIDE 3 API: `http://localhost:8088`
- AIDE 3 anomaly consumer: `http://localhost:8089`

### Local Kubernetes Deployment

Apply manifests:

```bash
make k8s-deploy
```

or:

```bash
kubectl apply -f k8s/
```

Deploy by Helm:

```bash
helm install ml-app helm/ml-app
helm install aide2-platform helm/aide2-platform
helm install aide3-platform helm/aide3-platform
```

### Cloud Deployment on GKE/GCE with Terraform

Provision infrastructure:

```bash
make terraform-init
make terraform-plan
make terraform-apply
```

Terraform includes resources for:

- GKE cluster and node pools
- GCE (MinIO host)
- GCS buckets (artifacts/lakehouse/registry/checkpoints/aide3-model-registry)
- Cloud Composer (Airflow)

## CI/CD Pipeline

### Continuous Integration and Testing

GitHub Actions runs on push/PR and includes:

- Linting and style checks
- Pytest execution
- Coverage validation (`>= 80%`)
- Build and security scan

### Continuous Deployment

Deployment flow:

1. Build/push container images (app and data services)
2. Trigger deployment stage (manual on protected branch)
3. Update Kubernetes workloads and run smoke checks

## Observability

The platform ships with all three pillars:

- **Metrics:** Prometheus + Grafana dashboards
- **Logs:** Loki + Promtail
- **Traces:** Jaeger

Optional model/data monitoring:

- Evidently drift reports (`monitoring/evidently_monitoring.py`)

## Tech Stack

- **Languages:** Python 3.11
- **ML/MLOps:** Scikit-learn, MLflow, DVC, KServe, FastAPI, Kubeflow Pipelines
- **Data Engineering:** Spark, Flink, Kafka, Trino, Airflow, Feast, Delta Lake
- **Eventing:** Knative Eventing
- **Storage:** MinIO, GCS, PostgreSQL
- **Observability:** Prometheus, Grafana, Loki, Jaeger
- **Infra/Platform:** Docker, Kubernetes, Helm, Terraform, GitHub Actions

---


