# Run Guide - ML DevOps Application

## Prerequisites
- Docker & Docker Compose
- Python 3.11+
- kubectl (for K8s deployment)

## Quick Start (Local Docker)

```bash
# 1. Start all services
docker-compose up --build -d

# 2. Check service health
curl http://localhost:8000/health

# 3. Train the model
docker exec ml-application python app.py

# 4. Make predictions
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [5.1, 3.5, 1.4, 0.2]}'
```

## Services & URLs

| Service | URL | Description |
|---------|-----|-------------|
| ML App API | http://localhost:8000 | FastAPI endpoints |
| MLflow UI | http://localhost:5000 | Experiment tracking |
| Grafana | http://localhost:3000 | Monitoring dashboards |
| Prometheus | http://localhost:9090 | Metrics collection |
| Jaeger UI | http://localhost:16686 | Distributed tracing |
| cAdvisor | http://localhost:8080 | Container metrics |

## API Endpoints

- `GET /` - API info
- `GET /health` - Health check
- `POST /predict` - Make predictions
- `GET /metrics` - Prometheus metrics
- `GET /info` - Model metadata
- `GET /docs` - OpenAPI docs (Swagger)

## Running Tests

```bash
# All tests with coverage
pytest tests/ -v --cov=. --cov-report=html --cov-fail-under=80

# Just unit tests
pytest tests/test_api.py -v

# Just integration tests
pytest tests/test_integration.py -v
```

## Kubernetes Deployment

```bash
# Apply manifests
kubectl apply -f k8s/

# Check status
kubectl get pods -l app=ml-app
kubectl get svc

# Helm deployment
helm install ml-app helm/ml-app/
```

## Monitoring

1. Open Grafana at http://localhost:3000 (admin/admin)
2. Dashboards are auto-provisioned
3. Prometheus scrapes metrics every 15s
4. Loki collects container logs via Promtail
5. Jaeger traces at http://localhost:16686

## Stopping Services

```bash
docker-compose down        # Keep volumes
docker-compose down -v     # Remove volumes too
```
