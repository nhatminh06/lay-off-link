.PHONY: help install test test-aide1 test-aide2 test-aide3 lint format docker-build docker-compose-up docker-compose-down k8s-deploy clean

help:
	@echo "AIDE Combined Project - Available Commands"
	@echo "============================================"
	@echo ""
	@echo "--- General ---"
	@echo "install              Install all Python dependencies"
	@echo "test                 Run all tests (AIDE 1 + AIDE 2)"
	@echo "lint                 Run linters"
	@echo "format               Format code with black"
	@echo "clean                Clean up temporary files"
	@echo ""
	@echo "--- AIDE 1: MLOps ---"
	@echo "test-aide1           Run AIDE 1 tests with coverage"
	@echo "train                Train ML model with MLflow"
	@echo "serve                Run FastAPI model server"
	@echo "aide1-up             Start AIDE 1 services (MLflow + ML App)"
	@echo ""
	@echo "--- AIDE 2: Data Engineering ---"
	@echo "test-aide2           Run AIDE 2 tests"
	@echo "ingest               Ingest NYC Taxi data to MinIO"
	@echo "spark-bronze         Run Spark Bronze ingestion"
	@echo "spark-silver         Run Spark Silver transformation"
	@echo "spark-gold           Run Spark Gold aggregation"
	@echo "feast-serve          Start Feast feature server"
	@echo "aide2-up             Start AIDE 2 services (MinIO, Kafka, Spark, ...)"
	@echo ""
	@echo "--- AIDE 3: Real-time MLOps ---"
	@echo "test-aide3           Run AIDE 3 tests"
	@echo "aide3-pipeline       Compile Kubeflow pipeline spec"
	@echo "aide3-serve          Run AIDE 3 FastAPI inference API"
	@echo "aide3-consumer       Run AIDE 3 anomaly event consumer"
	@echo "aide3-up             Start AIDE 3 services (Serving + Event Consumer)"
	@echo ""
	@echo "--- Infrastructure ---"
	@echo "all-up               Start ALL services (AIDE 1 + AIDE 2 + AIDE 3)"
	@echo "all-down             Stop all services"
	@echo "k8s-deploy           Deploy to Kubernetes"
	@echo "terraform-init       Initialize Terraform"
	@echo "terraform-plan       Plan Terraform changes"
	@echo "terraform-apply      Apply Terraform changes"

install:
	pip install -r requirements.txt
	pip install black flake8 pylint

# ============================================
# Tests
# ============================================

test: test-aide1 test-aide2 test-aide3

test-aide1:
	pytest aide1/tests/ -v --cov=aide1 --cov-report=html --cov-report=term

test-aide2:
	pytest aide2/tests/ -v --cov=aide2 --cov-report=term

test-aide3:
	pytest aide3/tests/ -v --cov=aide3 --cov-report=term

lint:
	flake8 aide1/ aide2/ aide3/ --count --max-line-length=127 --statistics
	pylint aide1/app.py aide1/api.py --fail-under=7.0

format:
	black aide1/ aide2/ aide3/

# ============================================
# AIDE 1: MLOps
# ============================================

train:
	cd aide1 && python app.py --n-estimators 100 --max-depth 5

train-experiment:
	@echo "Running multiple experiments..."
	cd aide1 && python app.py --n-estimators 50 --max-depth 3
	cd aide1 && python app.py --n-estimators 100 --max-depth 5
	cd aide1 && python app.py --n-estimators 200 --max-depth 10

serve:
	cd aide1 && uvicorn api:app --reload --host 0.0.0.0 --port 8000

mlflow-ui:
	mlflow ui --host 0.0.0.0 --port 5000

# ============================================
# AIDE 2: Data Engineering
# ============================================

ingest:
	python aide2/ingestion/ingest_nyc_taxi.py --from 2024-01 --to 2024-03

kafka-produce:
	python aide2/ingestion/kafka_producer.py --rate 10

spark-bronze:
	cd aide2 && spark-submit spark/bronze_ingestion.py

spark-silver:
	cd aide2 && spark-submit spark/silver_transform.py

spark-gold:
	cd aide2 && spark-submit spark/gold_aggregation.py

spark-pipeline: spark-bronze spark-silver spark-gold

feast-apply:
	cd aide2/feast && feast apply

feast-materialize:
	cd aide2/feast && feast materialize-incremental $$(date -u +"%Y-%m-%dT%H:%M:%S")

feast-serve:
	cd aide2/feast && uvicorn serve:app --reload --host 0.0.0.0 --port 6566

# ============================================
# Docker Compose (Profiles)
# ============================================

docker-build:
	docker build -t ml-app:latest -f aide1/Dockerfile aide1/
	docker build -t spark-jobs:latest -f aide2/Dockerfile.spark aide2/
	docker build -t flink-jobs:latest -f aide2/Dockerfile.flink aide2/
	docker build -t feast-server:latest aide2/feast/
	docker build -t aide3-serving:latest -f aide3/serving/Dockerfile .

aide1-up:
	docker compose --profile aide1 up -d
	@echo "AIDE 1 services started!"
	@echo "  MLflow:     http://localhost:5000"
	@echo "  ML App:     http://localhost:8000"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

aide2-up:
	docker compose --profile aide2 up -d
	@echo "AIDE 2 services started!"
	@echo "  MinIO:      http://localhost:9000 (minioadmin/minioadmin)"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Kafka:      localhost:29092"
	@echo "  Spark UI:   http://localhost:8081"
	@echo "  Flink UI:   http://localhost:8082"
	@echo "  Trino:      http://localhost:8083"
	@echo "  Airflow:    http://localhost:8084 (airflow/airflow)"
	@echo "  Feast:      http://localhost:6566"

aide3-pipeline:
	python -m aide3.kubeflow.pipeline --output aide3/kubeflow/pipeline.yaml

aide3-serve:
	uvicorn aide3.serving.api:app --reload --host 0.0.0.0 --port 8088

aide3-consumer:
	uvicorn aide3.knative.event_consumer:app --reload --host 0.0.0.0 --port 8089

aide3-up:
	docker compose --profile aide3 up -d
	@echo "AIDE 3 services started!"
	@echo "  AIDE3 API:         http://localhost:8088"
	@echo "  Anomaly Consumer:  http://localhost:8089"

all-up:
	docker compose --profile aide1 --profile aide2 --profile aide3 up -d
	@echo "All services started! (AIDE 1 + AIDE 2 + AIDE 3)"

all-down:
	docker compose --profile aide1 --profile aide2 --profile aide3 down -v

logs:
	docker compose logs -f

# ============================================
# Kubernetes
# ============================================

k8s-deploy:
	kubectl apply -f k8s/
	@echo "Waiting for deployments..."
	kubectl wait --for=condition=available --timeout=300s deployment/ml-app-deployment
	@echo "Deployments ready!"
	kubectl get pods
	kubectl get services

k8s-delete:
	kubectl delete -f k8s/

k8s-status:
	kubectl get all
	kubectl top nodes
	kubectl top pods

# ============================================
# Terraform
# ============================================

terraform-init:
	cd terraform && terraform init

terraform-plan:
	cd terraform && terraform plan

terraform-apply:
	cd terraform && terraform apply

terraform-destroy:
	cd terraform && terraform destroy

# ============================================
# Utilities
# ============================================

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "htmlcov" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	rm -rf build dist

setup-dev:
	@echo "Setting up development environment..."
	python -m venv venv
	. venv/bin/activate && pip install -r requirements.txt
	. venv/bin/activate && pip install black flake8 pylint
	@echo "Development environment ready!"
	@echo "Activate with: source venv/bin/activate"

demo-aide1:
	@echo "Starting AIDE 1 Demo..."
	@make aide1-up
	@sleep 10
	curl -X POST http://localhost:8000/predict -H "Content-Type: application/json" -d '{"features": [5.1, 3.5, 1.4, 0.2]}'
	@echo "\nAIDE 1 demo complete! Visit http://localhost:8000/docs"

demo-aide2:
	@echo "Starting AIDE 2 Demo..."
	@make aide2-up
	@sleep 30
	@echo "AIDE 2 services are running. Ingest data with: make ingest"
