#!/bin/bash
set -e

echo "=== ML DevOps Application Setup ==="

echo "[1/4] Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker first."
    exit 1
fi

echo "[2/4] Building and starting services..."
docker-compose up --build -d

echo "[3/4] Waiting for services to start..."
sleep 15

echo "[4/4] Checking service health..."
echo "  MLflow:     http://localhost:5000"
echo "  ML App:     http://localhost:8000"
echo "  Grafana:    http://localhost:3000"
echo "  Prometheus: http://localhost:9090"
echo "  Jaeger:     http://localhost:16686"

curl -sf http://localhost:8000/health > /dev/null && echo "  ML App:    OK" || echo "  ML App:    STARTING..."
curl -sf http://localhost:9090/-/healthy > /dev/null && echo "  Prometheus: OK" || echo "  Prometheus: STARTING..."

echo ""
echo "=== All services started! ==="
echo "Run 'docker-compose logs -f' to view logs"
