#!/bin/bash
set -e

echo "Starting deployment..."

if [ ! -f "drive-audit-service.json" ]; then
    echo "Error: drive-audit-service.json not found!"
    exit 1
fi

if [ ! -f ".env" ]; then
    echo "Creating .env from template..."
    cp .env.example .env
    echo "Please edit .env file before running again."
    exit 1
fi

source .env

if [ -z "$API_BEARER_TOKEN" ]; then
    echo "Error: API_BEARER_TOKEN not set"
    exit 1
fi

mkdir -p downloads

echo "Checking port 8000..."
# Stop and remove any existing containers
docker compose down 2>/dev/null || true
docker container prune -f

# Kill any process using port 8000
sudo fuser -k 8000/tcp 2>/dev/null || true
sudo ss -tlnp | grep :8000 | awk '{print $6}' | grep -o 'pid=[0-9]*' | cut -d'=' -f2 | xargs -r sudo kill -9 2>/dev/null || true

echo "Building Docker image..."
docker build -t drive-audit-api .

echo "Starting services..."
docker compose up -d

echo "Waiting for service..."
sleep 10

if curl -f http://localhost:8001/health > /dev/null 2>&1; then
    echo "Deployment successful! API running at http://localhost:8001"
else
    echo "Health check failed. Check logs: docker compose logs"
    exit 1
fi