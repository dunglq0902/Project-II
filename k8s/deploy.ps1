# k8s/deploy.ps1
# Master deployment script for Crypto Analytics Platform on K8s (Docker Desktop)
# Usage: .\k8s\deploy.ps1

$ErrorActionPreference = "Stop"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Crypto Analytics Platform - K8s Deploy " -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# ── Phase 0: Build Docker images locally ─────────────────────
Write-Host "[Phase 0/6] Building Docker images locally..." -ForegroundColor Yellow

$buildContext = (Get-Location).Path

Write-Host "  Building ingestion image..." -ForegroundColor Gray
docker build -t crypto-analytics/ingestion:latest -f docker/Dockerfile.ingestion $buildContext
if ($LASTEXITCODE -ne 0) { throw "Failed to build ingestion image" }

Write-Host "  Building alert-engine image..." -ForegroundColor Gray
docker build -t crypto-analytics/alert-engine:latest -f docker/Dockerfile.alert-engine $buildContext
if ($LASTEXITCODE -ne 0) { throw "Failed to build alert-engine image" }

Write-Host "  Building dashboard image..." -ForegroundColor Gray
docker build -t crypto-analytics/dashboard:latest -f docker/Dockerfile.dashboard $buildContext
if ($LASTEXITCODE -ne 0) { throw "Failed to build dashboard image" }

Write-Host "  Building spark image..." -ForegroundColor Gray
docker build -t crypto-analytics/spark:latest -f docker/Dockerfile.spark $buildContext
if ($LASTEXITCODE -ne 0) { throw "Failed to build spark image" }

Write-Host "[OK] All images built successfully" -ForegroundColor Green
Write-Host ""

# ── Phase 1: Namespace ────────────────────────────────────────
Write-Host "[Phase 1/7] Creating namespace..." -ForegroundColor Yellow
kubectl apply -f k8s/00-namespace.yaml
Write-Host "[OK] Namespace created" -ForegroundColor Green
Write-Host ""

# ── Phase 2: Secrets ─────────────────────────────────────────
Write-Host "[Phase 2/7] Creating secrets..." -ForegroundColor Yellow
kubectl apply -f k8s/01-secrets.yaml
Write-Host "[OK] Secrets created" -ForegroundColor Green
Write-Host ""

# ── Phase 3: Storage (StorageClass + PVCs) ───────────────────
Write-Host "[Phase 3/7] Creating storage resources (StorageClass + PVCs)..." -ForegroundColor Yellow
kubectl apply -f k8s/01a-storage.yaml
Write-Host "[OK] Storage resources created" -ForegroundColor Green
Write-Host ""

# ── Phase 4: Infrastructure ──────────────────────────────────
Write-Host "[Phase 4/7] Deploying infrastructure (Kafka - MongoDB - MinIO)..." -ForegroundColor Yellow
kubectl apply -f k8s/02-infra.yaml
Write-Host "Waiting for infrastructure to be ready..."

# Wait for Kafka
Write-Host "  Waiting for Kafka..." -ForegroundColor Gray
kubectl rollout status statefulset/kafka -n crypto-analytics --timeout=180s

# Wait for MongoDB
Write-Host "  Waiting for MongoDB..." -ForegroundColor Gray
kubectl rollout status statefulset/mongodb -n crypto-analytics --timeout=120s

# Wait for MinIO
Write-Host "  Waiting for MinIO..." -ForegroundColor Gray
kubectl rollout status statefulset/minio -n crypto-analytics --timeout=120s

Write-Host "[OK] Infrastructure ready" -ForegroundColor Green
Write-Host ""

# ── Phase 5: Init Jobs ───────────────────────────────────────
Write-Host "[Phase 5/7] Running init jobs (topics - buckets - seed)..." -ForegroundColor Yellow

# Delete old jobs if exist (jobs are immutable)
kubectl delete job kafka-init minio-init mongo-seed -n crypto-analytics --ignore-not-found 2>$null

kubectl apply -f k8s/03-init-jobs.yaml

# Wait for kafka-init
Write-Host "  Waiting for kafka-init job..." -ForegroundColor Gray
kubectl wait --for=condition=complete job/kafka-init -n crypto-analytics --timeout=120s 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Warning: kafka-init might still be running - checking..." -ForegroundColor Yellow
    Start-Sleep -Seconds 15
}

# Wait for minio-init
Write-Host "  Waiting for minio-init job..." -ForegroundColor Gray
kubectl wait --for=condition=complete job/minio-init -n crypto-analytics --timeout=120s 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Warning: minio-init might still be running" -ForegroundColor Yellow
}

# Wait for mongo-seed
Write-Host "  Waiting for mongo-seed job..." -ForegroundColor Gray
kubectl wait --for=condition=complete job/mongo-seed -n crypto-analytics --timeout=120s 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "  Warning: mongo-seed might still be running" -ForegroundColor Yellow
}

Write-Host "[OK] Init jobs completed" -ForegroundColor Green
Write-Host ""

# ── Phase 6: Spark ───────────────────────────────────────────
Write-Host "[Phase 6/7] Deploying Spark (Master + Worker + Streaming)..." -ForegroundColor Yellow
kubectl apply -f k8s/04-spark.yaml

Write-Host "  Waiting for Spark Master..." -ForegroundColor Gray
kubectl rollout status deployment/spark-master -n crypto-analytics --timeout=120s

Write-Host "  Waiting for Spark Worker..." -ForegroundColor Gray
kubectl rollout status deployment/spark-worker -n crypto-analytics --timeout=120s

Write-Host "[OK] Spark ready" -ForegroundColor Green
Write-Host ""

# ── Phase 7: Applications ────────────────────────────────────
Write-Host "[Phase 7/7] Deploying applications (Ingestion - Alerts - Dashboard)..." -ForegroundColor Yellow
kubectl apply -f k8s/05-apps.yaml

Write-Host "  Waiting for Alert API..." -ForegroundColor Gray
kubectl rollout status deployment/alert-api -n crypto-analytics --timeout=120s

Write-Host "  Waiting for Dashboard..." -ForegroundColor Gray
kubectl rollout status deployment/dashboard -n crypto-analytics --timeout=120s

Write-Host "  Waiting for Ingestion..." -ForegroundColor Gray
kubectl rollout status deployment/ingestion-service -n crypto-analytics --timeout=120s

Write-Host "[OK] Applications ready" -ForegroundColor Green
Write-Host ""

# ── Summary ───────────────────────────────────────────────────
Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Deployment Complete! " -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Resources:" -ForegroundColor Yellow
kubectl get all -n crypto-analytics
Write-Host ""
Write-Host "PVCs:" -ForegroundColor Yellow
kubectl get pvc -n crypto-analytics
Write-Host ""
Write-Host "Access:" -ForegroundColor Yellow
Write-Host "  Dashboard:    http://localhost:30501"
Write-Host "  Spark UI:     http://localhost:30080 (if NodePort configured)"
Write-Host "  MinIO Console: kubectl port-forward svc/minio 9001:9001 -n crypto-analytics"
Write-Host ""
Write-Host "Troubleshooting:" -ForegroundColor Yellow
Write-Host "  If NodePort does not work - use port-forward:"
Write-Host "  kubectl port-forward svc/dashboard 8501:8501 -n crypto-analytics"
Write-Host ""

