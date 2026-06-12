# k8s/teardown.ps1
# Xóa toàn bộ resources K8s của project
# Chạy: .\k8s\teardown.ps1
# Lưu ý: Mặc định giữ lại PVCs và PVs để bảo toàn dữ liệu

param(
    [switch]$DeleteData  # Thêm flag -DeleteData để xóa cả PVC/PV (mất dữ liệu!)
)

Write-Host "Tearing down Crypto Analytics Platform from K8s..." -ForegroundColor Red

# Delete application + compute resources
kubectl delete -f k8s/05-apps.yaml --ignore-not-found 2>$null
kubectl delete -f k8s/04-spark.yaml --ignore-not-found 2>$null
kubectl delete -f k8s/03-init-jobs.yaml --ignore-not-found 2>$null

# Delete StatefulSets nhưng KHÔNG xóa PVC
# (StatefulSets sẽ bị xóa, nhưng PVC được giữ lại bởi Retain policy)
kubectl delete -f k8s/02-infra.yaml --ignore-not-found 2>$null
kubectl delete -f k8s/01-secrets.yaml --ignore-not-found 2>$null

if ($DeleteData) {
    Write-Host "" 
    Write-Host "⚠  -DeleteData flag detected: DELETING ALL PVCs and data!" -ForegroundColor Red
    Write-Host "   Dữ liệu MongoDB, Kafka, MinIO sẽ bị xóa vĩnh viễn!" -ForegroundColor Red
    
    # Xóa PVCs
    kubectl delete pvc --all -n crypto-analytics --ignore-not-found 2>$null
    
    # Xóa PVs đã Released (dữ liệu từ Retain policy)
    $releasedPVs = kubectl get pv -o json 2>$null | ConvertFrom-Json
    if ($releasedPVs -and $releasedPVs.items) {
        foreach ($pv in $releasedPVs.items) {
            if ($pv.status.phase -eq "Released" -and $pv.spec.claimRef.namespace -eq "crypto-analytics") {
                Write-Host "  Deleting released PV: $($pv.metadata.name)" -ForegroundColor Yellow
                kubectl delete pv $pv.metadata.name --ignore-not-found 2>$null
            }
        }
    }
    
    # Xóa StorageClass
    kubectl delete -f k8s/01a-storage.yaml --ignore-not-found 2>$null
    
    Write-Host "✓ All data volumes deleted" -ForegroundColor Yellow
} else {
    Write-Host ""
    Write-Host "ℹ  PVCs và PVs được giữ lại để bảo toàn dữ liệu." -ForegroundColor Cyan
    Write-Host "   Để xóa cả dữ liệu, chạy: .\k8s\teardown.ps1 -DeleteData" -ForegroundColor Cyan
}

# Delete namespace
kubectl delete -f k8s/00-namespace.yaml --ignore-not-found 2>$null

Write-Host ""
Write-Host "✓ Teardown complete" -ForegroundColor Green
