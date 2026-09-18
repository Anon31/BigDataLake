#!/bin/bash
set -eu
# Crée les buckets nécessaires dans MinIO (idempotent)
: "${MC_HOST_local:=http://minioadmin:minioadmin@minio:9000}"
mc alias set local "$MC_HOST_local" >/dev/null 2>&1 || true
for bucket in my-bucket warehouse; do
  if ! mc ls "local/$bucket" >/dev/null 2>&1; then
    echo "[setup-buckets] Création du bucket $bucket"
    mc mb "local/$bucket"
  else
    echo "[setup-buckets] Bucket $bucket déjà existant"
  fi
done
echo "[setup-buckets] Terminé ✔"