#!/bin/bash

set -e

BUCKET="my-bucket"

echo "=== 1. Configuration du bucket (accès public anonyme) ==="
docker exec minio-s3 mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
docker exec minio-s3 mc mb local/${BUCKET} --ignore-existing
docker exec minio-s3 mc anonymous set public local/${BUCKET} >/dev/null

echo ""
echo "=== 2. POST (upload) : envoi d'un fichier via curl ==="
echo "Hello from BigDataLake!" > /tmp/test-upload.txt
curl -s -X PUT \
  -T /tmp/test-upload.txt \
  -H "Content-Type: text/plain" \
  "http://localhost:9000/${BUCKET}/test-upload.txt"
echo "Upload OK"

echo ""
echo "=== 3. GET (download) : récupération du fichier via curl ==="
curl -s -o /tmp/test-download.txt \
  "http://localhost:9000/${BUCKET}/test-upload.txt"
echo "Contenu reçu :"
cat /tmp/test-download.txt

echo ""
echo "=== 4. GET : liste des objets du bucket ==="
curl -s "http://localhost:9000/${BUCKET}/"