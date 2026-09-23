# Architecture

## Vue d'ensemble

```
                    ~/.local/share/opencode/
                    ├─ log/opencode.log        (logs structurés key=value)
                    └─ opencode.db             (SQLite : session / message / part)

   [scripts] python3.10 log_to_parquet.py / db_to_parquet.py
                        (ou plugin OpenCode automatique à chaque session.idle)
                                │  (docker cp + mc cp)
                                ▼
                      ┌─────────────────────┐
                      │  MinIO (S3)          │  9000 API / 9001 console
                      │  my-bucket          │   opencode-logs.parquet
                      │  conversations/*    │   parts.parquet, sessions.parquet
                      │  warehouse/         │   données + métadonnées Iceberg
                      └──────────┬──────────┘
                                 │ http://minio:9000 (réseau Docker)
                    ┌────────────┴─────────────┐
                    ▼                          ▼
            DuckDB + Streamlit          Spark 3.5 (S3A)
            (dashboard :8501)              + Nessie catalog
                                        (tables Iceberg)
```

## Services Docker

Tout est déclaré dans `docker-compose.yml`.

| Service | Image | Ports | Rôle |
|---|---|---|---|
| `minio` | `quay.io/minio/minio` | 9000, 9001 | Stockage objet S3 |
| `streamlit` | build local (`Dockerfile`) | 8501 | Dashboard web |
| `nessie` | `ghcr.io/projectnessie/nessie:0.108.4` | 19120 | Catalogue git-like, store RocksDB persistant |
| `spark-master` | build local (`Dockerfile.spark`) | 7077, 8080 | Master Spark standalone |
| `spark-worker` | build local (`Dockerfile.spark`) | 8081 | Worker Spark standalone |
| `minio-setup` | `quay.io/minio/minio` | — | One-shot : crée les buckets |
| `iceberg-init` | build local (`Dockerfile.spark`) | — | Job : parquet → tables Iceberg (profil `iceberg`) |
| `docs` | `ghcr.io/rockbenben/docsify-server` | 3000 | Site de documentation (pages en Markdown) |

## Le réseau Docker : deux « mondes »

- Les conteneurs se parlent via le **réseau composé par docker-compose** en
  utilisant les **noms de services** : `http://minio:9000`, `http://nessie:19120`.
- Depuis l'hôte (navigateur, scripts), on passe par les **ports publiés** :
  `http://localhost:9000`, `http://localhost:8501`, etc.

> ⚠️ Ne pas confondre les deux : c'est LA source de la plupart des erreurs 403
> ou « connexion refusée » (voir [Dépannage](depannage.md)).

## Flux de données

1. **Ingestion** : les scripts Python lisent les fichiers d'OpenCode sur l'hôte,
   écrivent un parquet temporaire, puis l'uploent dans MinIO via
   `docker cp` + `mc cp`.
2. **Exploration** : `app.py` liste les parquet (requête signée AWS SigV4), les
   lit via DuckDB (`read_parquet`), et construit les graphiques Altair.
3. **Big Data** : le job PySpark lit les parquet source via `s3a://` et crée des
   **tables Iceberg** (format sourcé dans `metadata.json`, snapshots versionnés)
   dans le catalogue **Nessie**, données stockées dans `s3://warehouse/`.

## Volumes & persistance

- `minio-data` : conteneur S3 (survit à `docker compose down`).
- `nessie-data` : store RocksDB de Nessie (métadonnées du catalogue).
- `docker compose down -v` supprime tout → **reset complet**.

## Mémoire

Le `docker-compose.yml` borne l'usage RAM de chaque service (`mem_limit`) et de
chaque JVM (Nessie `-Xmx768m`, Spark daemons `640m`, driver/executor ~512–768m).
Voir [Dépannage → Mémoire](depannage.md#mémoire--wsl2).