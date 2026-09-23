# Accueil

Bienvenue dans la documentation du TP **Big Data Lake** : analyse des conversations
d'un agent IA (OpenCode) à travers un mini data lake maison.

## Objectif

Reproduire un flux complet de **data engineering** sur une machine personnelle :

```
OpenCode (agent IA)  →  MinIO (objet S3)  →  DuckDB + Streamlit (analyse/visu)
                                       ↘  Spark + Nessie + Iceberg (tables transactionnelles)
```

## Ce que contient ce data lake

| Briques | Outils | Fichier clé |
|---|---|---|
| Stockage objet S3 | **MinIO** | `docker-compose.yml` |
| Visualisation web | **Streamlit + Altair** | `app.py` |
| SQL local sur S3 | **DuckDB** | `app.py` |
| Format de table transactionnel | **Apache Iceberg 1.5.0** | `scripts/iceberg_init.py` |
| Catalogue git-like | **Nessie 0.108.4** | `docker-compose.yml` |
| Moteur distribué | **Spark 3.5.4** | `Dockerfile.spark` |
| Ingestion | Python 3.10 + plugin OpenCode | `scripts/*.py`, `.opencode/plugin/` |

## Démarrage rapide

```bash
docker compose up -d                        # démarre toute la stack
docker compose up --build -d streamlit      # (re)construit l'app après modifications
python3.10 scripts/db_to_parquet.py         # ingest les conversations en parquet
open http://localhost:8501                  # dashboard Streamlit
docker compose run --rm iceberg-init        # crée les tables Iceberg
open http://localhost:3000                  # cette documentation
```

## Ports utiles

| Port | Service |
|---|---|
| 8501 | Dashboard Streamlit |
| 3000 | Documentation (docsify) |
| 9000 / 9001 | MinIO API / console |
| 19120 | Nessie (API v2) |
| 8080 / 8081 | Spark master / worker UI |

Identifiants S3/MinIO par défaut : `minioadmin` / `minioadmin`.
Buckets : `my-bucket` (données sources), `warehouse` (données Iceberg).

## Sommaire

- [📐 Architecture](architecture.md) — schéma, services Docker, flux de données
- [⚙️ Installation](installation.md) — prérequis et reproduction pas à pas
- [📥 Ingestion](ingestion.md) — logs, conversations, plugin OpenCode
- [📊 Dashboard](dashboard.md) — les 6 onglets de l'application Streamlit
- [🧊 Apache Iceberg](iceberg.md) — Spark + Nessie, et comment vérifier Iceberg
- [🔧 Dépannage](depannage.md) — les pièges qui font perdre du temps
- [📚 Glossaire](glossaire.md) — chaque technologie expliquée
- [📓 Journal de bord](journal.md) — tout ce qui a été fait, étape par étape
- [🚀 Roadmap](roadmap.md) — pistes d'extension