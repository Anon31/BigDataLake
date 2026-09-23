# Journal de bord

## Objectif

Construire un **mini data lake** autour des conversations d'OpenCode pour un TP :
ingestion, stockage objet, analyse SQL/visuelle et tables transactionnelles Iceberg.

## Étape 1 — Poser le socle : Mini data lake Docker

- **MinIO** (S3 local) + une app **Streamlit** qui lit des parquet de logs.
- Signature AWS **SigV4 écrite à la main** (sans boto3) pour avertir les 403 S3.
- Contrôle : lecture du parquet + 5 graphiques Altair (activité temporelle,
  niveaux de log, tokens par session, top sessions, coût cumulé).

### Piège rencontré 🔴
- **403 signature does not match** : la signature utilisait `cksum` et l'endpoint
  `localhost:9000` depuis le conteneur. Corrigé en construisant une vraie
  **AWS4-HMAC-SHA256** (région fixe `us-east-1`, style de chemin) et en passant
  par `minio:9000` dans le réseau Docker.

## Étape 2 — Ingester les conversations d'OpenCode

- `scripts/db_to_parquet.py` : **opencode.db** (SQLite : `session`, `message`,
  `part`) → `conversations/{parts,sessions}.parquet` dans MinIO.
  - copie temporaire de la base (lecture non-bloquante) ;
  - jointures `part → message → session` ;
  - extraction typée selon `part_type` (text, reasoning, tool, step-finish, patch).
- `scripts/log_to_parquet.py` : `opencode.log` → parquet (global ou par session).
- Dashboard réécrit en **6 onglets** : Vue d'ensemble, Timeline, Agent actions,
  Fichiers modifiés, Sessions, SQL explorer.
- Récupération de la base : grace au téléchargement direct (WSL2, `docker cp` +
  `mc cp`).

## Étape 3 — Spark + Nessie + Iceberg

- Ajout du service **Nessie** (catalogue, API v2, store RocksDB persistant).
- `Dockerfile.spark` : Spark 3.5.4 standalone + jars (à la construction) :
  `iceberg-spark-runtime-3.5_2.12-1.5.0.jar`, `hadoop-aws-3.3.4.jar`,
  `aws-java-sdk-bundle-1.12.262.jar`.
- Service `iceberg-init` (profil `iceberg`) exécute `iceberg_init.py` :
  parquet → **tables Iceberg** dans Nessie + warehouse MinIO.
- Résultats : `conversations.parts` **732 lignes**, `conversations.sessions` **6 lignes**.

### Vérifications de conformité
- Entries Nessie : tables typées `ICEBERG_TABLE` (marqueur fort).
- Warehouse : `metadata/*.metadata.json` + `snap-*.avro` (structure Iceberg).
- Runtime Iceberg effectivement embarqué dans les images Spark.

## Étape 4 — Export automatique (plugin)

- Plugin **OpenCode** `.opencode/plugin/export-to-s3.ts` branché sur
  `session.idle` : chaque session terminée → export logs + conversations vers MinIO.
- La donnée du TP s'ingère désormais **toute seule**.

## Étape 5 — Documentation web (docsify)

- Mise en place du site **docsify** (service `docs`, port 3000) : toute cette
  documentation Markdown devient un site navigable avec recherche plein-texte.

## Prochaines étapes possibles

cf. [Roadmap](roadmap.md) : requêtes Big Data sur Iceberg (time travel, POW),
nettoyage des données, CI, etc.