# Glossaire des technologies

Chaque brique du projet, en une page.

## 🪣 MinIO
Objet-store **compatible S3** auto-hébergé. Ici il fournit un vrai « data lake
objet » : `my-bucket` (parquet sources) et `warehouse` (données + métadonnées
Iceberg). API S3 complète (GET/PUT, presign) → n'importe quel client S3 fonctionne.

## 📄 Parquet
Format de **stockage colonnaire** : hautes performances en lecture/analyse
(une requête ne lit que les colonnes utiles), compression, intégré à Spark/DuckDB.
C'est le format physique *sous* Iceberg.

## 🦆 DuckDB
Moteur SQL **analytique in-process** (OLAP). Ici il lit les parquet de MinIO
par-dessus S3 et sert les agrégats du dashboard. Zéro serveur, intégré au
process Streamlit. Ligne clé : `CREATE SECRET` / `read_parquet([...], union_by_name=true)`.

## 🔐 AWS Signature v4
Protocole de **signature des requêtes S3**. La requête est canonisée (méthode,
URI, query triée, en-têtes), signée avec HMAC-SHA256 et la clé secrète. Meilleure
sécurité et surtout… la source historique des **403** du TP quand elle est mal
construite (région, style URL, horloge).

## 🎨 Streamlit
Framework Python pour dashboards : tout s'actualise à chaque interaction, code
court, widgets (`st.selectbox`, `st.multiselect`, `st.dataframe`, `st.chat_message`…).
Sert ici la couche de **visualisation** (port 8501).

## 📈 Altair
API de **graphiques déclaratifs** (Vega-Lite en Python) utilisée par Streamlit
(aire temporelle, barres, courbe de coût…).

## 🧊 Apache Iceberg 1.5.0
Format de **table transactionnelle sur data lake** ("open table format"). ACID,
snapshots versionnés (`snap-*.avro`), time travel, évolution de schéma,
catalogage. Runtime : `iceberg-spark-runtime-3.5_2.12-1.5.0.jar`.

## 🌳 Nessie 0.108.4
**Catalogue « git-like »** pour Iceberg : les métadonnées des tables sont
versionnées comme des commits, branches, merges (un « Git pour les données »).
API v2 REST sur :19120, store RocksDB persistant dans `nessie-data`.

## ⚡ Apache Spark 3.5.4
Moteur de **calcul distribué**. Standalone = master (:8080) + worker (:8081).
Exécute le job `iceberg-init` (PySpark) qui lit les parquet via S3A (`hadoop-aws`)
et écrit les tables Iceberg dans le catalogue Nessie.

## 🧩 plugin OpenCode
Module TS local (`.opencode/plugin/export-to-s3.ts`) branché sur l'événement
`session.idle` : il déclenche automatiquement l'export des conversations d'OpenCode
vers MinIO → la donnée arrive **toute seule** à chaque session.

## 🐳 Docker / docker compose
Conteneurisation de l'ensemble : un `docker-compose.yml` décrit 8 services,
le réseau, les volumes persistants (`minio-data`, `nessie-data`) et les
limitations mémoire. Le réseau interne est le point de friction : `minio:9000`
dans les conteneurs vs `localhost:9000` depuis l'hôte.

## 🦺 WSL2 (Windows)
SSH-sous-système Linux 2 : Docker Desktop tourne sous WSL2, on déclare la mémoire
dans `.wslconfig`. Sources de limites RAM rencontrées dans le TP.