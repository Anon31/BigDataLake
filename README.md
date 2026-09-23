# TP Big Data Lake — Analyse des conversations d'un agent IA (OpenCode)

Ce TP construit un mini **data lake** complet sur machine personnelle pour analyser
les conversations et les logs d'un agent IA (OpenCode). On y retrouve les briques
classiques d'une architecture Big Data :

| Briques | Outils utilisés | Rôle |
|---|---|---|
| **Stockage objet (S3)** | MinIO | Conteneur des fichiers `parquet` (raw + silver) |
| **Catalogue de tables + format table** | Nessie + Apache Iceberg | Tables transactionnelles git-like, snapshots, branches |
| **Moteur de calcul distribué** | Apache Spark 3.5 (standalone) | Création / lecture des tables Iceberg |
| **SQL local** | DuckDB | Reads/agrégations rapides directement sur S3 |
| **Visualisation** | Streamlit + Altair | Dashboard web avec onglets |
| **Ingestion** | Scripts Python + plugin OpenCode | Export automatique des données en parquet |
| **Documentation** | docsify | Site web de doc (port 3000) servi par Docker |

> Le but pédagogique est de reproduire un flux complet : **source (agent IA) →
> objets (MinIO) → requêtes (DuckDB) → visuels (Streamlit) → table transactionnelle
> (Iceberg via Spark + Nessie)**, le tout en docker-compose.

---

## 📖 Documentation

Une **documentation web (docsify)** est embarquée dans le projet :

```bash
docker compose up -d docs     # puis ouvrir http://localhost:3000
```

Les sources Markdown sont dans [`docs/`](docs/) (architecture, installation,
ingestion, dashboard, Iceberg, dépannage, glossaire, journal, roadmap).

---

## 1. Architecture

```
                          ~/.local/share/opencode/
                          ├─ log/opencode.log        (logs structurés key=value)
                          └─ opencode.db             (SQLite: session/message/part)

        [scripts] python3.10 log_to_parquet.py / db_to_parquet.py
                          (ou plugin .opencode automatique)
                                  │  (docker cp + mc cp)
                                  ▼
                        ┌─────────────────────┐
                        │   MinIO (s3://)     │ 9000 API / 9001 console
                        │  my-bucket          │   opencode-logs.parquet
                        │  conversations/*    │   parts.parquet, sessions.parquet
                        │  warehouse/ (iceberg)│  données + métadonnées Iceberg
                        └──────────┬──────────┘
                                   │ http://minio:9000 (réseau Docker)
                      ┌────────────┴─────────────┐
                      ▼                          ▼
              DuckDB (CREATE SECRET s3)   Spark 3.5 (s3a://)
              + Streamlit app             + Nessie catalog
                   :8501                        :19120 (+ master :8080, worker :8081)
```

### Services docker (docker-compose.yml)

| Service | Image | Port(s) | Rôle |
|---|---|---|---|
| `minio` (`minio-s3`) | quay.io/minio/minio | 9000 (API), 9001 (console) | Stockage objet S3 |
| `streamlit` (`streamlit-app`) | build local (`Dockerfile`) | 8501 | Dashboard web |
| `nessie` | ghcr.io/projectnessie/nessie:0.108.4 | 19120 | Catalogue git-like (store RocksDB persistant) |
| `spark-master` | build local (`Dockerfile.spark`) | 7077 (RPC), 8080 (UI) | Master standalone |
| `spark-worker` | build local (`Dockerfile.spark`) | 8081 (UI) | Worker standalone |
| `spark-sql-server` | build local (`Dockerfile.spark`) | 9100 (HTTP) | API SQL : SparkSession persistante (catalogue `iceberg`) pour l'onglet Streamlit |
| `minio-setup` | quay.io/minio/minio | — (one-shot) | Crée les buckets `my-bucket` et `warehouse` |
| `iceberg-init` | build local (`Dockerfile.spark`) | — (job, profil `iceberg`) | Crée les tables Iceberg depuis les parquet |

### Identifiants & valeurs par défaut

- S3/MinIO : `minioadmin` / `minioadmin`
- Buckets : `my-bucket` (données sources), `warehouse` (données Iceberg)
- Endpoint **interne** Docker : `http://minio:9000` (à utiliser dans les conteneurs)
- Endpoint **externe** : `http://localhost:9000` (console : `http://localhost:9001`)

---

## 2. Prérequis

- **Docker** (Desktop recommandé sous Windows/macOS, engine seul sous Linux).
  Sous Windows, l'environnement est **WSL2** — voir la section « Point mémoire » §7.
- **Python 3.10+** sur l'hôte avec `pandas` et `pyarrow` (pour les scripts d'export).
  Sur la machine de dev : `python3.10` disposait de pandas 2.3.3 + pyarrow 25.0.1.
- **OpenCode** (pour récupérer vos vraies conversations) — optionnel si l'on veut
  tester sans données issues de l'agent.
- Environ **≥ 4 Go de RAM recommandé 8 Go** pour la stack complète (voir §7).

---

## 3. Arborescence du projet

```
BigDataLake/
├── docker-compose.yml        # toute la stack
├── Dockerfile                # image Streamlit (python:3.11-slim + streamlit/duckdb/requests)
├── Dockerfile.spark          # image Spark 3.5.4 + jars Iceberg 1.5.0 + hadoop-aws
├── requirements.txt          # streamlit, duckdb, requests
├── app.py                    # dashboard Streamlit (7 onglets)
├── scripts/
│   ├── log_to_parquet.py     # opencode.log → s3://my-bucket/opencode-logs.parquet
│   ├── db_to_parquet.py      # opencode.db  → s3://my-bucket/conversations/{parts,sessions}.parquet
│   ├── setup-buckets.sh      # création des buckets (one-shot)
│   ├── iceberg_init.py       # job PySpark : parquet → tables Iceberg (catalogue nessie→iceberg)
│   └── spark_sql_server.py   # API HTTP : SparkSession persistante → requêtes SQL (onglet Spark/Iceberg)
└── .opencode/plugin/
    └── export-to-s3.ts       # export automatique à chaque session OpenCode terminée
```

---

## 4. Reproduction pas à pas

### Étape 1 — Préparer les fichiers

L'ensemble des fichiers nécessaires se trouve dans ce dépôt. Vérifier que la
structure du §3 est bien présente. Sur Linux, s'assurer que `python3.10` dispose
de `pandas` et `pyarrow` :

```bash
python3.10 -c "import pandas, pyarrow; print(pandas.__version__, pyarrow.__version__)"
```

Si absent : `python3.10 -m pip install pandas pyarrow`.

### Étape 2 — Démarrer la stack

```bash
docker compose up -d              # minio + streamlit + nessie + spark master/worker + minio-setup
docker compose ps                 # vérifier que tout est "Up"
```

Le service `minio-setup` s'exécute une fois et crée les buckets `my-bucket` et
`warehouse` (idempotent). Vérifier :

```bash
docker logs minio-setup           # doit afficher "[setup-buckets] Terminé ✔"
curl -s http://localhost:19120/api/v2/config   # réponse JSON = Nessie OK
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/   # 200 = Spark master OK
```

⚠️ Sur une machine de test, MinIO peut mettre quelques secondes avant d'être prêt
ou le port 9001/8080 peut être occupé : adapter les ports si besoin.

### Étape 3 — Ingerer les données

Les scripts d'export lisent les fichiers produits par OpenCode (*) puis les
déposent dans MinIO via `docker cp` + `mc cp` (ils s'appuient sur le nom de
conteneur `minio-s3`).

```bash
# 3a. logs (événements : session.start, session.idle, ...)
python3.10 scripts/log_to_parquet.py
#    → s3://my-bucket/opencode-logs.parquet

# 3b. conversations (messages + parts : texte, raisonnement, appels d'outils, patches)
python3.10 scripts/db_to_parquet.py
#    → s3://my-bucket/conversations/parts.parquet
#    → s3://my-bucket/conversations/sessions.parquet
```

(*) Sources des données sur Linux :
- `~/.local/share/opencode/log/opencode.log` (logs key=value)
- `~/.local/share/opencode/opencode.db` (SQLite : tables `session`, `message`, `part`)

> Les exports sont aussi déclenchés automatiquement à la fin de chaque session
> OpenCode grâce au **plugin** (voir Étape 5).

### Étape 4 — Visualiser (Streamlit)

Après un rebuild pour prendre en compte `app.py` :

```bash
docker compose up --build -d streamlit
```

Ouvrir **http://localhost:8501**. Le dashboard proposé par `app.py` :

1. **Vue d'ensemble** — charge les parquet de logs sélectionnés via DuckDB
   (`read_parquet([...], union_by_name=true, filename=true)`) et affiche 5 graphiques
   Altair (activité temporelle, niveaux de log, tokens/session, top sessions, coût cumulé).
2. **Timeline** — chronologie d'une conversation : textes utilisateur/assistant,
   raisonnement, appels d'outils (input/output), fins de step, fichiers modifiés.
3. **Agent actions** — tous les appels d'outils, filtrables par outil et par session.
4. **Fichiers modifies** — les fichiers touchés par l'agent (patchs) par session.
5. **Sessions** — tableau de bord par session (nombre de parts, d'outils, tokens, coût).
6. **SQL explorer** — requête libre DuckDB sur les tables `parts`, `sessions`, `logs`.
7. **Spark SQL / Iceberg** — requête SQL libre exécutée par le cluster Spark sur
   les tables Iceberg (`iceberg.opencode.parts`, `iceberg.opencode.sessions`).

### Étape 5 — Export automatique (plugin OpenCode)

Le plugin (actif dans ce répertoire) déclenche les exports à chaque fin de session
(`session.idle`) :

```bash
python3.10 scripts/log_to_parquet.py --session <SESSION_ID>   # export unitaire logs
python3.10 scripts/db_to_parquet.py                           # export complet conversations
```

Cela garantit que MinIO est toujours à jour après l'utilisation d'OpenCode.
(Sur un PC personnel, soit conserver ce dossier comme répertoire de travail
d'OpenCode, soit copier ces scripts et le plugin dans son propre espace.)

### Étape 6 — Apache Iceberg (Spark + Nessie)

Les tables `opencode.parts` et `opencode.sessions` sont créées dans le
catalogue Nessie (enregistré sous le nom **`iceberg`** dans Spark) en lisant les
parquet du bucket `my-bucket` :

```bash
docker compose run --rm iceberg-init
```

Ce job PySpark (mode client, master `spark://spark-master:7077`) :
1. attend que Nessie soit prêt (`GET /api/v2/config`) ;
2. crée le namespace `iceberg.opencode` ;
3. lit `s3a://my-bucket/conversations/*.parquet` (S3A) ;
4. exécute `CREATE TABLE IF NOT EXISTS iceberg.opencode.parts USING iceberg AS SELECT ...`
   → idempotent (ne fait rien si la table existe) ;
5. affiche les tables et les **snapshots** Iceberg.

Résultat attendu (sur les données du TP) :
- `iceberg.opencode.parts` → **732 lignes**
- `iceberg.opencode.sessions` → **6 lignes**

Vérifications :

```bash
# catalogue Nessie
curl -s "http://localhost:19120/api/v2/trees/main/entries?max-records=25" | python3 -m json.tool
#    → 3 entries : NAMESPACE opencode, ICEBERG_TABLE parts, ICEBERG_TABLE sessions

# warehouse (fichiers Iceberg : data + metadata.json + snapshots avro)
docker exec minio-s3 mc ls -r local/warehouse
```

Exploration Spark SQL (une table Iceberg dans Nessie, branche `main`) :

```sql
SHOW TABLES IN iceberg.opencode;
SELECT count(*) FROM iceberg.opencode.parts;
SELECT part_type, count(*) FROM iceberg.opencode.parts GROUP BY 1 ORDER BY 2 DESC;
SELECT * FROM iceberg.opencode.parts.system.snapshots;
-- branches git-like Nessie :
--   CALL iceberg.system.create_branch('exp', 'main');  (nécessite l'extension Nessie Spark)
```

### Étape 7 — Requêtes Spark SQL depuis le dashboard (onglet « Spark SQL / Iceberg »)

Un service `spark-sql-server` garde une SparkSession persistante (catalogue
`iceberg`) et expose une API HTTP sur le port **9100**. L'onglet « Spark SQL /
Iceberg » de Streamlit soumet des requêtes SQL libres, exécutées par le
cluster Spark sur les tables Iceberg :

```bash
docker compose up -d spark-sql-server     # démarre l'API (~30 s de démarrage)

# test unitaire de l'API :
curl -s http://localhost:9100/health
curl -s -X POST http://localhost:9100/query -H "Content-Type: application/json" \
     -d '{"sql": "SELECT * FROM iceberg.opencode.parts LIMIT 5"}'
```

Dans Streamlit, on peut alors taper par exemple :

```sql
SELECT * FROM iceberg.opencode.parts LIMIT 20
SELECT part_type, count(*) AS n FROM iceberg.opencode.parts GROUP BY 1 ORDER BY 2 DESC
SELECT * FROM iceberg.opencode.parts.system.snapshots
SHOW TABLES IN iceberg.opencode
```

---

## 5. Journal de bord du TP (ce qui a été fait, étape par étape)

1. **Service Streamlit + MinIO** : `docker-compose.yml` avec MinIO (port 9000/9001)
   et une app Streamlit construite par `Dockerfile` (python:3.11-slim,
   `requirements.txt` : `streamlit`, `duckdb`, `requests`). `app.py` se connecte à
   MinIO via l'endpoint **interne** `http://minio:9000`.

2. **403 d'authentification S3 → signature AWS SigV4** : MinIO refuse les requêtes
   non signées. Implémentation à la main de **AWS Signature V4** dans `app.py` :
   `canonical request` (méthode, URI encodée, query string triée), headers signés
   (`x-amz-date`, `x-amz-content-sha256` avec `EMPTY_SHA256`), chaîne à signer,
   `Credential=<AK>/<date>/<region>/s3/aws4_request`. Bug rencontré : la query
   string et l'URI doivent être **canonicalisées/encodées** correctement, sinon 403.

3. **DuckDB + Parquet** : `CREATE SECRET (TYPE s3, KEY_ID ..., SECRET ...,
   ENDPOINT 'minio:9000', REGION 'us-east-1', USE_SSL false, URL_STYLE 'path')`,
   puis `SELECT * FROM read_parquet('s3://my-bucket/...')`. Erreur de schéma
   corrigée avec **`union_by_name=true`** (les parquet de logs n'ont pas exactement
   les mêmes colonnes).

4. **Premier dashboard** : 5 graphiques Altair (activité par heure, niveaux de log,
   tokens par session, top sessions, coût cumulé). Constat : le champ `cost` est
   vide dans les logs (le coût est renseigné au niveau des parts, pas des logs).

5. **Export des conversations (opencode.db)** : découverte que les conversations
   réelles de l'agent sont dans `~/.local/share/opencode/opencode.db` (SQLite —
   tables `session`, `message`, `part`), pas dans `opencode.log` (qui ne contient
   que des événements de cycle de vie). Création de `scripts/db_to_parquet.py` :
   copie de la base en lecture seule (sécurité/WAL), jointure part→message→session,
   extraction des champs selon le type (`text`, `reasoning`, `tool`, `step-finish`,
   `patch`), sérialisation JSON des entrées/sorties d'outils, puis upload via
   `docker cp` + `mc cp local/<bucket>/<key>`.

6. **Dashboard conversationnel** : rewrite de `app.py` avec **6 onglets**
   (Vue d'ensemble, Timeline, Agent actions, Fichiers modifies, Sessions,
   SQL explorer), `st.chat_message`, `st.expander`, `json.loads` sur les JSON
   stockés en texte, et registre DuckDB des DataFrames (`con.register`).

7. **Plugin OpenCode** : `.opencode/plugin/export-to-s3.ts` écoute l'événement
   `session.idle` et lance les deux scripts d'export (`--session` pour les logs,
   full pour les conversations). Simple plugin TypeScript avec `$` (Bun shell).

8. **Apache Iceberg (choix : Spark + Nessie)** :
   - recherche de compatibilité → combinaison retenue : **Spark 3.5.4 + Iceberg
     1.5.0** (`iceberg-spark-runtime-3.5_2.12:1.5.0`) **+ Nessie 0.108.4**
     (Nessie 0.108.4 ↔ Iceberg 1.5 ↔ Spark 3.3/3.4/3.5, table de compat officielle) ;
   - `Dockerfile.spark` : `apache/spark:3.5.4` + `python3` + les jars Iceberg,
     **`hadoop-aws-3.3.4`** et **`aws-java-sdk-bundle-1.12.262`** (l'image Spark
     officielle n'embarque PAS hadoop-aws / S3A) ;
   - services `nessie` (store **RocksDB** persistant) + `spark-master` +
     `spark-worker` + job `iceberg-init` ; service `minio-setup` qui crée le
     bucket `warehouse` (manquant, d'où le `NoSuchBucket` initial) ;
   - premier job : `UnsupportedFileSystemException: No FileSystem for scheme "s3"`
     → ajout de `fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem` (le catalogue
     Iceberg écrit en `s3://warehouse/` via HadoopFileIO, alors que l'image n'a
     enregistré que `s3a://`) ;
   - résultats vérifiés : namespace + 2 tables `ICEBERG_TABLE` dans Nessie,
     fichiers data + metadata/snapshots dans `s3://warehouse/`, lecture Spark OK.

9. **Bornage mémoire (WSL2)** : sur la machine de dev (VM WSL2 limitée à ~3,8 Go),
   la stack complète provoquait de l'**OOM** (Nessie/Quarkus montait à 80 % de la
   RAM par défaut). Correctifs appliqués dans le compose : `mem_limit` par service,
   `JAVA_OPTS_APPEND="-Xms256m -Xmx768m"` pour Nessie, `SPARK_DAEMON_MEMORY=640m`
   pour master/worker, `spark.driver.memory=512m` (Spark refuse < 450 Mo),
   executor 768m / 1 core. Recommandation : **8 Go** de RAM à la maison.

10. **Requêtes SQL sur Iceberg depuis Streamlit (onglet « Spark SQL / Iceberg »)** :
    - le catalogue Spark est enregistré sous le nom **`iceberg`** (base **`opencode`**,
      tables `parts`/`sessions`) — le nommage interne Nessie reste inchangé ;
    - `scripts/spark_sql_server.py` : mini API HTTP (stdlib `http.server`) qui garde
      une SparkSession persistante (catalogue `iceberg`, même config S3A que
      `iceberg_init.py`), expose `GET /health` (liste les tables) et
      `POST /query` (exécute le SQL, renvoie colonnes + lignes, cap 2000 rows) ;
    - service `spark-sql-server` (même `Dockerfile.spark`, port **9100**, `restart:
      unless-stopped`) ; l'onglet Streamlit fait un simple `requests.post` ;
    - détail honnête : `rows_affected` est calculé par un `df.count()` (action Spark
      supplémentaire) ; la première requête est lente (~30 s de démarrage du driver).

---

## 6. Commandes utiles

```bash
docker compose up -d --build           # démarrer / reconstruire toute la stack
docker compose logs -f streamlit       # logs de l'app
docker compose ps                      # état des services
docker compose run --rm iceberg-init   # (re)créer les tables Iceberg (idempotent)
docker compose up -d spark-sql-server  # API SQL Spark (onglet Iceberg du dashboard)

# Spark SQL (API du spark-sql-server, port 9100)
curl -s http://localhost:9100/health
curl -s -X POST http://localhost:9100/query -H "Content-Type: application/json" \
     -d '{"sql": "SELECT count(*) AS n FROM iceberg.opencode.parts"}'

# données
python3.10 scripts/log_to_parquet.py
python3.10 scripts/db_to_parquet.py
python3.10 scripts/db_to_parquet.py --session <SESSION_ID>   # export ciblé (log_to_parquet aussi)

# inspecter MinIO
docker exec minio-s3 mc ls -r local/my-bucket
docker exec minio-s3 mc ls -r local/warehouse

# arrêt propre
docker compose down                    # garde les volumes (données)
docker compose down -v                 # supprime aussi les volumes (reset complet !)
```

### Ports

| Port | Service | Adresse |
|---|---|---|
| 8501 | Streamlit | http://localhost:8501 |
| 9000 | MinIO API | http://localhost:9000 |
| 9001 | MinIO console | http://localhost:9001 |
| 19120 | Nessie (API v2) | http://localhost:19120/api/v2 |
| 7077 | Spark master RPC | — |
| 8080 | Spark master UI | http://localhost:8080 |
| 8081 | Spark worker UI | http://localhost:8081 |
| 9100 | Spark SQL API | http://localhost:9100/health |

---

## 7. Points d'attention / « ça mord quand tu ne sais pas »

- **Endpoint interne vs externe** : dans les conteneurs (DuckDB, Spark), toujours
  `http://minio:9000`. Depuis l'hôte, `http://localhost:9000`. 
- **MinIO impose la signature AWS SigV4** : requêtes non signées → `AccessDenied`.
  La signature est faite « à la main » dans `app.py` (pas de `boto3` nécessaire).
- **`union_by_name=true`** : indispensable avec des parquet aux schémas hétérogènes
  (logs) ; sinon erreur de « schema mismatch ».
- **`use_container_width` supprimé dans Streamlit ≥ 2026** : utiliser
  `width="stretch"` (une migration a été faite dans `app.py`).
- **hadoop-aws n'est PAS dans `apache/spark`** : sans les jars S3A, `s3a://` et `s3://`
  échouent. Et `fs.s3.impl` doit être câblé sur `S3AFileSystem` pour le schéma `s3://`
  utilisé par le catalogue Iceberg.
- **La RAM** : Nessie (Quarkus) par défaut consomme ~80 % de la RAM de la machine ;
  Spark aussi. Le compose borne tout (`mem_limit`, `-Xmx`). Pour un usage confortable :
  `memory=8GB` dans `C:\Users\<vous>\.wslconfig` (Windows) :
  ```ini
  [wsl2]
  memory=8GB
  ```
- **Persistance** : les données MinIO et le store RocksDB de Nessie sont dans des
  **volumes Docker** (`minio-data`, `nessie-data`) → survivent à `docker compose down`
  mais pas à `down -v`.
- **Nessie : `user: "0:0"`** est nécessaire parce que le volume est créé root par
  compose alors que l'image tourne en uid 65534 (`nobody`) — sinon erreur
  `Permission denied` sur le répertoire RocksDB.

---

## 8. Pistes d'extension

- Branches / tags Nessie : `CALL iceberg.system.create_branch('experiment', 'main')`
  puis lecture `??iceberg...` — ou l'extension `nessie-spark-extensions`.
- `iceberg-init` enrichi pour d'autres formats (données en silo, partitionnement
  Iceberg par date (`PARTITIONED BY (year(dt))`)).
- Déplacer l'export des logs vers le pipeline Iceberg (table `logs`).
- Un moteur type **Trino** pour requêter Iceberg en SQL portable (autre bonne
  extension du TP).