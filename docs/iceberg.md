# Apache Iceberg : tables transactionnelles

## Pourquoi Iceberg sur ce TP

Les parquet bruts dans `my-bucket` sont **immutables et non versionnés**. Iceberg
apporte, sur un lakehouse : **transactions ACID**, **snapshots** (historique
versionné), **time travel**, **évolution de schéma** et un vrai catalogue.

La stack utilisée :

| Composant | Version / valeur |
|---|---|
| Moteur | Apache Spark 3.5.4 (standalone : master :8080, worker :8081) |
| Runtime Iceberg | `iceberg-spark-runtime-3.5_2.12-1.5.0.jar` |
| Catalogue | Nessie 0.108.4 (`org.apache.iceberg.nessie.NessieCatalog`) |
| Entrepôt | `s3://warehouse` (MinIO) |

## Le job `iceberg-init`

Défini dans `scripts/iceberg_init.py`, lancé avec :

```bash
docker compose run --rm iceberg-init
```

Étapes :

1. `createOrReplaceSparkSession()` — config du catalogue Nessie, branche
   default, `catalog.sql.enabled`, S3A (endpoint `http://minio:9000`,
   path-style, credentials minioadmin).
2. Lecture des parquet source via `spark.read.parquet("s3a://my-bucket/conversations/...")`.
3. Création des bases `conversations` et de **deux tables Iceberg** :

   ```sql
   CREATE OR REPLACE TABLE conversations.parts
   USING iceberg
   -- + INSERT / MERGE pour appliquer les nouvelles lignes
   ```

   Résultat obtenu avec les données du TP :
   - `conversations.parts` → **732 lignes**
   - `conversations.sessions` → **6 lignes**
4. Une requête de contrôle affiche quelques lignes et `SELECT count(*)`.

## Comment prouver qu'« Iceberg est vraiment utilisé »

Trois signes objectifs :

### 1. Le catalogue déclare des tables typées

```bash
curl -s "http://localhost:19120/api/v2/trees/main/entries?max-records=25" | python3 -m json.tool
```

Réponse (NeSSie v2) :

```json
{ "entries": [
    { "name": "conversations", "type": "NAMESPACE" },
    { "name": "parts",      "type": "ICEBERG_TABLE" },
    { "name": "sessions",   "type": "ICEBERG_TABLE" }
] }
```

Un simple entrepôt de parquet n'aurait **aucun** catalogue → c'est la signature
la plus forte.

### 2. L'organisation du warehouse

```bash
docker exec minio-s3 mc ls -r local/warehouse
```

Chaque table Iceberg possède :

```
warehouse/conversations.db/parts/          ou  .../parts/
├── data/                                  # fichiers parquet
└── metadata/
    ├── snap-*.avro                        # SNAPSHOTS versionnés
    ├── *.metadata.json                    # manifestes + schéma + historique
    └── ...
```

`metadata/*.metadata.json` + `snap-*.avro` n'existent **que** pour des tables
Iceberg (Spark/Hive n'écrivent que `data/*.parquet`).

### 3. Le runtime Iceberg dans les conteneurs

```bash
docker compose exec -T spark-master ls /opt/spark/jars/iceberg-*.jar
```

## Time travel avec Nessie

Nessie est un catalogue « git-like » : il garde des **commits** sur une branche.

```bash
curl -s http://localhost:19120/api/v2/trees/main/log | python3 -m json.tool
```

On peut montrer les références (`main`), revenir à un commit antérieur, ou
créer une branche expérimentale — la liste des commits du catalogue raconte
l'historique des `CREATE TABLE` et transactions.

## Le Dockerfile.spark

Construit l'image Spark et **télécharge à la construction** (pas au runtime) :

- `iceberg-spark-runtime-3.5_2.12-1.5.0.jar`
- `hadoop-aws-3.3.4.jar` + `aws-java-sdk-bundle-1.12.262.jar` (connecteur S3A)

Profil par défaut = `standalone` (daemons master/worker) ; profil `iceberg` =
lancer le job `iceberg_init.py`.

## Pour aller plus loin

- Time travel : `SELECT * FROM parts VERSION AS OF <snapshot-id>`
- Évolution de schéma : `ALTER TABLE ... ADD COLUMN`
- Branches Nessie : `CREATE BRANCH exp` puis requêter sur `http://nessie:19120`.