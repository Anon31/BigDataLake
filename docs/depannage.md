# Dépannage & pièges

Tous les écueils rencontrés pendant le TP, avec la solution. À lire avant
d'appeler à l'aide 🙃

## Le réseau Docker : « [Errno 111] Connection refused »

**Symptôme** : l'app Streamlit ne charge pas les parquet / paquets perdu dans
`get_object` ; log `[Errno 111] Connection refused`.

**Cause** : depuis un conteneur, on doit utiliser les **noms de services**
(`minio:9000`), pas `localhost` (qui désigne le conteneur lui-même). L'inverse
depuis l'hôte.

**Solution** :
- endpooint DuckDB / Spark coté conteneur : `http://minio:9000` ;
- depuis une commande hôte : `http://localhost:9000`.

## HTTP 403 signature does not match

**Symptôme** : `boto3`/DuckDB/Spark renvoient des **403 AccessDenied** malgré de
bonnes credentials.

**Cause** : signature AWS SigV4 incorrecte (mauvaise **région**, mauvais **style
d'URL**, cloches horloge, ou chaîne signée mal construite).

**Solution** (app `app.py`) : la requête est signée « à la main » avec AWS SigV4 ;
utiliser une **région fixe** (`us-east-1`), un **style de chemin** (`path`) et
l'en-tête `x-amz-content-sha256` correctement calculé. Vérifier aussi que
l'horloge hôte/conteneur est synchro (les signatures expirent vite).

> Erreur du TP : initialement la signature utilisait `cksum` et l'endpoint
> `localhost:9000` depuis le conteneur → 403 systématique. Corrigé en
> construisant la signature avec `AWS4-HMAC-SHA256` et l'URI réelle.

## « ERROR: olbdavro » ou coffre / catalog Nessie vide

**Symptôme** : le job Spark échoue à créer les tables, ou Nessie est vide.

**Causes courantes** :
- Nessie n'a pas encore fini de démarrer (attendre ~30 s) ;
- région/URL du warehouse incohérentes entre la config du catalogue et réalité ;
- mémoire insuffisante (voir plus bas).

**Diagnostic** :

```bash
curl -s http://localhost:19120/api/v2/config
docker compose logs nessie | tail -50
docker compose logs iceberg-init | tail -80
```

## Mémoire (WSL2 / Docker Desktop)

**Symptôme** : builds lents, containers OOM-killed, Nessie/Spark qui meurent.

**Causes** : chaque JVM tient ~500 à 800 Mo (Nessie `-Xmx768m`, Spark daemons
`640m`, driver/executor locaux ~512–768 Mo) + l'image weji. **8 Go recommandés.**

**Paramètres à surveiller** :
- Docker Desktop / WSL2 : `%USERPROFILE%\.wslconfig` (maxMemory) ;
- `docker-compose.yml` : `mem_limit` par service ;
- Spark : réécrit `SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY` et devez si besoin
  réduire les largestes de place.

## Les images Spark/Iceberg prennent du temps

Le premier `docker compose up` télécharge `iceberg-spark-runtime`, `hadoop-aws`,
`aws-java-sdk-bundle` depuis Maven Central dans le Dockerfile.spark →
**long au premier build, instantané ensuite** (couche en cache).

## datasets sont vides dans le dashboard

- Le parquet n'a pas été uploadé → relancer `scripts/db_to_parquet.py`.
- La requête SigV4 échoue en silence → consulter les logs streamlit :
  ```bash
  docker compose logs streamlit --tail 40
  ```
- L'endpoint interne utilisé par DuckDB est `minio:9000` (jamais `localhost`).

## Docker : le conteneur n'existe pas / `mc` introuvable

Les scripts d'upload font `docker cp` vers le conteneur **`minio-s3`** puis
`mc cp local/...`. Si le transfert échoue :
- vérifier le nom du conteneur : `docker ps --format '{{.Names}}'` ;
- adapter la constante dans le script si besoin.

## Reset complet

Quand tout est cassé :

```bash
docker compose down -v        # supprime les volumes (MinIO + Nessie)
python3.10 scripts/db_to_parquet.py && docker compose run --rm iceberg-init
```