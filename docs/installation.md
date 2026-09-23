# Installation & reproduction

## Prérequis

- **Docker** avec `docker compose` (Desktop sous Windows/macOS, engine sous Linux).
- **Python 3.10+** sur l'hôte avec `pandas` et `pyarrow` (pour les scripts d'export) :
  ```bash
  python3.10 -c "import pandas, pyarrow; print(pandas.__version__, pyarrow.__version__)"
  # si absent → python3.10 -m pip install pandas pyarrow
  ```
- **OpenCode** installé et utilisé (pour générer les données : logs + base SQLite).
- **8 Go de RAM** recommandé (voir la section Mémoire).

## Arborescence du projet

```
BigDataLake/
├── docker-compose.yml        # toute la stack (8 services)
├── Dockerfile                # image Streamlit
├── Dockerfile.spark          # image Spark 3.5.4 + jars Iceberg/hadoop-aws
├── requirements.txt          # streamlit, duckdb, requests
├── app.py                    # dashboard Streamlit (6 onglets)
├── README.md                 # guide de reproduction (vite)
├── docs/                     # documentation web (docsify)
│   ├── index.html
│   ├── _sidebar.md
│   └── *.md                  # les pages de cette documentation
└── scripts/
    ├── log_to_parquet.py     # opencode.log → parquet S3
    ├── db_to_parquet.py      # opencode.db  → conversations/*.parquet
    ├── setup-buckets.sh      # création des buckets
    └── iceberg_init.py       # job PySpark : parquet → tables Iceberg
```

## Préparer les fichiers

Les scripts d'export s'appuient sur le nom de conteneur **`minio-s3`** et sur le
chemin d'OpenCode par défaut (`~/.local/share/opencode/`). Si votre installation
diffère, adapter `scripts/log_to_parquet.py` et `scripts/db_to_parquet.py`
(les constantes `LOG_PATH` / `DB_PATH` sont en tête de fichiers).

## Démarrer la stack

```bash
docker compose up -d              # minio + streamlit + nessie + spark + setup
docker compose up -d docs         # (optionnel) la documentation docsify
docker compose ps                 # vérifier que tout est "Up"
docker logs minio-setup           # doit afficher "[setup-buckets] Terminé ✔"
```

Contrôles de bon fonctionnement :

```bash
curl -s http://localhost:19120/api/v2/config          # JSON = Nessie OK
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/   # 200 = Spark master
docker exec minio-s3 mc ls local/my-bucket            # bucket accessible
```

> Les images Spark/Iceberg sont construites au premier `up` (téléchargement des
> jars depuis Maven Central) : patience à la première utilisation.

## Créer les tables Iceberg

Après le premier export de conversations :

```bash
docker compose run --rm iceberg-init
```

Résultat attendu avec les données du TP : `parts` = **732 lignes**, `sessions` = **6 lignes**.

## Mise à jour des données

```bash
python3.10 scripts/log_to_parquet.py
python3.10 scripts/db_to_parquet.py
```

Puis rafraîchir le dashboard (http://localhost:8501).

## Arrêt

```bash
docker compose down        # arrêt, volumes conservés
docker compose down -v     # arrêt + suppression des volumes (reset complet !)
```