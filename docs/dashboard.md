# Dashboard Streamlit

L'application `app.py` (buildée dans le service `streamlit`, port **8501**) est
le cœur visuel du TP. Elle se connecte à MinIO **sans boto3** : les requêtes
S3 sont signées « à la main » avec **AWS Signature V4**, puis les parquet sont
lus via **DuckDB**.

## Onglets

### 1. Vue d'ensemble (logs)
- Sélection des fichiers de logs à charger (multi-select).
- Lecture DuckDB : `read_parquet([...], union_by_name=true, filename=true)`.
- Métriliques : lignes, colonnes, fichiers chargés.
- **5 graphiques Altair** :
  1. activité temporelle (aire, par heure) ;
  2. répartition par niveau de log ;
  3. tokens (entrée/sortie) par session ;
  4. top 10 des sessions (volume de messages) ;
  5. coût cumulé (courbe) — à noter : le champ `cost` est souvent vide dans les
     logs (le coût réel est porté par les `step-finish` des parts).
- Données brutes + schéma + statistiques dans des expanders.

### 2. Timeline de la conversation
Sélection d'une session → chronologie rendue avec `st.chat_message` :

- texte **utilisateur** / **assistant** ;
- **raisonnement** (caption) ;
- **appels d'outils** : expander avec input (JSON indenté) et output (texte) ;
- **step-finish** : tokens + coût ;
- **patch** : fichiers modifiés (JSON parsé via `json.loads`).

### 3. Agent actions
Tous les appels d'outils, filtrables par **outil** et par **session** — le détail
input/output JSON de chaque appel.

### 4. Fichiers modifiés
Agrège les `patch` : nombre de fichiers par session (graphique en barres) +
tableau (horodatage, session, fichier).

### 5. Sessions
Tableau de bord par session : nombre de parts, d'outils, de messages, coût et
tokens cumulés (agrégats pandas), trié par date, + graphique « parts par session ».

### 6. SQL explorer
Requête SQL libre **DuckDB** sur les tables enregistrées `parts`, `sessions` et
`logs`. Exemple prérempli :

```sql
SELECT session_id, part_type, count(*) AS n
FROM parts GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 50;
```

## Points techniques notables

- **Signature SigV4** : `sign_request()` construit la `canonical request`
  (URI encodée, query string triée), signe avec HMAC-SHA256, ajoute
  `x-amz-date`, `x-amz-content-sha256` (payload vide = `EMPTY_SHA256`).
- **DuckDB** : `CREATE SECRET (TYPE s3, KEY_ID ..., SECRET ..., ENDPOINT 'minio:9000',
  REGION 'us-east-1', USE_SSL false, URL_STYLE 'path')`.
- **`width="stretch"`** remplace l'ancien `use_container_width` (supprimé par
  Streamlit en 2026).

## Lancer / relancer

```bash
docker compose up -d streamlit          # démarre
docker compose up --build -d streamlit  # après toute modification d'app.py
open http://localhost:8501
```

> `Dockerfile` fait `COPY app.py .` : toute modification de `app.py` nécessite
> une reconstruction de l'image.