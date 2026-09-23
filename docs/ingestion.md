# Ingestion des données

## Les deux sources de données OpenCode

OpenCode stocke deux types d'informations dans `~/.local/share/opencode/` :

### 1. Le journal de logs (`log/opencode.log`)
Lignes au format `clé=valeur` documentant le **cycle de vie** des sessions
(`session.start`, `session.idle`, …). Riche en événements mais **sans le détail
des conversations**.

### 2. La base SQLite (`opencode.db`)
Les **vraies conversations** : tables `session`, `message` et `part`.

| Table | Contenu |
|---|---|
| `session` | une conversation (titre, dossier, résumé +/- lignes, dates) |
| `message` | un tour de chat (rôle `user` / `assistant`) |
| `part` | un bloc de contenu, typé : `text`, `reasoning`, `tool`, `step-start`, `step-finish`, `patch` |

C'est ce qui alimente les onglets « Timeline », « Agent actions », « Fichiers modifiés ».

## Scripts d'export

### `scripts/log_to_parquet.py`

```bash
python3.10 scripts/log_to_parquet.py                     # → s3://my-bucket/opencode-logs.parquet
python3.10 scripts/log_to_parquet.py --session <ID>      # → s3://my-bucket/sessions/<ID>.parquet
```

Lit `opencode.log`, parse chaque ligne `clé=valeur`, normalise `timestamp`,
écrit un parquet, puis l'uploade via `docker cp` + `mc cp`.

### `scripts/db_to_parquet.py`

```bash
python3.10 scripts/db_to_parquet.py                      # → conversations/{parts,sessions}.parquet
```

Points importants :

- **Lecture sécurisée** : copie la base dans un répertoire temporaire (avec
  `-wal`/`-shm` si présents) et lit la copie en lecture seule — on ne verrouille
  pas la base active d'OpenCode.
- **Jointures** : `part → message → session` pour récupérer le rôle et le titre.
- **Extraction typée** selon le type de part :
  - `text` → texte utilisateur / assistant ;
  - `reasoning` → raisonnement + durée ;
  - `tool` → outil, statut, **entrée et sortie** sérialisées en JSON ;
  - `step-finish` → `reason`, tokens (input/output/total), `cost` ;
  - `patch` → liste des fichiers modifiés (JSON).
- **Upload** : `docker cp` vers `minio-s3` puis `mc cp local/<bucket>/<key>`.

## Export automatique : plugin OpenCode

Le plugin `.opencode/plugin/export-to-s3.ts` écoute l'événement **`session.idle`**
et, à chaque fin de session :

```bash
python3.10 scripts/log_to_parquet.py --session <ID>   # logs de la session
python3.10 scripts/db_to_parquet.py                   # conversations (full)
```

Cela maintient MinIO à jour sans manipulation manuelle.

> Le plugin vit dans le dossier `.opencode/` du projet : c'est un plugin local
> TypeScript (Bun shell, API `@opencode-ai/plugin`). Sur un autre PC, penser à
> rapprocher ce dossier du répertoire de travail OpenCode.

## Format des parquet produits

- `conversations/parts.parquet` : une ligne par part (part_id, message_id,
  session_id, role, title, part_type, text, tool, tool_input, tool_output,
  tokens_*, cost, files, time_created, time_updated).
- `conversations/sessions.parquet` : une ligne par session (id, slug, title,
  directory, path, summary_additions/deletions, time_created/updated).
- `opencode-logs.parquet` : les événements du journal (`timestamp`, `level`,
  `type`, `session.id`, …).

Ces trois fichiers sont la base de la lecture DuckDB (dashboard) et de la
construction des tables Iceberg.