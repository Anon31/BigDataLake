# Roadmap — aller plus loin

## Big Data réellement

- Requêtes **Spark** sur les tables Iceberg (au lieu du dashboard seul) :
  join `parts` + `sessions`, agrégats par session, `MERGE` sur les nouvelles parts.
- **Time travel** : `SELECT * FROM parts VERSION AS OF <snapshot>` ; montrer
  l'historique des snapshots dans les `metadata` Iceberg.
- **Branches Nessie** : créer une branche expérimentale, muter des données,
  fusionner (le « Git des données », démo spectaculaire).
- Partitionner la table des parts (par session ou mois) et montrer l'évolution
  de schéma (ADD/DROP COLUMN).

## Données

- **Nettoyage** : dédupliquer par `message_id`, gérer les `part` obsolètes,
  normaliser le coût (champ renseigné seulement dans certains `step-finish`).
- Conserver un **catalogue des fichiers** (table `inventory`) pour masquer le
  `read_parquet([...], union_by_name)` fragile.
- Ingérer aussi les **prompts/messages fichier** (parts `step-start`/`step-finish`
  avec `files`) pour une analyse « quels fichiers sont le plus touchés ».

## Qualité & outils

- **CI** (GitHub Actions) : lint `ruff` sur `app.py` + scripts, build des images
  Docker, smoke test du dashboard.
- **Tests** : pytest sur la signature SigV4 (vecteurs S3 officiels) et sur les
  parsers de logs / extraction des parts.
- **Logs structurés** : exporter aussi `opencode.log` vers le warehouse pour
  croiser événements de session et conversations.

## UI

- Sélecteur de **branche/commits Nessie** dans le dashboard (time travel dans
  l'onglet Timeline). Justification pédagogique : voir l'état des données au
  fil des snapshots.
- Export des graphiques/requêtes en PNG, personnalisation du thème Altair.
- Multi-utilisateurs : `session_state` pour éviter que deux vues se percutent.

## Ops

- **Healthchecks** sur chaque service dans docker-compose.
- **Documentation de déploiement** : script de provisionnement des prérequis
  (Python, Docker, `.wslconfig`).
- **Sécurité** : sortir les credentials S3 de `app.py` vers des variables
  d'environnement / secrets (éviter de les committer).