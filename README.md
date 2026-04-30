# ⚽ Football Data Pipeline — Real Madrid Squad Stats

> **Pipeline Data Engineering de bout en bout** : Ingestion API → Docker → Databricks (Architecture Medallion)  
> Projet portfolio réalisé dans le cadre de la préparation à une alternance — École 3iL

---

## 📐 Architecture globale

```
┌─────────────────────────────────────────────────────────────────────┐
│                       DATA PIPELINE                                 │
│                                                                     │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐  │
│   │   RapidAPI   │───▶│    Docker    │───▶│      Databricks      │  │
│   │  Football    │    │  Container   │    │  Architecture        │  │
│   │    Data      │    │ ingestion.py │    │    Medallion         │  │
│   └──────────────┘    └──────────────┘    │                      │  │
│                                           │  🥉 Bronze (brut)    │  │
│   Fallback JSON ──────────────────────▶  │  🥈 Silver (propre)  │  │
│   (mode test)                            │  🥇 Gold (agrégé)    │  │
│                                           └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Flux de données

| Étape | Outil | Description |
|-------|-------|-------------|
| **Ingestion** | Python + Requests | Appel API RapidAPI ou lecture JSON local |
| **Conteneurisation** | Docker | Isolation et portabilité du script |
| **Bronze** | PySpark + Delta | JSON brut persisté tel quel |
| **Silver** | PySpark + `explode` | Aplatissement de la structure imbriquée |
| **Gold** | PySpark + Delta | Table de performance agrégée, prête BI |

---

## 🗂️ Structure du projet

```
football-pipeline/
├── ingestion.py                  # Script d'ingestion (mode api / test)
├── transformation_databricks.py  # Pipeline Medallion Bronze→Silver→Gold
├── Dockerfile                    # Image Docker optimisée python:3.9-slim
├── requirements.txt              # Dépendances Python
├── raw_real_madrid_stats.json    # Données de test (fallback local)
└── README.md                     # Ce fichier
```

---

## ⚙️ Prérequis

- **Docker** ≥ 20.x ([installer Docker](https://docs.docker.com/get-docker/))
- **Compte RapidAPI** + abonnement `free-api-live-football-data` ([lien API](https://rapidapi.com/))
- **Databricks** Community Edition ou Workspace d'entreprise

---

## 🐍 1. Lancement local (sans Docker)

```bash
# Cloner le projet
git clone https://github.com/votre-username/football-pipeline.git
cd football-pipeline

# Installer les dépendances
pip install -r requirements.txt

# --- Mode TEST (pas besoin de clé API) ---
MODE=test python ingestion.py

# --- Mode API (clé RapidAPI requise) ---
export RAPIDAPI_KEY="votre_cle_ici"
MODE=api python ingestion.py
```

---

## 🐳 2. Build et lancement Docker

### Build de l'image

```bash
# ⚠️ Le point final (.) est OBLIGATOIRE — il indique le contexte de build
docker build -t data-ingestion-football .
```

### Lancement en mode TEST (pas de clé API nécessaire)

```bash
docker run -e MODE=test data-ingestion-football
```

### Lancement en mode API (production)

```bash
docker run \
  -e RAPIDAPI_KEY="votre_cle_rapidapi" \
  -e MODE=api \
  data-ingestion-football
```

### Récupérer le fichier JSON généré

```bash
# Créer le conteneur sans l'exécuter, copier le JSON, puis supprimer
docker create --name temp-container data-ingestion-football
docker cp temp-container:/app/raw_real_madrid_stats.json ./output.json
docker rm temp-container
```

---

## ☁️ 3. Databricks — Pipeline Medallion

### Étape 1 : Uploader le fichier JSON dans DBFS

Dans un notebook Databricks, exécutez :

```python
# Upload depuis votre machine locale vers DBFS
dbutils.fs.mkdirs("dbfs:/data/football/raw/")
# Puis glissez-déposez le JSON dans Data → DBFS → /data/football/raw/
```

Ou via la CLI Databricks :

```bash
databricks fs cp raw_real_madrid_stats.json dbfs:/data/football/raw/
```

### Étape 2 : Importer le script dans Databricks

1. Allez dans **Workspace** → **Import**
2. Sélectionnez **"File"** et uploadez `transformation_databricks.py`
3. Ou **copiez-collez** le contenu dans un nouveau notebook Python

### Étape 3 : Exécuter le pipeline

```python
# Dans une cellule notebook Databricks
run_pipeline()
```

### Étape 4 : Interroger les tables Gold en SQL

```sql
-- Dans une cellule %sql du notebook
SELECT name, position_group, goals, assists, performance_score, market_tier
FROM football_db.gold_real_madrid_performance
ORDER BY performance_rank
LIMIT 15;
```

---

## 🔍 Structure des données imbriquées (API)

L'API renvoie une structure JSON profonde à 4 niveaux :

```json
{
  "response": {
    "list": {
      "squad": [                    ← explode() #1 (par type de poste)
        {
          "type": "Forwards",
          "members": [             ← explode() #2 (par joueur)
            {
              "id": 401,
              "name": "Vinícius Júnior",
              "stats": {
                "goals": 24,
                "assists": 9,
                "transfer_value": 180000000.0
              }
            }
          ]
        }
      ]
    }
  }
}
```

La couche **Silver** utilise `F.explode()` deux fois pour aplatir cette structure.

---

## ⚠️ Leçons apprises — Erreurs fréquentes

| Erreur | Cause | Solution |
|--------|-------|----------|
| **HTTP 403** | Clé API invalide ou quota dépassé | Vérifier `RAPIDAPI_KEY` et le plan RapidAPI |
| **HTTP 404** | URL de l'endpoint incorrecte | Utiliser exactement l'URL dans `ingestion.py` |
| **Paramètre API** | Utilisation de `team_id` au lieu de `equipe` | Le paramètre correct est `equipe` |
| **Docker build** | Point final oublié | `docker build -t data-ingestion-football .` |
| **KeyError Silver** | Structure JSON différente de l'attendu | Vérifier avec `df.printSchema()` en Bronze |

---

## 🏗️ Architecture Medallion — Détail

```
Bronze  → Données brutes, immuables, aucune transformation
Silver  → Données propres, typées, structure plate
Gold    → Agrégats métier, prêts pour BI et Machine Learning
```

### Table Gold — Colonnes produites

| Colonne | Type | Description |
|---------|------|-------------|
| `player_id` | Integer | Identifiant unique |
| `name` | String | Nom du joueur |
| `goals` | Integer | Buts marqués |
| `assists` | Integer | Passes décisives |
| `performance_score` | Integer | Buts + Passes |
| `goals_per_game` | Double | Buts / Matchs joués |
| `performance_rank` | Integer | Classement dans le squad |
| `market_tier` | String | Classe de valeur marchande |

---

## 🛠️ Technologies utilisées

| Technologie | Version | Rôle |
|-------------|---------|------|
| Python | 3.9 | Ingestion et logique métier |
| Requests | 2.31 | Appels API HTTP |
| Docker | 20.x | Conteneurisation |
| Apache Spark | 3.x | Traitement distribué |
| Delta Lake | Intégré DBR | Stockage transactionnel |
| Databricks | Community / Pro | Orchestration et notebook |

---

## 📈 Évolutions possibles

- [ ] Ajout d'**Apache Airflow** pour l'orchestration planifiée
- [ ] **Tests unitaires** avec pytest sur `ingestion.py`
- [ ] Intégration **CI/CD GitHub Actions** (build Docker automatique)
- [ ] Ajout de plusieurs équipes (paramétrage dynamique `equipe_id`)
- [ ] Dashboard **Power BI** ou **Grafana** connecté à la table Gold

---

## 👤 Auteur

Étudiant ingénieur — École d'ingénieurs 3iL  
Portfolio GitHub | Préparation alternance Data Engineering

---

*Projet réalisé à des fins pédagogiques dans le cadre d'un portfolio alternance.*
