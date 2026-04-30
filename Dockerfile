# =============================================================================
# Dockerfile — Pipeline d'ingestion Football Data
# =============================================================================
# Image de base : python:3.9-slim (légère, sans packages inutiles)
# Build : docker build -t data-ingestion-football .
#                                                  ↑ Le point final est OBLIGATOIRE
# Run  : docker run -e RAPIDAPI_KEY="votre_cle" -e MODE="api" data-ingestion-football
# =============================================================================

FROM python:3.9-slim

# --- Métadonnées de l'image ---
LABEL maintainer="Etudiant 3iL <portfolio@3il.fr>"
LABEL description="Pipeline d'ingestion des statistiques football via RapidAPI"
LABEL version="1.0.0"

# --- Variables d'environnement par défaut ---
# MODE peut être surchargé au runtime : -e MODE=api
ENV MODE=test \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# --- Répertoire de travail dans le conteneur ---
WORKDIR /app

# --- Copie et installation des dépendances ---
# On copie requirements.txt EN PREMIER pour exploiter le cache Docker :
# si requirements.txt n'a pas changé, cette couche n'est pas reconstruite.
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# --- Copie du code source ---
COPY ingestion.py .

# --- Copie du fichier de fallback (mode test) ---
# Ce fichier est utilisé quand MODE=test (pas besoin de clé API).
# Il peut être absent si on tourne toujours en MODE=api.
COPY raw_real_madrid_stats.json* ./

# --- Commande par défaut ---
# La variable MODE est lue dans ingestion.py via os.environ.get("MODE", "test")
CMD ["python", "ingestion.py"]
