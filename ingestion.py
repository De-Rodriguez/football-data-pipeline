"""
ingestion.py — Pipeline Data Engineering | Real Madrid Squad Stats
==================================================================
Auteur  : Étudiant 3iL (Portfolio Alternance)
Source  : RapidAPI — free-api-live-football-data
Mode    : "api" (appel réseau) ou "test" (lecture JSON local)

Leçons apprises
---------------
- HTTP 403 : clé API invalide ou abonnement insuffisant sur RapidAPI.
- HTTP 404 : URL mal formée — utiliser EXACTEMENT l'endpoint ci-dessous.
- Paramètre équipe : `equipe` (PAS `team_id`).
- ID Real Madrid : 8633
"""

import os
import json
import logging
import requests
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration du logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
API_URL = "https://free-api-live-football-data.p.rapidapi.com/football-get-team-players"
REAL_MADRID_ID = 8633
LOCAL_FALLBACK_FILE = "raw_real_madrid_stats.json"
OUTPUT_FILE = "raw_real_madrid_stats.json"


# ---------------------------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------------------------

def get_api_key() -> str:
    """
    Récupère la clé API depuis la variable d'environnement RAPIDAPI_KEY.
    Lève une ValueError si la variable n'est pas définie.
    """
    api_key = os.environ.get("RAPIDAPI_KEY")
    if not api_key:
        raise ValueError(
            "Variable d'environnement RAPIDAPI_KEY manquante.\n"
            "  → Ajoutez-la avec : export RAPIDAPI_KEY='votre_cle_ici'\n"
            "  → Ou lancez en mode test : MODE=test python ingestion.py"
        )
    return api_key


def fetch_from_api(team_id: int = REAL_MADRID_ID) -> dict:
    """
    Appelle l'API RapidAPI et retourne le JSON brut.

    Gestion des erreurs HTTP critiques :
        - 403 : Problème d'abonnement ou clé API incorrecte.
        - 404 : URL mal formée (vérifier API_URL et le paramètre `equipe`).
        - Autres : Timeout, erreurs réseau, etc.

    Args:
        team_id: Identifiant de l'équipe (défaut : Real Madrid = 8633).

    Returns:
        Dictionnaire Python contenant la réponse JSON de l'API.
    """
    api_key = get_api_key()

    headers = {
        "x-rapidapi-host": "free-api-live-football-data.p.rapidapi.com",
        "x-rapidapi-key": api_key,
    }

    # ATTENTION : le paramètre s'appelle `equipe` (et NON `team_id`)
    params = {"equipe": str(team_id)}

    logger.info(f"Appel API → {API_URL} | equipe={team_id}")

    try:
        response = requests.get(
            API_URL,
            headers=headers,
            params=params,
            timeout=15,
        )

        # --- Gestion des erreurs HTTP ---
        if response.status_code == 403:
            logger.error(
                "HTTP 403 — Accès refusé.\n"
                "  Causes possibles :\n"
                "  1. Clé API RAPIDAPI_KEY incorrecte ou expirée.\n"
                "  2. Abonnement insuffisant sur RapidAPI (plan gratuit dépassé).\n"
                "  → Vérifiez votre dashboard : https://rapidapi.com/hub"
            )
            raise PermissionError("HTTP 403 : Accès refusé par l'API.")

        if response.status_code == 404:
            logger.error(
                "HTTP 404 — Ressource introuvable.\n"
                f"  URL utilisée : {response.url}\n"
                "  → Vérifiez que l'URL de base est exactement :\n"
                f"     {API_URL}\n"
                "  → Vérifiez que le paramètre est bien `equipe` et non `team_id`."
            )
            raise ValueError("HTTP 404 : URL de l'API incorrecte.")

        # Lève une HTTPError pour tous les autres codes 4xx/5xx
        response.raise_for_status()

        data = response.json()
        logger.info(f"Réponse reçue — statut API : {data.get('status', 'inconnu')}")
        return data

    except requests.exceptions.Timeout:
        logger.error("Timeout — L'API n'a pas répondu dans les 15 secondes.")
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Erreur réseau : {e}")
        raise


def load_from_local(filepath: str = LOCAL_FALLBACK_FILE) -> dict:
    """
    Charge les données depuis un fichier JSON local (mode test / fallback).

    Args:
        filepath: Chemin vers le fichier JSON de fallback.

    Returns:
        Dictionnaire Python contenant les données brutes.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier local introuvable : {filepath}\n"
            "  → Exécutez d'abord en mode 'api' pour générer ce fichier,\n"
            "    ou placez un fichier JSON de test à cet emplacement."
        )

    logger.info(f"Mode TEST — Lecture depuis : {filepath}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_raw_data(data: dict, output_path: str = OUTPUT_FILE) -> None:
    """
    Sauvegarde les données brutes dans un fichier JSON horodaté.

    Args:
        data     : Dictionnaire à sauvegarder.
        output_path : Chemin du fichier de sortie.
    """
    # Ajout d'une métadonnée d'ingestion pour la traçabilité
    enriched = {
        "_ingestion_metadata": {
            "ingested_at": datetime.utcnow().isoformat() + "Z",
            "source": API_URL,
            "team_id": REAL_MADRID_ID,
        },
        **data,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    logger.info(f"Données sauvegardées → {output_path}")


def validate_response(data: dict) -> bool:
    """
    Valide la structure minimale attendue de la réponse API.

    Structure attendue :
        response > list > squad > members

    Args:
        data: Dictionnaire JSON brut.

    Returns:
        True si la structure est valide, False sinon.
    """
    try:
        squads = data["response"]["list"]["squad"]
        if not isinstance(squads, list) or len(squads) == 0:
            logger.warning("Aucun squad trouvé dans la réponse.")
            return False

        # Vérification de la présence de membres dans le premier squad
        first_members = squads[0].get("members", [])
        if not isinstance(first_members, list):
            logger.warning("Structure `members` absente ou invalide.")
            return False

        logger.info(
            f"Validation OK — {len(squads)} squad(s) trouvé(s), "
            f"{len(first_members)} membres dans le premier."
        )
        return True

    except KeyError as e:
        logger.error(f"Clé manquante dans la réponse : {e}")
        return False


# ---------------------------------------------------------------------------
# Point d'entrée principal
# ---------------------------------------------------------------------------

def run(mode: str = "api") -> dict:
    """
    Orchestre le pipeline d'ingestion.

    Args:
        mode : "api"  → Appel réseau vers RapidAPI (production).
               "test" → Lecture depuis le fichier JSON local (développement).

    Returns:
        Dictionnaire contenant les données brutes ingérées.
    """
    logger.info(f"=== Démarrage du pipeline d'ingestion | Mode : {mode.upper()} ===")

    if mode == "test":
        data = load_from_local()
    elif mode == "api":
        data = fetch_from_api(team_id=REAL_MADRID_ID)
        save_raw_data(data)
    else:
        raise ValueError(f"Mode inconnu : '{mode}'. Utilisez 'api' ou 'test'.")

    # Validation de la structure
    is_valid = validate_response(data)
    if not is_valid:
        logger.warning(
            "La réponse ne correspond pas à la structure attendue.\n"
            "  → Vérifiez l'API ou le fichier JSON local."
        )

    logger.info("=== Pipeline d'ingestion terminé avec succès ===")
    return data


if __name__ == "__main__":
    # Lecture du mode depuis la variable d'environnement (défaut : "test")
    mode = os.environ.get("MODE", "test").lower()
    result = run(mode=mode)

    # Affichage d'un résumé en sortie standard
    try:
        squads = result["response"]["list"]["squad"]
        total_players = sum(len(s.get("members", [])) for s in squads)
        print(f"\n✅ Ingestion réussie — {total_players} joueur(s) récupéré(s).")
    except (KeyError, TypeError):
        print("\n⚠️  Données ingérées mais structure inattendue — vérifiez le JSON.")
