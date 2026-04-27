#!/bin/bash
# ============================================
# 🎌 AniData Lab - Script de démarrage
# Stack: Elasticsearch + Grafana + Airflow
# ============================================

set -e

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo -e "${BOLD}🎌 AniData Lab — Démarrage de l'environnement${NC}"
echo "=============================================="
echo ""

# --- Vérifier Docker ---
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker n'est pas installé !${NC}"
    echo "   Installez Docker Desktop : https://www.docker.com/get-started"
    exit 1
fi

if ! docker info &> /dev/null 2>&1; then
    echo -e "${RED}❌ Docker n'est pas démarré !${NC}"
    echo "   Lancez Docker Desktop et réessayez."
    exit 1
fi

echo -e "${GREEN}✅ Docker est opérationnel${NC}"

# --- Vérifier VS Code ---
if command -v code &> /dev/null; then
    echo -e "${GREEN}✅ VS Code est installé${NC}"
else
    echo -e "${YELLOW}⚠️  VS Code non détecté (optionnel mais recommandé)${NC}"
fi

# --- Vérifier Python ---
if command -v python3 &> /dev/null; then
    PY_VERSION=$(python3 --version 2>&1)
    echo -e "${GREEN}✅ $PY_VERSION${NC}"
else
    echo -e "${YELLOW}⚠️  Python3 non détecté — nécessaire pour le Data Refinement${NC}"
fi

# --- Vérifier les données ---
echo ""
echo -e "${BOLD}📦 Vérification des données...${NC}"

MISSING=0
for file in anime.csv rating_complete.csv anime_with_synopsis.csv; do
    if [ -f "data/$file" ]; then
        SIZE=$(du -h "data/$file" | cut -f1)
        echo -e "   ${GREEN}✅ $file ($SIZE)${NC}"
    else
        echo -e "   ${RED}❌ $file - MANQUANT${NC}"
        MISSING=1
    fi
done

if [ $MISSING -eq 1 ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Des fichiers sont manquants !${NC}"
    echo "   Téléchargez-les depuis :"
    echo "   https://www.kaggle.com/datasets/hernan4444/anime-recommendation-database-2020"
    echo "   et placez-les dans le dossier data/"
    echo ""
    read -p "Continuer quand même ? (o/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[OoYy]$ ]]; then
        exit 1
    fi
fi

# --- Configurer vm.max_map_count si Linux ---
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    CURRENT=$(sysctl -n vm.max_map_count 2>/dev/null || echo 0)
    if [ "$CURRENT" -lt 262144 ]; then
        echo ""
        echo -e "${YELLOW}⚙️  Configuration Linux pour Elasticsearch...${NC}"
        sudo sysctl -w vm.max_map_count=262144
    fi
fi

# --- Lancement ---
echo ""
echo -e "${BOLD}🐳 Lancement des services Docker...${NC}"
echo "   (Premier lancement : ~5-10 min pour télécharger les images)"
echo ""

docker compose up -d

echo ""
echo -e "${BOLD}⏳ Attente du démarrage des services (45 secondes)...${NC}"
sleep 45

# --- Vérification ---
echo ""
echo -e "${BOLD}🔍 Vérification des services...${NC}"
echo ""

check_service() {
    local name=$1
    local url=$2
    if curl -s -o /dev/null -w "%{http_code}" "$url" | grep -q "200\|302"; then
        echo -e "   ${GREEN}✅ $name${NC} → $url"
    else
        echo -e "   ${YELLOW}⏳ $name pas encore prêt${NC} → $url (patientez 1-2 min)"
    fi
}

check_service "Elasticsearch" "http://localhost:9200"
check_service "Grafana"       "http://localhost:3000"
check_service "Airflow"       "http://localhost:8080"

echo ""
echo "=============================================="
echo -e "${BOLD}🎌 AniData Lab est prêt !${NC}"
echo ""
echo "   📊 Grafana        → http://localhost:3000  (admin / anidata)"
echo "   🔄 Airflow        → http://localhost:8080  (admin / admin)"
echo "   🔍 Elasticsearch  → http://localhost:9200"
echo ""
echo "   💻 Ouvrir dans VS Code : code ."
echo ""
echo "   Pour arrêter : docker compose down"
echo "=============================================="
