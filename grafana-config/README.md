# 🎌 Configuration Grafana — AniData Lab

## Installation

### 1. Copier les fichiers dans votre projet

Remplacez le dossier `grafana/` existant dans `anidata-lab/` :

```bash
# Depuis le dossier où vous avez extrait ce ZIP
cp -r grafana-config/provisioning  anidata-lab/grafana/provisioning
cp -r grafana-config/dashboards    anidata-lab/grafana/dashboards
cp    grafana-config/grafana.ini   anidata-lab/grafana/grafana.ini
```

### 2. Mettre à jour le service Grafana dans docker-compose.yml

Remplacez la section `grafana:` dans votre `docker-compose.yml` par :

```yaml
  grafana:
    image: grafana/grafana-oss:10.3.1
    container_name: anidata-grafana
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
      - ./grafana/grafana.ini:/etc/grafana/grafana.ini:ro
    ports:
      - "3000:3000"
    mem_limit: 128m
    depends_on:
      elasticsearch:
        condition: service_healthy
    networks:
      - anidata-network
```

### 3. Relancer Grafana

```bash
# Arrêter et supprimer le volume Grafana (reset complet)
docker compose stop grafana
docker compose rm -f grafana
docker volume rm anidata-lab_grafana_data 2>/dev/null

# Relancer
docker compose up -d grafana
```

### 4. Ouvrir le dashboard

Ouvrez : **http://localhost:3000**

Le dashboard devrait s'afficher automatiquement comme page d'accueil.

Si vous voyez la page de login : **admin / anidata**

## Ce qui est configuré

### Datasource (auto-provisionnée)
- **Nom** : Elasticsearch - Anime
- **UID** : anidata-es
- **URL** : http://elasticsearch:9200
- **Index** : anime
- **Time field** : aired_start
- **Version ES** : 8.0.0

### Dashboard (12 panels)

| Panel | Type | Données |
|-------|------|---------|
| Total animes | Stat | Compteur total |
| Score moyen | Stat | Moyenne des scores > 0 |
| Total membres | Stat | Somme des membres |
| Répartition par type | Pie chart | TV / OVA / Movie / Special... |
| Top 10 studios | Bar chart | Studios les plus productifs |
| Top 15 genres | Bar chart | Genres les plus fréquents |
| Score moyen par type | Bar chart | Quel type est le mieux noté |
| Catégorie de score | Pie chart | Mauvais / Moyen / Bon / Excellent |
| Studios Top/Mid/Indie | Pie chart | Classification des studios |
| Animes par décennie | Bar chart | Évolution temporelle |
| Drop ratio par genre | Bar chart | Taux d'abandon par genre |

### Plage de temps
La plage est fixée à **1917 → 2026** pour couvrir tout le dataset.

## Dépannage

### Le dashboard est vide
1. Vérifiez que ES a des données : `curl http://localhost:9200/anime/_count`
2. Si count = 0, relancez l'indexation : `python scripts/06_indexation_es.py`
3. Relancez Grafana : `docker compose restart grafana`

### Erreur de datasource
```bash
# Vérifier que ES est accessible depuis le réseau Docker
docker compose exec grafana curl -s http://elasticsearch:9200
```

### Reset complet de Grafana
```bash
docker compose stop grafana
docker compose rm -f grafana
docker volume rm anidata-lab_grafana_data
docker compose up -d grafana
```
