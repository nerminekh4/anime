"""
DAG : anidata_etl
Pipeline ETL complet : Extract → Transform → Load
AniData Lab — Sakura Analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ============================================
# CHEMINS
# ============================================
DATA_RAW = "/opt/airflow/data/raw"
OUTPUT_DIR = "/opt/airflow/output"

# ============================================
# DEFAULT ARGS
# ============================================
default_args = {
    'owner': 'anidata',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 23),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


# ╔══════════════════════════════════════════╗
# ║           EXTRACT — FONCTIONS            ║
# ╚══════════════════════════════════════════╝

def extraire_anime(**context):
    import pandas as pd
    import os

    chemin = f"{DATA_RAW}/anime.csv"
    print(f"📂 Extraction : {chemin}")

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"❌ Fichier manquant : {chemin}")

    df = pd.read_csv(chemin)
    print(f"✅ {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
    print(f"   Colonnes : {list(df.columns)}")

    info = {
        "fichier": "anime.csv",
        "lignes": df.shape[0],
        "colonnes": df.shape[1],
        "noms_colonnes": list(df.columns),
        "nan_total": int(df.isnull().sum().sum()),
    }
    context['ti'].xcom_push(key='anime_info', value=info)
    return info


def extraire_synopsis(**context):
    import pandas as pd
    import os

    chemin = f"{DATA_RAW}/anime_with_synopsis.csv"
    print(f"📂 Extraction : {chemin}")

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"❌ Fichier manquant : {chemin}")

    df = pd.read_csv(chemin)
    print(f"✅ {df.shape[0]:,} lignes × {df.shape[1]} colonnes")

    info = {
        "fichier": "anime_with_synopsis.csv",
        "lignes": df.shape[0],
        "colonnes": df.shape[1],
        "noms_colonnes": list(df.columns),
        "nan_total": int(df.isnull().sum().sum()),
    }
    context['ti'].xcom_push(key='synopsis_info', value=info)
    return info


def extraire_ratings(**context):
    import pandas as pd
    import os

    chemin = f"{DATA_RAW}/rating_complete.csv"
    print(f"📂 Extraction (échantillon) : {chemin}")

    if not os.path.exists(chemin):
        print(f"⚠️  Fichier manquant : {chemin} — ignoré")
        info = {"fichier": "rating_complete.csv", "disponible": False, "lignes_sample": 0}
        context['ti'].xcom_push(key='ratings_info', value=info)
        return info

    df_sample = pd.read_csv(chemin, nrows=100_000)
    print(f"✅ Échantillon : {df_sample.shape[0]:,} lignes × {df_sample.shape[1]} colonnes")

    info = {
        "fichier": "rating_complete.csv",
        "lignes_sample": df_sample.shape[0],
        "colonnes": df_sample.shape[1],
        "noms_colonnes": list(df_sample.columns),
        "disponible": True,
    }
    context['ti'].xcom_push(key='ratings_info', value=info)
    return info


def valider_schemas(**context):
    ti = context['ti']
    anime_info    = ti.xcom_pull(task_ids='extraire_anime',    key='anime_info')
    synopsis_info = ti.xcom_pull(task_ids='extraire_synopsis', key='synopsis_info')
    ratings_info  = ti.xcom_pull(task_ids='extraire_ratings',  key='ratings_info')

    erreurs = []
    warnings = []

    print("\n🔍 VALIDATION DES SCHÉMAS\n" + "="*50)

    # Colonnes obligatoires anime.csv (insensible à la casse)
    obligatoires_anime = ["mal_id", "name", "score", "genres", "episodes", "type", "members"]
    colonnes_anime = [c.lower() for c in anime_info['noms_colonnes']]
    for col in obligatoires_anime:
        if col in colonnes_anime:
            print(f"  ✅ anime.csv — '{col}' présent")
        else:
            warnings.append(f"anime.csv : colonne '{col}' manquante")
            print(f"  ⚠️  anime.csv — '{col}' MANQUANT")

    if anime_info['lignes'] < 10000:
        erreurs.append(f"anime.csv : {anime_info['lignes']:,} lignes seulement (min 10 000)")
    else:
        print(f"  ✅ anime.csv — {anime_info['lignes']:,} lignes OK")

    # Synopsis
    obligatoires_syn = ["mal_id", "name", "synopsis"]
    colonnes_syn = [c.lower() for c in synopsis_info['noms_colonnes']]
    for col in obligatoires_syn:
        if col in colonnes_syn:
            print(f"  ✅ synopsis — '{col}' présent")
        else:
            warnings.append(f"synopsis : colonne '{col}' manquante")
            print(f"  ⚠️  synopsis — '{col}' MANQUANT")

    print(f"\n{'='*50}")
    print(f"  Erreurs bloquantes : {len(erreurs)}")
    print(f"  Avertissements     : {len(warnings)}")

    if erreurs:
        raise ValueError("Validation Extract échouée :\n" + "\n".join(erreurs))

    rapport = {
        "statut": "OK",
        "warnings": warnings,
        "anime_lignes": anime_info['lignes'],
        "synopsis_lignes": synopsis_info['lignes'],
        "ratings_disponible": ratings_info.get('disponible', False),
    }
    ti.xcom_push(key='rapport_extract', value=rapport)
    print("\n✅ Validation Extract OK")
    return rapport


# ╔══════════════════════════════════════════╗
# ║          TRANSFORM — FONCTIONS           ║
# ╚══════════════════════════════════════════╝

def nettoyer_anime(**context):
    import pandas as pd
    import numpy as np
    import os

    print("\n🧹 NETTOYAGE — anime.csv\n" + "="*50)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(f"{DATA_RAW}/anime.csv")
    n_initial = len(df)
    print(f"  Chargé : {n_initial:,} lignes")

    # 1. Normaliser les noms de colonnes
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(" ", "_").str.replace("-", "_")
    )
    print(f"  ✅ Colonnes normalisées : {list(df.columns)}")

    # 2. Supprimer les doublons
    avant = len(df)
    df = df.drop_duplicates()
    id_col = next((c for c in ["mal_id", "anime_id", "uid"] if c in df.columns), None)
    if id_col:
        df = df.drop_duplicates(subset=[id_col], keep="first")
    print(f"  ✅ Doublons supprimés : {avant - len(df):,}")

    # 3. NaN déguisés → NaN réels
    nan_vals = ["Unknown", "unknown", "N/A", "n/a", "NA", "None", "none", "-", "", " "]
    replacements = 0
    for col in df.select_dtypes(include=["object"]).columns:
        mask = df[col].isin(nan_vals)
        replacements += mask.sum()
        df.loc[mask, col] = np.nan
    print(f"  ✅ NaN déguisés remplacés : {replacements:,}")

    # 4. Score = 0 → NaN
    score_col = next((c for c in df.columns if c.lower() == "score"), None)
    if score_col:
        df[score_col] = pd.to_numeric(df[score_col], errors="coerce")
        zeros = (df[score_col] == 0).sum()
        df.loc[df[score_col] == 0, score_col] = np.nan
        print(f"  ✅ Scores = 0 → NaN : {zeros:,}")

    # 5. Correction des types numériques
    num_cols = ["episodes", "ranked", "popularity", "members", "favorites",
                "watching", "completed", "on_hold", "dropped", "plan_to_watch",
                "score_10","score_9","score_8","score_7","score_6",
                "score_5","score_4","score_3","score_2","score_1"]
    for col in num_cols:
        if col in df.columns and df[col].dtype == object:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 6. Nettoyage des colonnes texte
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip().str.replace(r"\s+", " ", regex=True)

    # 7. Nettoyage colonnes multi-valuées
    for col in ["genres", "studios", "producers", "licensors"]:
        if col in df.columns:
            def clean_list(val):
                if pd.isna(val): return np.nan
                items = [i.strip() for i in str(val).split(",")]
                items = [i for i in items if i and i not in ["Unknown", ""]]
                return ", ".join(items) if items else np.nan
            df[col] = df[col].apply(clean_list)

    # 8. Dates
    if "aired" in df.columns and df["aired"].dropna().str.contains(" to ").any():
        df["aired_start"] = pd.to_datetime(
            df["aired"].str.split(" to ").str[0].str.strip(), errors="coerce")
        df["aired_end"] = pd.to_datetime(
            df["aired"].str.split(" to ").str[1].str.strip(), errors="coerce")

    # 9. Marquer les outliers
    df["is_outlier"] = False
    if score_col and score_col in df.columns:
        mask = df[score_col].notna() & ((df[score_col] < 1) | (df[score_col] > 10))
        df.loc[mask, "is_outlier"] = True

    # Export
    chemin_out = f"{OUTPUT_DIR}/anime_cleaned.csv"
    df.to_csv(chemin_out, index=False, encoding="utf-8")
    taille = os.path.getsize(chemin_out) / (1024*1024)
    print(f"\n  ✅ Exporté : {chemin_out} ({taille:.1f} MB)")
    print(f"  Résultat : {len(df):,} lignes × {df.shape[1]} colonnes")

    info = {"lignes": len(df), "colonnes": df.shape[1], "chemin": chemin_out}
    context['ti'].xcom_push(key='cleaned_info', value=info)
    return info


def feature_engineering(**context):
    import pandas as pd
    import numpy as np
    import os

    print("\n⚙️  FEATURE ENGINEERING\n" + "="*50)

    df = pd.read_csv(f"{OUTPUT_DIR}/anime_cleaned.csv")
    n_cols_avant = df.shape[1]
    print(f"  Chargé : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")

    # Feature 1 — Score pondéré
    if "score" in df.columns and "members" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        df["members"] = pd.to_numeric(df["members"], errors="coerce")
        df["weighted_score"] = (df["score"] * np.log10(df["members"].fillna(0) + 1)).round(2)
        df.loc[df["score"].isna(), "weighted_score"] = np.nan
        print(f"  ✅ weighted_score créé")

    # Feature 2 — Ratio d'abandon
    if "dropped" in df.columns and "completed" in df.columns:
        df["dropped"] = pd.to_numeric(df["dropped"], errors="coerce")
        df["completed"] = pd.to_numeric(df["completed"], errors="coerce")
        total = df["dropped"].fillna(0) + df["completed"].fillna(0)
        df["drop_ratio"] = np.where(total > 0, (df["dropped"].fillna(0) / total).round(4), np.nan)
        print(f"  ✅ drop_ratio créé")

    # Feature 3 — Catégorie de score
    if "score" in df.columns:
        df["score_category"] = pd.cut(
            df["score"], bins=[0, 5, 6.5, 8, 10],
            labels=["Mauvais", "Moyen", "Bon", "Excellent"],
            include_lowest=True
        )
        print(f"  ✅ score_category créée")

    # Feature 4 — Tier studio
    studio_col = next((c for c in ["studios", "studio"] if c in df.columns), None)
    if studio_col:
        df["main_studio"] = df[studio_col].str.split(", ").str[0]
        counts = df["main_studio"].value_counts()
        def tier(s):
            if pd.isna(s): return np.nan
            c = counts.get(s, 0)
            return "Top" if c >= 50 else ("Mid" if c >= 10 else "Indie")
        df["studio_tier"] = df["main_studio"].apply(tier)
        print(f"  ✅ studio_tier créé")

    # Feature 5 — Décennie
    date_col = next((c for c in ["aired_start", "aired", "premiered"] if c in df.columns), None)
    if date_col:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        df["year"] = dates.dt.year
        df["decade"] = (df["year"] // 10 * 10).astype("Int64")
        print(f"  ✅ year et decade créés")

    # Feature 6 — Nombre de genres
    genre_col = next((c for c in ["genres", "genre"] if c in df.columns), None)
    if genre_col:
        df["n_genres"] = df[genre_col].str.split(", ").str.len()
        df.loc[df[genre_col].isna(), "n_genres"] = np.nan
        df["main_genre"] = df[genre_col].str.split(", ").str[0]
        print(f"  ✅ n_genres et main_genre créés")

    # Feature 7 — Engagement ratio
    if "favorites" in df.columns and "members" in df.columns:
        df["favorites"] = pd.to_numeric(df["favorites"], errors="coerce")
        df["engagement_ratio"] = np.where(
            df["members"].fillna(0) > 0,
            (df["favorites"].fillna(0) / df["members"]).round(4),
            np.nan
        )
        print(f"  ✅ engagement_ratio créé")

    # Feature 8 — Durée en minutes
    if "duration" in df.columns and df["duration"].dtype == object:
        def parse_duration(val):
            if pd.isna(val): return np.nan
            val = str(val).lower()
            minutes = 0
            if "hr" in val:
                try: minutes += int(val.split("hr")[0].strip().split()[-1]) * 60
                except: pass
            if "min" in val:
                try: minutes += int(val.split("min")[0].strip().split()[-1])
                except: pass
            return minutes if minutes > 0 else np.nan
        df["duration_minutes"] = df["duration"].apply(parse_duration)
        print(f"  ✅ duration_minutes créé")

    # ── Export gold avec versioning ───────────────────────────────
    # On garde toujours anime_gold.csv (lu par les tâches suivantes)
    # ET on sauvegarde une copie versionnée pour la traçabilité.
    # Exemple : anime_gold_v20260327.csv (une version par jour de run)
    version      = context['ds'].replace('-', '')          # ex: "20260327"
    chemin_gold  = f"{OUTPUT_DIR}/anime_gold.csv"          # fichier "latest" — lu par le reste du pipeline
    chemin_versionne = f"{OUTPUT_DIR}/anime_gold_v{version}.csv"  # copie versionnée

    df.to_csv(chemin_gold,       index=False, encoding="utf-8")   # ← pipeline continue à lire celui-ci
    df.to_csv(chemin_versionne,  index=False, encoding="utf-8")   # ← archive de ce run

    taille   = os.path.getsize(chemin_gold) / (1024 * 1024)
    nouvelles = df.shape[1] - n_cols_avant

    print(f"\n  ✅ Gold latest   : {chemin_gold} ({taille:.1f} MB)")
    print(f"  ✅ Gold versionné : {chemin_versionne}")
    print(f"  {nouvelles} nouvelles features créées → {df.shape[1]} colonnes au total")
    print(f"  Version du run   : v{version}")

    # Lister toutes les versions disponibles
    versions_dispo = sorted([
        f for f in os.listdir(OUTPUT_DIR)
        if f.startswith("anime_gold_v") and f.endswith(".csv")
    ])
    print(f"  Versions archivées : {versions_dispo}")

    info = {
        "lignes"           : len(df),
        "colonnes"         : df.shape[1],
        "chemin"           : chemin_gold,
        "chemin_versionne" : chemin_versionne,
        "version"          : f"v{version}",
        "nouvelles_features": nouvelles,
    }
    context['ti'].xcom_push(key='gold_info', value=info)
    return info


def valider_gold(**context):
    import pandas as pd
    import numpy as np

    ti = context['ti']
    df = pd.read_csv(f"{OUTPUT_DIR}/anime_gold.csv")

    print("\n✔️  VALIDATION GOLD\n" + "="*50)
    erreurs = []
    total_pass = 0
    total_fail = 0

    def check(nom, condition, detail=""):
        nonlocal total_pass, total_fail
        if condition:
            total_pass += 1
            print(f"  ✅ PASS — {nom}")
        else:
            total_fail += 1
            erreurs.append(nom)
            print(f"  ❌ FAIL — {nom}" + (f" ({detail})" if detail else ""))

    check("Dataset non vide", len(df) > 0)
    check("Au moins 10 000 animes", len(df) >= 10000, f"{len(df):,} lignes")

    id_col = next((c for c in ["mal_id","anime_id","uid"] if c in df.columns), None)
    if id_col:
        check(f"Clé {id_col} unique", df[id_col].duplicated().sum() == 0)

    check("Aucun doublon exact", df.duplicated().sum() == 0)

    nan_pct = df.isna().sum().sum() / (df.shape[0] * df.shape[1]) * 100
    check("Taux NaN < 30%", nan_pct < 30, f"{nan_pct:.1f}%")

    if "score" in df.columns:
        scores = pd.to_numeric(df["score"], errors="coerce").dropna()
        check("Scores entre 1 et 10", (scores >= 1).all() and (scores <= 10).all())
        check("Aucun score = 0", (scores != 0).all())

    if "drop_ratio" in df.columns:
        dr = df["drop_ratio"].dropna()
        check("drop_ratio entre 0 et 1", (dr >= 0).all() and (dr <= 1).all())

    if "score_category" in df.columns:
        valides = {"Mauvais","Moyen","Bon","Excellent"}
        actuelles = set(df["score_category"].dropna().unique())
        check("score_category valide", actuelles.issubset(valides))

    print(f"\n  PASS: {total_pass} | FAIL: {total_fail} | Taux: {total_pass/(total_pass+total_fail)*100:.0f}%")

    if erreurs:
        raise ValueError(f"Validation Gold échouée : {erreurs}")

    rapport = {"statut": "OK", "pass": total_pass, "fail": total_fail, "lignes": len(df), "colonnes": df.shape[1]}
    ti.xcom_push(key='rapport_gold', value=rapport)
    print("\n✅ Dataset Gold validé !")
    return rapport


# ╔══════════════════════════════════════════╗
# ║            LOAD — FONCTIONS              ║
# ╚══════════════════════════════════════════╝

def preparer_json(**context):
    import pandas as pd
    import json
    import os

    print("\n📦 PRÉPARATION JSON pour Elasticsearch\n" + "="*50)

    df = pd.read_csv(f"{OUTPUT_DIR}/anime_gold.csv")

    # Convertir les types Int64 nullable → float (JSON compatible)
    for col in df.select_dtypes(include=["Int64"]).columns:
        df[col] = df[col].astype("float64")

    # Convertir les Categorical → str
    for col in df.select_dtypes(include=["category"]).columns:
        df[col] = df[col].astype(str).replace("nan", None)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chemin_json = f"{OUTPUT_DIR}/anime_gold.json"

    records = df.where(df.notna(), None).to_dict(orient="records")
    with open(chemin_json, "w", encoding="utf-8") as f:
        for record in records:
            clean = {k: v for k, v in record.items() if v is not None}
            f.write(json.dumps(clean, ensure_ascii=False) + "\n")

    taille = os.path.getsize(chemin_json) / (1024*1024)
    print(f"  ✅ {len(records):,} documents NDJSON exportés ({taille:.1f} MB)")
    print(f"  Chemin : {chemin_json}")

    info = {"nb_docs": len(records), "chemin": chemin_json, "taille_mb": round(taille, 2)}
    context['ti'].xcom_push(key='json_info', value=info)
    return info


def indexer_elasticsearch(**context):
    import json
    import time

    try:
        from elasticsearch import Elasticsearch, helpers
    except ImportError:
        raise ImportError("❌ Module elasticsearch manquant. Ajoutez-le dans _PIP_ADDITIONAL_REQUIREMENTS du docker-compose.")

    ti = context['ti']
    json_info = ti.xcom_pull(task_ids='preparer_json', key='json_info')
    chemin_json = json_info['chemin']

    ES_HOST = "http://elasticsearch:9200"  # nom du service Docker
    INDEX_NAME = "anime"

    print(f"\n🔍 INDEXATION ELASTICSEARCH\n" + "="*50)
    print(f"  Host  : {ES_HOST}")
    print(f"  Index : {INDEX_NAME}")

    # Connexion avec retries
    es = Elasticsearch(ES_HOST, request_timeout=30)
    for attempt in range(10):
        try:
            es.cluster.health()
            print(f"  ✅ Connecté à Elasticsearch")
            break
        except Exception as e:
            if attempt < 9:
                print(f"  ⏳ ES pas prêt ({attempt+1}/10)... attente 5s")
                time.sleep(5)
            else:
                raise ConnectionError(f"Impossible de se connecter à {ES_HOST} : {e}")

    # Charger les documents
    with open(chemin_json, "r", encoding="utf-8") as f:
        docs = [json.loads(line) for line in f if line.strip()]
    print(f"  📂 {len(docs):,} documents chargés")

    # Supprimer et recréer l'index
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"  🗑️  Ancien index supprimé")

    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "mal_id":           {"type": "integer"},
                "name":             {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "english_name":     {"type": "text"},
                "japanese_name":    {"type": "text"},
                "score":            {"type": "float"},
                "weighted_score":   {"type": "float"},
                "score_category":   {"type": "keyword"},
                "type":             {"type": "keyword"},
                "source":           {"type": "keyword"},
                "rating":           {"type": "keyword"},
                "genres":           {"type": "keyword"},
                "main_genre":       {"type": "keyword"},
                "n_genres":         {"type": "integer"},
                "studios":          {"type": "keyword"},
                "main_studio":      {"type": "keyword"},
                "studio_tier":      {"type": "keyword"},
                "episodes":         {"type": "integer"},
                "members":          {"type": "long"},
                "favorites":        {"type": "long"},
                "popularity":       {"type": "integer"},
                "ranked":           {"type": "float"},
                "drop_ratio":       {"type": "float"},
                "engagement_ratio": {"type": "float"},
                "duration_minutes": {"type": "integer"},
                "watching":         {"type": "long"},
                "completed":        {"type": "long"},
                "on_hold":          {"type": "long"},
                "dropped":          {"type": "long"},
                "plan_to_watch":    {"type": "long"},
                "aired_start":      {"type": "date", "format": "yyyy-MM-dd||epoch_millis||strict_date_optional_time", "ignore_malformed": True},
                "aired_end":        {"type": "date", "format": "yyyy-MM-dd||epoch_millis||strict_date_optional_time", "ignore_malformed": True},
                "year":             {"type": "integer"},
                "decade":           {"type": "integer"},
                "is_outlier":       {"type": "boolean"},
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"  ✅ Index '{INDEX_NAME}' créé")

    # Indexation bulk avec progression
    def generate_actions(documents):
        for doc in documents:
            doc_id = doc.get("mal_id", doc.get("anime_id"))
            clean = {k: v for k, v in doc.items()
                     if v is not None and not (isinstance(v, float) and v != v)}
            action = {"_index": INDEX_NAME, "_source": clean}
            if doc_id:
                action["_id"] = str(doc_id)
            yield action

    start = time.time()
    success, errors = 0, 0

    for ok_flag, result in helpers.streaming_bulk(
        es, generate_actions(docs),
        chunk_size=500, raise_on_error=False, raise_on_exception=False
    ):
        if ok_flag:
            success += 1
        else:
            errors += 1
        total = success + errors
        if total % 2000 == 0 or total == len(docs):
            pct = total / len(docs) * 100
            bar = "█" * int(pct / 2.5) + "░" * (40 - int(pct / 2.5))
            print(f"\r  [{bar}] {pct:5.1f}% — {total:,}/{len(docs):,}", end="", flush=True)

    print()
    elapsed = time.time() - start
    es.indices.refresh(index=INDEX_NAME)

    print(f"\n  ✅ Indexation terminée en {elapsed:.1f}s")
    print(f"  Succès : {success:,} | Erreurs : {errors:,}")

    result_info = {"success": success, "errors": errors, "elapsed": round(elapsed, 1), "index": INDEX_NAME}
    ti.xcom_push(key='indexation_result', value=result_info)
    return result_info


def verifier_indexation(**context):
    import time
    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        raise ImportError("Module elasticsearch manquant.")

    ti = context['ti']
    result = ti.xcom_pull(task_ids='indexer_elasticsearch', key='indexation_result')

    es = Elasticsearch("http://elasticsearch:9200", request_timeout=30)
    INDEX_NAME = "anime"

    print("\n🔎 VÉRIFICATION FINALE\n" + "="*50)

    # Comptage
    count = es.count(index=INDEX_NAME)["count"]
    print(f"  Documents indexés : {count:,}")

    # Taille
    stats = es.indices.stats(index=INDEX_NAME)
    size_mb = stats["indices"][INDEX_NAME]["total"]["store"]["size_in_bytes"] / (1024*1024)
    print(f"  Taille index      : {size_mb:.1f} MB")

    # Tests de recherche
    tests = [
        ("match_all",     {"query": {"match_all": {}}}),
        ("Naruto",        {"query": {"match": {"name": "naruto"}}}),
        ("Score > 9",     {"query": {"range": {"score": {"gte": 9}}}}),
        ("Genre Action",  {"query": {"term": {"main_genre": "Action"}}}),
    ]

    print(f"\n  Tests de requêtes :")
    for nom, query in tests:
        try:
            res = es.search(index=INDEX_NAME, body={**query, "size": 1})
            hits = res["hits"]["total"]["value"]
            top = res["hits"]["hits"][0]["_source"].get("name", "?") if res["hits"]["hits"] else "—"
            print(f"  ✅ {nom:20s} → {hits:,} résultats (ex: {top})")
        except Exception as e:
            print(f"  ⚠️  {nom:20s} → erreur : {e}")

    # Top 5 genres
    agg = es.search(index=INDEX_NAME, body={
        "size": 0,
        "aggs": {"genres": {"terms": {"field": "main_genre", "size": 5}}}
    })
    print(f"\n  Top 5 genres :")
    for b in agg["aggregations"]["genres"]["buckets"]:
        print(f"    • {b['key']:20s} — {b['doc_count']:,}")

    print(f"""
{'='*50}
  ✅ PIPELINE ETL TERMINÉ !

  📊 Grafana   → http://localhost:3000  (admin / anidata)
  🔍 ES Search → http://localhost:9200/anime/_search
  🌀 Airflow   → http://localhost:8080
{'='*50}
""")
    return {"count": count, "size_mb": round(size_mb, 1)}


# ╔══════════════════════════════════════════╗
# ║           DÉFINITION DU DAG              ║
# ╚══════════════════════════════════════════╝

with DAG(
    dag_id='anidata_etl',
    default_args=default_args,
    description='Pipeline ETL complet AniData Lab — Extract → Transform → Load',
    schedule_interval=None,
    catchup=False,
    tags=['anidata', 'etl', 'complet'],
) as dag:

    # ── EXTRACT ──────────────────────────────
    t_extract_anime = PythonOperator(
        task_id='extraire_anime',
        python_callable=extraire_anime,
    )
    t_extract_synopsis = PythonOperator(
        task_id='extraire_synopsis',
        python_callable=extraire_synopsis,
    )
    t_extract_ratings = PythonOperator(
        task_id='extraire_ratings',
        python_callable=extraire_ratings,
    )
    t_valider_schemas = PythonOperator(
        task_id='valider_schemas',
        python_callable=valider_schemas,
    )

    # ── TRANSFORM ────────────────────────────
    t_nettoyer = PythonOperator(
        task_id='nettoyer_anime',
        python_callable=nettoyer_anime,
    )
    t_features = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering,
    )
    t_valider_gold = PythonOperator(
        task_id='valider_gold',
        python_callable=valider_gold,
    )

    # ── LOAD ─────────────────────────────────
    t_preparer_json = PythonOperator(
        task_id='preparer_json',
        python_callable=preparer_json,
    )
    t_indexer = PythonOperator(
        task_id='indexer_elasticsearch',
        python_callable=indexer_elasticsearch,
    )
    t_verifier = PythonOperator(
        task_id='verifier_indexation',
        python_callable=verifier_indexation,
    )

    # ── TRIGGER vers le DAG de détection d'anomalies ──────────
    t_trigger_anomalies = TriggerDagRunOperator(
        task_id='declencher_detection_anomalies',
        trigger_dag_id='anomaly_detector',   # ID du DAG de détection
        wait_for_completion=False,           # Ne pas bloquer le pipeline ETL
        reset_dag_run=True,                  # Relancer même si déjà tourné
    )

    # ── DÉPENDANCES ───────────────────────────────────────────────
    # Extract séquentiel
    t_extract_anime >> t_extract_synopsis >> t_extract_ratings >> t_valider_schemas

    # Transform séquentiel
    t_valider_schemas >> t_nettoyer >> t_features >> t_valider_gold

    # Load séquentiel
    t_valider_gold >> t_preparer_json >> t_indexer >> t_verifier

    # Déclencher automatiquement anomaly_detector à la fin
    t_verifier >> t_trigger_anomalies