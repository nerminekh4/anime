"""
DAG : anomaly_detector
Détection d'anomalies dans les données AniData Lab

Ce DAG est déclenché automatiquement à la fin du pipeline ETL principal
(anidata_etl) via un TriggerDagRunOperator.

Il applique 4 types de règles métier pour détecter les anomalies :
  1. Anomalies de score (outliers statistiques, scores impossibles)
  2. Anomalies d'engagement (drop_ratio / score incohérents)
  3. Spam de ratings (comportements suspects dans les avis)
  4. Incohérences de données (valeurs impossibles)

Le rapport est sauvegardé en JSON et les anomalies sont indexées
dans un index Elasticsearch dédié : 'anime_anomalies'.

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# ============================================
# CHEMINS (mêmes que le DAG ETL)
# ============================================
DATA_RAW   = "/opt/airflow/data/raw"
OUTPUT_DIR = "/opt/airflow/output"
GOLD_CSV   = f"{OUTPUT_DIR}/anime_gold.csv"
RATINGS_CSV = f"{DATA_RAW}/rating_complete.csv"

# ============================================
# DEFAULT ARGS
# ============================================
default_args = {
    'owner'            : 'anidata',
    'depends_on_past'  : False,
    'start_date'       : datetime(2026, 3, 23),
    'email_on_failure' : False,
    'retries'          : 2,
    'retry_delay'      : timedelta(minutes=1),
}


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 1 — Charger le dataset gold                        ║
# ╚══════════════════════════════════════════════════════════╝

def charger_gold(**context):
    """
    Charge le dataset gold produit par le pipeline ETL.
    Vérifie qu'il existe et qu'il contient suffisamment de lignes.
    Passe les métadonnées aux tâches suivantes via XCom.
    """
    import pandas as pd
    import os

    print("\n📂 CHARGEMENT DU DATASET GOLD\n" + "="*50)

    if not os.path.exists(GOLD_CSV):
        raise FileNotFoundError(
            f" Dataset gold introuvable : {GOLD_CSV}\n"
            f"   Assurez-vous que le pipeline ETL (anidata_etl) a bien tourné."
        )

    df = pd.read_csv(GOLD_CSV)
    print(f"   {df.shape[0]:,} lignes × {df.shape[1]} colonnes chargées")
    print(f"  Colonnes disponibles : {list(df.columns)}")

    # Vérification minimale
    if len(df) < 1000:
        raise ValueError(f"Dataset trop petit ({len(df)} lignes) — pipeline ETL incomplet ?")

    # Statistiques de base pour les tâches suivantes
    meta = {
        "lignes"   : df.shape[0],
        "colonnes" : df.shape[1],
        "score_mean": float(pd.to_numeric(df.get("score", pd.Series()), errors="coerce").mean()),
        "score_std" : float(pd.to_numeric(df.get("score", pd.Series()), errors="coerce").std()),
    }
    context['ti'].xcom_push(key='gold_meta', value=meta)
    print(f"\n  Score moyen : {meta['score_mean']:.2f} ± {meta['score_std']:.2f}")
    return meta


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 2 — Détection d'anomalies de score                 ║
# ╚══════════════════════════════════════════════════════════╝

def detecter_anomalies_scores(**context):
    """
    Détecte les anomalies statistiques sur les scores des animes.

    Règles appliquées :
      R1 — Score hors bornes [1, 10]
      R2 — Z-score > 3  (outlier statistique extrême)
      R3 — Score < 4 mais members > 100 000 (popularité sans qualité — suspect)
      R4 — Score > 9 mais members < 1 000 (trop parfait pour peu de monde)
    """
    import pandas as pd
    import numpy as np

    ti = context['ti']
    meta = ti.xcom_pull(task_ids='charger_gold', key='gold_meta')

    print("\n🔍 DÉTECTION — Anomalies de score\n" + "="*50)

    df = pd.read_csv(GOLD_CSV)

    # Préparer les colonnes numériques
    df["score"]   = pd.to_numeric(df.get("score",   pd.Series([np.nan]*len(df))), errors="coerce")
    df["members"] = pd.to_numeric(df.get("members", pd.Series([np.nan]*len(df))), errors="coerce")

    anomalies = []

    # ── Règle R1 : Score hors bornes ─────────────────────────
    score_notna = df["score"].notna()
    mask_r1 = score_notna & ((df["score"] < 1) | (df["score"] > 10))
    n_r1 = mask_r1.sum()
    print(f"  R1 — Score hors [1,10]                 : {n_r1:,}")
    for _, row in df[mask_r1].iterrows():
        anomalies.append({
            "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
            "name"         : str(row.get("name", "?")),
            "type_anomalie": "SCORE_HORS_BORNES",
            "severite"     : "CRITIQUE",
            "valeur"       : float(row["score"]),
            "detail"       : f"Score {row['score']} hors de [1, 10]",
        })

    # ── Règle R2 : Outlier statistique (Z-score > 3) ─────────
    mean_s = meta["score_mean"]
    std_s  = meta["score_std"]
    if std_s > 0:
        z_scores = ((df["score"] - mean_s) / std_s).abs()
        mask_r2  = score_notna & (z_scores > 3)
        n_r2     = mask_r2.sum()
        print(f"  R2 — Outlier statistique (Z > 3)       : {n_r2:,}")
        for _, row in df[mask_r2].iterrows():
            z = (row["score"] - mean_s) / std_s
            anomalies.append({
                "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
                "name"         : str(row.get("name", "?")),
                "type_anomalie": "SCORE_OUTLIER_STAT",
                "severite"     : "AVERTISSEMENT",
                "valeur"       : float(row["score"]),
                "detail"       : f"Z-score = {z:.2f} (seuil = 3)",
            })

    # ── Règle R3 : Score bas + grande popularité ─────────────
    mask_r3 = (score_notna & df["members"].notna()
               & (df["score"] < 4) & (df["members"] > 100_000))
    n_r3 = mask_r3.sum()
    print(f"  R3 — Score < 4 mais members > 100K      : {n_r3:,}")
    for _, row in df[mask_r3].iterrows():
        anomalies.append({
            "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
            "name"         : str(row.get("name", "?")),
            "type_anomalie": "POPULARITE_SUSPECT",
            "severite"     : "AVERTISSEMENT",
            "valeur"       : float(row["score"]),
            "detail"       : f"Score {row['score']:.1f} avec {int(row['members']):,} membres",
        })

    # ── Règle R4 : Score très haut + peu de membres ───────────
    mask_r4 = (score_notna & df["members"].notna()
               & (df["score"] > 9) & (df["members"] < 1_000))
    n_r4 = mask_r4.sum()
    print(f"  R4 — Score > 9 mais members < 1 000     : {n_r4:,}")
    for _, row in df[mask_r4].iterrows():
        anomalies.append({
            "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
            "name"         : str(row.get("name", "?")),
            "type_anomalie": "SCORE_PARFAIT_SUSPECT",
            "severite"     : "AVERTISSEMENT",
            "valeur"       : float(row["score"]),
            "detail"       : f"Score {row['score']:.1f} avec seulement {int(row['members']):,} membres",
        })

    print(f"\n   {len(anomalies):,} anomalies de score détectées")
    ti.xcom_push(key='anomalies_scores', value=anomalies)
    return {"nb_anomalies": len(anomalies)}


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 3 — Détection d'anomalies d'engagement             ║
# ╚══════════════════════════════════════════════════════════╝

def detecter_anomalies_engagement(**context):
    """
    Détecte les anomalies dans les ratios d'engagement.

    Règles appliquées :
      R5 — drop_ratio > 0.7 ET score > 7 (très abandonné mais très bien noté)
      R6 — engagement_ratio > 0.15  (trop de favoris / membres = suspect)
      R7 — members > 0 mais completed = 0 ET dropped = 0 (membres fantômes)
      R8 — is_outlier = True (déjà marqué lors du nettoyage)
    """
    import pandas as pd
    import numpy as np

    ti = context['ti']
    print("\n🔍 DÉTECTION — Anomalies d'engagement\n" + "="*50)

    df = pd.read_csv(GOLD_CSV)

    # Convertir les colonnes nécessaires
    for col in ["drop_ratio", "engagement_ratio", "score", "members", "completed", "dropped"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    anomalies = []

    # ── Règle R5 : Fortement abandonné MAIS très bien noté ────
    if "drop_ratio" in df.columns and "score" in df.columns:
        mask_r5 = (df["drop_ratio"].notna() & df["score"].notna()
                   & (df["drop_ratio"] > 0.7) & (df["score"] > 7))
        n_r5 = mask_r5.sum()
        print(f"  R5 — drop_ratio > 0.7 ET score > 7      : {n_r5:,}")
        for _, row in df[mask_r5].iterrows():
            anomalies.append({
                "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
                "name"         : str(row.get("name", "?")),
                "type_anomalie": "ENGAGEMENT_INCOHERENT",
                "severite"     : "AVERTISSEMENT",
                "valeur"       : float(row["drop_ratio"]),
                "detail"       : (f"drop_ratio={row['drop_ratio']:.2f} "
                                  f"mais score={row['score']:.1f} — incohérent"),
            })

    # ── Règle R6 : Trop d'engagement (favoris) ────────────────
    if "engagement_ratio" in df.columns:
        mask_r6 = df["engagement_ratio"].notna() & (df["engagement_ratio"] > 0.15)
        n_r6 = mask_r6.sum()
        print(f"  R6 — engagement_ratio > 15%              : {n_r6:,}")
        for _, row in df[mask_r6].iterrows():
            anomalies.append({
                "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
                "name"         : str(row.get("name", "?")),
                "type_anomalie": "ENGAGEMENT_EXCESSIF",
                "severite"     : "INFO",
                "valeur"       : float(row["engagement_ratio"]),
                "detail"       : f"engagement_ratio={row['engagement_ratio']:.4f} (>15%)",
            })

    # ── Règle R7 : Membres sans activité enregistrée ──────────
    if all(c in df.columns for c in ["members", "completed", "dropped"]):
        mask_r7 = (df["members"].notna() & df["members"] > 1000
                   & df["completed"].notna() & (df["completed"] == 0)
                   & df["dropped"].notna() & (df["dropped"] == 0))
        n_r7 = mask_r7.sum()
        print(f"  R7 — Membres fantômes (members>1K,0 actifs): {n_r7:,}")
        for _, row in df[mask_r7].iterrows():
            anomalies.append({
                "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
                "name"         : str(row.get("name", "?")),
                "type_anomalie": "MEMBRES_FANTOMES",
                "severite"     : "INFO",
                "valeur"       : float(row["members"]),
                "detail"       : f"{int(row['members']):,} membres mais completed=0 et dropped=0",
            })

    # ── Règle R8 : Outliers déjà marqués lors du nettoyage ────
    if "is_outlier" in df.columns:
        outliers_existants = df[df["is_outlier"] == True]
        n_r8 = len(outliers_existants)
        print(f"  R8 — Outliers marqués (nettoyage)        : {n_r8:,}")
        for _, row in outliers_existants.iterrows():
            anomalies.append({
                "mal_id"       : int(row.get("mal_id", row.get("anime_id", -1)) or -1),
                "name"         : str(row.get("name", "?")),
                "type_anomalie": "OUTLIER_NETOYAGE",
                "severite"     : "INFO",
                "valeur"       : float(row.get("score", -1) or -1),
                "detail"       : "Marqué comme outlier lors de l'étape de nettoyage",
            })

    print(f"\n   {len(anomalies):,} anomalies d'engagement détectées")
    ti.xcom_push(key='anomalies_engagement', value=anomalies)
    return {"nb_anomalies": len(anomalies)}


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 4 — Détection de spam de ratings                   ║
# ╚══════════════════════════════════════════════════════════╝

def detecter_spam_ratings(**context):
    """
    Analyse le fichier rating_complete.csv pour détecter des patterns
    de spam ou de manipulation de notes.

    Règles appliquées (sur un échantillon de 200 000 lignes) :
      R9  — Utilisateurs ayant noté > 3000 animes (irréaliste)
      R10 — Utilisateurs ayant mis la même note à TOUS leurs animes
      R11 — Animes où > 60% des notes sont des extrêmes (1 ou 10)

    Note : On travaille sur un échantillon pour éviter la saturation mémoire.
    """
    import pandas as pd
    import numpy as np
    import os

    ti = context['ti']
    print("\n🔍 DÉTECTION — Spam de ratings\n" + "="*50)

    anomalies = []

    if not os.path.exists(RATINGS_CSV):
        print(f"  ⚠️  Fichier ratings introuvable : {RATINGS_CSV} — détection spam ignorée")
        ti.xcom_push(key='anomalies_spam', value=[])
        return {"nb_anomalies": 0}

    # On charge un échantillon (le fichier fait 57M lignes)
    print(f"  Chargement d'un échantillon (200 000 lignes)...")
    df_r = pd.read_csv(RATINGS_CSV, nrows=200_000)
    print(f"   {df_r.shape[0]:,} lignes × {df_r.shape[1]} colonnes")
    print(f"  Colonnes : {list(df_r.columns)}")

    # Identifier les colonnes user, anime, rating
    # Typiquement : user_id, anime_id, rating
    col_user   = next((c for c in df_r.columns if "user" in c.lower()), None)
    col_anime  = next((c for c in df_r.columns if "anime" in c.lower()), None)
    col_rating = next((c for c in df_r.columns if "rating" in c.lower() or "score" in c.lower()), None)

    if not all([col_user, col_anime, col_rating]):
        print(f"  ⚠️  Colonnes attendues introuvables — skip spam detection")
        ti.xcom_push(key='anomalies_spam', value=[])
        return {"nb_anomalies": 0}

    print(f"  Colonnes détectées → user={col_user}, anime={col_anime}, rating={col_rating}")
    df_r[col_rating] = pd.to_numeric(df_r[col_rating], errors="coerce")

    # ── Règle R9 : Utilisateurs ultra-prolifiques ─────────────
    nb_notes_par_user = df_r[col_user].value_counts()
    spammers = nb_notes_par_user[nb_notes_par_user > 3000]
    n_r9 = len(spammers)
    print(f"  R9 — Users ayant noté > 3000 animes     : {n_r9:,}")
    for user_id, count in spammers.items():
        anomalies.append({
            "mal_id"       : int(user_id),
            "name"         : f"[User] {user_id}",
            "type_anomalie": "SPAM_PROLIFIQUE",
            "severite"     : "AVERTISSEMENT",
            "valeur"       : float(count),
            "detail"       : f"User {user_id} a noté {count:,} animes dans l'échantillon",
        })

    # ── Règle R10 : Utilisateur avec note unique pour tous ─────
    # → Tous ses ratings sont identiques (ex: tous 1 ou tous 10)
    notes_par_user = df_r.groupby(col_user)[col_rating]
    users_note_unique = notes_par_user.filter(
        lambda x: x.dropna().nunique() == 1 and len(x.dropna()) >= 10
    )
    user_ids_note_unique = df_r.loc[users_note_unique.index, col_user].unique()
    n_r10 = len(user_ids_note_unique)
    print(f"  R10 — Users avec 1 note unique (≥10 avis): {n_r10:,}")
    for user_id in user_ids_note_unique[:50]:  # Limiter à 50 dans le rapport
        note_unique = df_r[df_r[col_user] == user_id][col_rating].dropna().iloc[0]
        nb_avis = (df_r[col_user] == user_id).sum()
        anomalies.append({
            "mal_id"       : int(user_id),
            "name"         : f"[User] {user_id}",
            "type_anomalie": "SPAM_NOTE_UNIFORME",
            "severite"     : "CRITIQUE",
            "valeur"       : float(note_unique),
            "detail"       : f"User {user_id} a mis {note_unique} à tous ses {nb_avis:,} animes",
        })

    # ── Règle R11 : Animes avec trop de notes extrêmes ─────────
    notes_extremes_par_anime = df_r.groupby(col_anime).apply(
        lambda x: (x[col_rating].isin([1, 10])).sum() / len(x) if len(x) >= 20 else 0
    )
    animes_polarises = notes_extremes_par_anime[notes_extremes_par_anime > 0.6]
    n_r11 = len(animes_polarises)
    print(f"  R11 — Animes avec > 60% notes extrêmes  : {n_r11:,}")
    for anime_id, pct in animes_polarises.head(50).items():
        anomalies.append({
            "mal_id"       : int(anime_id),
            "name"         : f"[Anime ID] {anime_id}",
            "type_anomalie": "SPAM_NOTES_EXTREMES",
            "severite"     : "AVERTISSEMENT",
            "valeur"       : float(pct),
            "detail"       : f"{pct:.0%} des notes sont 1 ou 10 pour cet anime",
        })

    print(f"\n  ✅ {len(anomalies):,} anomalies de spam détectées")
    ti.xcom_push(key='anomalies_spam', value=anomalies)
    return {"nb_anomalies": len(anomalies)}


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 5 — Consolider et sauvegarder le rapport           ║
# ╚══════════════════════════════════════════════════════════╝

def consolider_rapport(**context):
    """
    Rassemble toutes les anomalies détectées par les tasks précédentes,
    calcule les statistiques globales et sauvegarde le rapport complet
    en JSON dans le répertoire output/.

    Principe d'idempotence : si le rapport existe déjà, il est écrasé.
    """
    import pandas as pd
    import json
    import os
    from datetime import datetime

    ti = context['ti']

    # Récupérer les anomalies de chaque task via XCom
    anomalies_scores     = ti.xcom_pull(task_ids='detecter_anomalies_scores',    key='anomalies_scores')     or []
    anomalies_engagement = ti.xcom_pull(task_ids='detecter_anomalies_engagement', key='anomalies_engagement') or []
    anomalies_spam       = ti.xcom_pull(task_ids='detecter_spam_ratings',          key='anomalies_spam')       or []

    print("\n📊 CONSOLIDATION DU RAPPORT\n" + "="*50)

    # Fusion de toutes les anomalies
    toutes = anomalies_scores + anomalies_engagement + anomalies_spam
    print(f"  Anomalies scores      : {len(anomalies_scores):,}")
    print(f"  Anomalies engagement  : {len(anomalies_engagement):,}")
    print(f"  Anomalies spam        : {len(anomalies_spam):,}")
    print(f"  ─────────────────────────────────")
    print(f"  TOTAL                 : {len(toutes):,}")

    # Statistiques par sévérité
    from collections import Counter
    sev_counts = Counter(a["severite"] for a in toutes)
    print(f"\n  Par sévérité :")
    for sev, count in sorted(sev_counts.items()):
        print(f"    {sev:15s} : {count:,}")

    # Statistiques par type
    type_counts = Counter(a["type_anomalie"] for a in toutes)
    print(f"\n  Par type :")
    for t, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t:30s} : {count:,}")

    # Construction du rapport final
    rapport = {
        "run_id"     : context.get("run_id", "manual"),
        "timestamp"  : datetime.utcnow().isoformat() + "Z",
        "dag_id"     : "anomaly_detector",
        "resume"     : {
            "total_anomalies"    : len(toutes),
            "par_severite"       : dict(sev_counts),
            "par_type"           : dict(type_counts),
            "nb_scores"          : len(anomalies_scores),
            "nb_engagement"      : len(anomalies_engagement),
            "nb_spam"            : len(anomalies_spam),
        },
        "anomalies"  : toutes,
    }

    # Sauvegarde (idempotent : on écrase si le fichier existe)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chemin_rapport = f"{OUTPUT_DIR}/rapport_anomalies.json"
    with open(chemin_rapport, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    taille = os.path.getsize(chemin_rapport) / 1024
    print(f"\n  ✅ Rapport sauvegardé : {chemin_rapport} ({taille:.1f} KB)")

    # Passer les infos aux tâches suivantes
    ti.xcom_push(key='rapport_path', value=chemin_rapport)
    ti.xcom_push(key='nb_total', value=len(toutes))
    ti.xcom_push(key='toutes_anomalies', value=toutes)
    return {"nb_total": len(toutes), "chemin": chemin_rapport}


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 6 — Indexer les anomalies dans Elasticsearch       ║
# ╚══════════════════════════════════════════════════════════╝

def indexer_anomalies_elasticsearch(**context):
    """
    Indexe les anomalies détectées dans un index Elasticsearch dédié :
    'anime_anomalies'.

    Principe d'idempotence : l'index est supprimé et recréé à chaque run,
    garantissant qu'il reflète toujours le dernier état du pipeline.

    Si Elasticsearch n'est pas disponible, la tâche génère un avertissement
    mais ne bloque pas le pipeline (soft fail).
    """
    import json
    import time
    import os

    try:
        from elasticsearch import Elasticsearch, helpers
    except ImportError:
        print("⚠️  Module elasticsearch non installé — indexation ignorée")
        return {"skipped": True}

    ti = context['ti']
    toutes_anomalies = ti.xcom_pull(task_ids='consolider_rapport', key='toutes_anomalies') or []
    nb_total         = ti.xcom_pull(task_ids='consolider_rapport', key='nb_total') or 0

    if nb_total == 0:
        print("  ℹ️  Aucune anomalie à indexer.")
        return {"indexed": 0}

    ES_HOST    = "http://elasticsearch:9200"
    INDEX_NAME = "anime_anomalies"

    print(f"\n🔍 INDEXATION DES ANOMALIES\n" + "="*50)
    print(f"  Host  : {ES_HOST}")
    print(f"  Index : {INDEX_NAME}")
    print(f"  Docs  : {nb_total:,}")

    # Connexion avec retries
    es = Elasticsearch(ES_HOST, request_timeout=30)
    for attempt in range(5):
        try:
            es.cluster.health()
            print(f"  ✅ Connecté à Elasticsearch")
            break
        except Exception as e:
            if attempt < 4:
                print(f"  ⏳ ES pas prêt ({attempt+1}/5)... attente 5s")
                time.sleep(5)
            else:
                print(f"  ⚠️  Impossible de se connecter à ES : {e} — indexation ignorée")
                return {"error": str(e)}

    # Mapping de l'index anomalies
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "mal_id"        : {"type": "integer"},
                "name"          : {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "type_anomalie" : {"type": "keyword"},
                "severite"      : {"type": "keyword"},
                "valeur"        : {"type": "float"},
                "detail"        : {"type": "text"},
                "detected_at"   : {"type": "date"},
            }
        }
    }

    # Idempotence : supprimer et recréer l'index
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"  🗑️  Ancien index '{INDEX_NAME}' supprimé")
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"  ✅ Index '{INDEX_NAME}' créé")

    # Ajouter un timestamp de détection à chaque anomalie
    from datetime import datetime
    ts = datetime.utcnow().isoformat() + "Z"
    for a in toutes_anomalies:
        a["detected_at"] = ts

    # Indexation bulk
    def generate_actions(docs):
        for i, doc in enumerate(docs):
            yield {"_index": INDEX_NAME, "_id": str(i), "_source": doc}

    start = time.time()
    success, errors = 0, 0
    for ok_flag, _ in helpers.streaming_bulk(
        es, generate_actions(toutes_anomalies),
        chunk_size=200, raise_on_error=False
    ):
        if ok_flag: success += 1
        else: errors += 1

    elapsed = time.time() - start
    es.indices.refresh(index=INDEX_NAME)
    count = es.count(index=INDEX_NAME)["count"]

    print(f"\n  ✅ Indexation terminée en {elapsed:.1f}s")
    print(f"  Succès : {success:,} | Erreurs : {errors:,}")
    print(f"  Documents dans '{INDEX_NAME}' : {count:,}")

    return {"indexed": success, "errors": errors}


# ╔══════════════════════════════════════════════════════════╗
# ║  TASK 7 — Notification finale                            ║
# ╚══════════════════════════════════════════════════════════╝

def notifier_fin(**context):
    """
    Affiche un résumé final dans les logs Airflow.
    Dans un vrai projet, cette task enverrait un email / Slack / Teams.
    """
    ti = context['ti']
    nb_total = ti.xcom_pull(task_ids='consolider_rapport', key='nb_total') or 0

    print("\n" + "═"*60)
    print("  ✅  PIPELINE ANOMALY_DETECTOR TERMINÉ")
    print("═"*60)
    print(f"""
  Résultats :
    → {nb_total:,} anomalies détectées au total
    → Rapport   : /opt/airflow/output/rapport_anomalies.json
    → Index ES  : http://elasticsearch:9200/anime_anomalies/_search

  Prochaines actions :
    → Consultez le rapport JSON pour le détail de chaque anomalie
    → Vérifiez les anomalies CRITIQUE en priorité
    → Grafana → http://localhost:3000 pour visualiser les anomalies

  Pour re-déclencher manuellement :
    → Airflow UI → DAG 'anomaly_detector' → Trigger DAG
""")
    print("═"*60)
    return {"status": "OK", "nb_anomalies": nb_total}


# ╔══════════════════════════════════════════════════════════╗
# ║  DÉFINITION DU DAG                                       ║
# ╚══════════════════════════════════════════════════════════╝

with DAG(
    dag_id='anomaly_detector',
    default_args=default_args,
    description="Detection d'anomalies AniData Lab - declenche apres anidata_etl",
    schedule_interval=None,   # Déclenché par TriggerDagRunOperator depuis anidata_etl
    catchup=False,            # Bonne pratique : pas de backfill automatique
    tags=['anidata', 'anomalies', 'production'],
    doc_md="""
    ## DAG : anomaly_detector

    Ce DAG analyse les données produites par le pipeline ETL et détecte
    automatiquement des anomalies selon des règles métier définies.

    ### Règles appliquées
    - **Scores** : outliers statistiques, valeurs impossibles
    - **Engagement** : incohérences drop_ratio/score, membres fantômes
    - **Spam** : utilisateurs prolifiques, notes uniformes, notes polarisées

    ### Déclenchement
    Automatiquement après `anidata_etl` via `TriggerDagRunOperator`.
    Peut aussi être lancé manuellement depuis l'UI Airflow.
    """,
) as dag:

    # ── TASKS ─────────────────────────────────────────────────
    t_charger = PythonOperator(
        task_id='charger_gold',
        python_callable=charger_gold,
    )

    t_anomalies_scores = PythonOperator(
        task_id='detecter_anomalies_scores',
        python_callable=detecter_anomalies_scores,
    )

    t_anomalies_engagement = PythonOperator(
        task_id='detecter_anomalies_engagement',
        python_callable=detecter_anomalies_engagement,
    )

    t_spam = PythonOperator(
        task_id='detecter_spam_ratings',
        python_callable=detecter_spam_ratings,
    )

    t_rapport = PythonOperator(
        task_id='consolider_rapport',
        python_callable=consolider_rapport,
    )

    t_indexer = PythonOperator(
        task_id='indexer_anomalies_elasticsearch',
        python_callable=indexer_anomalies_elasticsearch,
    )

    t_fin = PythonOperator(
        task_id='notifier_fin',
        python_callable=notifier_fin,
    )

    # ── DÉPENDANCES — toutes séquentielles ───────────────────
    # Explication du flux :
    #   1. Charger le gold → 2. Analyser les scores →
    #   3. Analyser l'engagement → 4. Analyser le spam →
    #   5. Consolider le rapport → 6. Indexer dans ES → 7. Notifier
    (
        t_charger
        >> t_anomalies_scores
        >> t_anomalies_engagement
        >> t_spam
        >> t_rapport
        >> t_indexer
        >> t_fin
    )
