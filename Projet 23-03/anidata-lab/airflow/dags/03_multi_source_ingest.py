"""
DAG 2 : multi_source_ingest
─────────────────────────────────────────────────────────────────
Process exact demandé :

  Task 1 : detecter_format  (BranchPythonOperator)
               ↙                      ↘
  Task 2a : convertir_xml      Task 2b : convertir_json
               ↘                      ↙
  Task 3 : passer_a_dag1   (trigger_rule='one_success')
                    ↓         XCom : chemin du CSV converti
  Task 4 : declencher_audit   (TriggerDagRunOperator → anidata_etl)

Les fichiers sources (anime_2.json et anime_3.xml) sont dans data/raw/.
Le CSV converti est sauvegardé dans output/converted_<format>.csv
puis passé via XCom au DAG 1 (anidata_etl) pour l'audit.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ============================================
# CHEMINS
# ============================================
DATA_RAW   = "/opt/airflow/data/raw"
OUTPUT_DIR = "/opt/airflow/output"

# Fichiers sources fournis par le professeur
FICHIER_JSON = f"{DATA_RAW}/anime_2.json"
FICHIER_XML  = f"{DATA_RAW}/anime_3.xml"

# ============================================
# DEFAULT ARGS
# ============================================
default_args = {
    'owner': 'anidata',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# ╔══════════════════════════════════════════╗
# ║   TASK 1 — DÉTECTION DU FORMAT           ║
# ╚══════════════════════════════════════════╝

def detecter_format(**context):
    """
    BranchPythonOperator : lit les premiers caractères du fichier
    pour détecter son format (XML ou JSON), sans se fier à l'extension.

    Retourne l'ID de la tâche suivante :
      → 'convertir_json'  si le fichier commence par { ou [
      → 'convertir_xml'   si le fichier commence par <?xml ou <
    """
    print(f"\n{'='*55}")
    print(f"  TASK 1 — DÉTECTION DU FORMAT")
    print(f"{'='*55}")

    # Chercher quel fichier est disponible
    fichier = None
    for chemin in [FICHIER_JSON, FICHIER_XML]:
        if os.path.exists(chemin):
            fichier = chemin
            break

    if not fichier:
        raise FileNotFoundError(
            f"Aucun fichier source trouvé.\n"
            f"Attendu : {FICHIER_JSON} ou {FICHIER_XML}"
        )

    # Lire les premiers caractères pour détecter le format
    with open(fichier, 'r', encoding='utf-8') as f:
        debut = f.read(100).strip()

    print(f"  Fichier   : {fichier}")
    print(f"  Début     : {debut[:60]}...")

    # Détection par CONTENU (pas par extension)
    if debut.startswith('{') or debut.startswith('['):
        format_detecte = 'json'
        tache_suivante = 'convertir_json'
    elif debut.startswith('<?xml') or debut.startswith('<'):
        format_detecte = 'xml'
        tache_suivante = 'convertir_xml'
    else:
        raise ValueError(f"Format non reconnu. Début du fichier : {debut[:60]}")

    print(f"  Format    : {format_detecte.upper()} ✅")
    print(f"  → Branche : {tache_suivante}")

    # Stocker le chemin et le format pour les tâches suivantes
    context['ti'].xcom_push(key='fichier_source', value=fichier)
    context['ti'].xcom_push(key='format_detecte', value=format_detecte)

    return tache_suivante


# ╔══════════════════════════════════════════╗
# ║   TASK 2a — CONVERTIR XML → CSV          ║
# ╚══════════════════════════════════════════╝

def convertir_xml(**context):
    """
    Si le fichier est XML : le parser et le convertir en CSV.
    Le chemin du CSV produit est poussé dans XCom.
    """
    import xml.etree.ElementTree as ET
    import pandas as pd

    ti = context['ti']
    fichier = ti.xcom_pull(task_ids='detecter_format', key='fichier_source')

    print(f"\n{'='*55}")
    print(f"  TASK 2a — CONVERSION XML → CSV")
    print(f"{'='*55}")
    print(f"  Source : {fichier}")

    # Parser le XML
    tree = ET.parse(fichier)
    root = tree.getroot()

    records = []
    for anime in root.findall('.//anime'):
        record = {}

        # Champs simples
        for champ in ['anime_id', 'name', 'type', 'episodes',
                       'rating', 'members', 'year', 'studio', 'status']:
            elem = anime.find(champ)
            if elem is not None and elem.text:
                record[champ] = elem.text.strip()

        # Genres (liste → chaîne séparée par virgules)
        genres_elem = anime.find('genres')
        if genres_elem is not None:
            genres = [g.text.strip() for g in genres_elem.findall('genre') if g.text]
            record['genres'] = ', '.join(genres)

        if record:
            records.append(record)

    df = pd.DataFrame(records)

    # Sauvegarder en CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chemin_csv = f"{OUTPUT_DIR}/converted_xml.csv"
    df.to_csv(chemin_csv, index=False, encoding='utf-8')

    print(f"  Lignes converties : {len(df)}")
    print(f"  Colonnes          : {list(df.columns)}")
    print(f"  CSV sauvegardé    : {chemin_csv} ✅")

    # XCom : passer le chemin du CSV à la tâche suivante
    ti.xcom_push(key='chemin_csv_converti', value=chemin_csv)
    ti.xcom_push(key='nb_lignes', value=len(df))

    return chemin_csv


# ╔══════════════════════════════════════════╗
# ║   TASK 2b — CONVERTIR JSON → CSV         ║
# ╚══════════════════════════════════════════╝

def convertir_json(**context):
    """
    Si le fichier est JSON : le parser et le convertir en CSV.
    Le chemin du CSV produit est poussé dans XCom.
    """
    import json
    import pandas as pd

    ti = context['ti']
    fichier = ti.xcom_pull(task_ids='detecter_format', key='fichier_source')

    print(f"\n{'='*55}")
    print(f"  TASK 2b — CONVERSION JSON → CSV")
    print(f"{'='*55}")
    print(f"  Source : {fichier}")

    # Parser le JSON
    with open(fichier, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Le JSON peut être une liste ou un objet avec clé "records"
    if isinstance(data, list):
        records = data
    elif isinstance(data, dict):
        records = data.get('records', data.get('data', [data]))
    else:
        raise ValueError(f"Structure JSON non reconnue : {type(data)}")

    df = pd.DataFrame(records)

    # Harmoniser : renommer 'genre' → 'genres' si nécessaire
    if 'genre' in df.columns and 'genres' not in df.columns:
        df = df.rename(columns={'genre': 'genres'})

    # Si genres est une liste → chaîne séparée par virgules
    if 'genres' in df.columns:
        df['genres'] = df['genres'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x)
        )

    # Sauvegarder en CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chemin_csv = f"{OUTPUT_DIR}/converted_json.csv"
    df.to_csv(chemin_csv, index=False, encoding='utf-8')

    print(f"  Lignes converties : {len(df)}")
    print(f"  Colonnes          : {list(df.columns)}")
    print(f"  CSV sauvegardé    : {chemin_csv} ✅")

    # XCom : passer le chemin du CSV à la tâche suivante
    ti.xcom_push(key='chemin_csv_converti', value=chemin_csv)
    ti.xcom_push(key='nb_lignes', value=len(df))

    return chemin_csv


# ╔══════════════════════════════════════════╗
# ║   TASK 3 — PASSER À DAG 1 VIA XCOM      ║
# ╚══════════════════════════════════════════╝

def passer_a_dag1(**context):
    """
    Récupère le chemin du CSV converti depuis XCom
    (peu importe si c'était XML ou JSON) et le prépare
    pour le TriggerDagRunOperator → DAG 1 (audit).

    trigger_rule='one_success' : cette tâche s'exécute
    dès qu'UNE des deux branches précédentes réussit.
    (Sans ça, Airflow attendrait les 2 branches — la tâche
    ne démarrerait jamais car l'une est toujours SKIPPED.)
    """
    ti = context['ti']

    # Lire le XCom depuis la branche qui a tourné
    chemin_csv = (
        ti.xcom_pull(task_ids='convertir_json', key='chemin_csv_converti')
        or
        ti.xcom_pull(task_ids='convertir_xml',  key='chemin_csv_converti')
    )
    nb_lignes = (
        ti.xcom_pull(task_ids='convertir_json', key='nb_lignes')
        or
        ti.xcom_pull(task_ids='convertir_xml',  key='nb_lignes')
    )
    format_detecte = ti.xcom_pull(task_ids='detecter_format', key='format_detecte')

    print(f"\n{'='*55}")
    print(f"  TASK 3 — PASSAGE À DAG 1 (AUDIT)")
    print(f"{'='*55}")
    print(f"  Format source     : {format_detecte.upper()}")
    print(f"  CSV converti      : {chemin_csv}")
    print(f"  Lignes            : {nb_lignes}")
    print(f"  → DAG 1 (anidata_etl) sera déclenché avec ce CSV")

    # Pousser dans XCom pour le TriggerDagRunOperator
    ti.xcom_push(key='csv_pour_dag1', value=chemin_csv)
    ti.xcom_push(key='conf_dag1', value={
        'source_csv':     chemin_csv,
        'source_format':  format_detecte,
        'nb_lignes':      nb_lignes,
        'declencheur':    'multi_source_ingest',
    })

    return chemin_csv


# ╔══════════════════════════════════════════╗
# ║           DÉFINITION DU DAG              ║
# ╚══════════════════════════════════════════╝

with DAG(
    dag_id='multi_source_ingest',
    default_args=default_args,
    description='DAG 2 — Détection XML/JSON → Conversion CSV → Déclenche DAG 1',
    schedule_interval='@daily',  # Se lance automatiquement chaque jour
    catchup=False,
    tags=['anidata', 'multi-source', 'json', 'xml'],
) as dag:

    # ── TASK 1 : DÉTECTION ───────────────────────────────────────
    t1_detecter = BranchPythonOperator(
        task_id='detecter_format',
        python_callable=detecter_format,
        doc_md=(
            "**Task 1 — BranchPythonOperator**\n\n"
            "Lit les premiers caractères du fichier source.\n"
            "- `{` ou `[` → JSON → branche `convertir_json`\n"
            "- `<?xml` ou `<` → XML → branche `convertir_xml`"
        ),
    )

    # ── TASK 2a : CONVERSION XML → CSV ───────────────────────────
    t2a_xml = PythonOperator(
        task_id='convertir_xml',
        python_callable=convertir_xml,
        doc_md=(
            "**Task 2a — XML → CSV**\n\n"
            "Parse le fichier XML avec `xml.etree.ElementTree`.\n"
            "Exporte le résultat en `output/converted_xml.csv`.\n"
            "Pousse le chemin du CSV dans XCom."
        ),
    )

    # ── TASK 2b : CONVERSION JSON → CSV ──────────────────────────
    t2b_json = PythonOperator(
        task_id='convertir_json',
        python_callable=convertir_json,
        doc_md=(
            "**Task 2b — JSON → CSV**\n\n"
            "Parse le fichier JSON avec `json.load()`.\n"
            "Exporte le résultat en `output/converted_json.csv`.\n"
            "Pousse le chemin du CSV dans XCom."
        ),
    )

    # ── TASK 3 : XCOM → DAG 1 ────────────────────────────────────
    t3_xcom = PythonOperator(
        task_id='passer_a_dag1',
        python_callable=passer_a_dag1,
        trigger_rule='one_success',   # S'exécute dès qu'une branche réussit
        doc_md=(
            "**Task 3 — XCom → DAG 1**\n\n"
            "`trigger_rule='one_success'` : démarre dès qu'UNE\n"
            "des deux branches précédentes réussit.\n"
            "Récupère le chemin CSV depuis XCom et prépare\n"
            "le conf pour le déclenchement de DAG 1."
        ),
    )

    # ── TASK 4 : DÉCLENCHER DAG 1 (AUDIT) ────────────────────────
    t4_trigger = TriggerDagRunOperator(
        task_id='declencher_audit_dag1',
        trigger_dag_id='anidata_etl',
        conf={
            'declencheur':   'multi_source_ingest',
            'source_format': '{{ ti.xcom_pull(task_ids="detecter_format", key="format_detecte") }}',
            'chemin_csv':    '{{ ti.xcom_pull(task_ids="passer_a_dag1",   key="csv_pour_dag1") }}',
        },
        wait_for_completion=False,
        reset_dag_run=True,
        doc_md=(
            "**Task 4 — TriggerDagRunOperator**\n\n"
            "Déclenche le DAG 1 (`anidata_etl`) pour l'audit.\n"
            "Passe en conf : format source, chemin CSV converti."
        ),
    )

    # ── DÉPENDANCES ───────────────────────────────────────────────
    #
    #   t1_detecter  (BranchPythonOperator)
    #       ↙                  ↘
    #   t2b_json           t2a_xml
    #       ↘                  ↙
    #       t3_xcom  (trigger_rule='one_success')
    #           ↓
    #       t4_trigger  →  DAG 1 : anidata_etl

    t1_detecter >> [t2b_json, t2a_xml]
    t2b_json    >> t3_xcom
    t2a_xml     >> t3_xcom
    t3_xcom     >> t4_trigger
